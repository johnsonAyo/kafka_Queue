const { Kafka } = require("kafkajs");
const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");
const {
  createLogger,
  config,
  Errors,
  runWithConcurrencyLimit,
} = require("../lib");

config.csvDir = config.csvDir || __dirname;
const logger = createLogger(config.clientId);

const kafka = new Kafka({ clientId: config.clientId, brokers: config.brokers });
const producer = kafka.producer({ idempotent: true });

async function processCSVFile(filePath, partitionIndex, totalFiles) {
  const filename = path.basename(filePath);
  let batch = [];
  let rowIndex = 0;
  let csvHeaders = [];

  const flush = (messages) => producer.send({ topic: config.topic, messages });

  await new Promise((resolve, reject) => {
    const readable = fs.createReadStream(filePath);
    const stream = readable.pipe(csv());

    const abort = (err) => {
      readable.destroy();
      stream.destroy();
      reject(err);
    };

    stream.on("headers", (headers) => {
      csvHeaders = headers;
    });

    stream.on("data", (row) => {
      if (!row || Object.keys(row).length === 0) {
        logger.warn("dropping_empty_row", { file: filename, rowIndex });
        return;
      }
      batch.push({
        key: String(rowIndex++),
        value: JSON.stringify(row),
        headers: { source: filename },
        partition: partitionIndex,
      });

      if (batch.length >= config.batchSize) {
        stream.pause();
        const toSend = batch;
        batch = [];
        flush(toSend)
          .then(() => stream.resume())
          .catch((err) => abort(Errors.kafkaSendError(filename, err)));
      }
    });

    stream.on("end", resolve);
    stream.on("error", (err) => abort(Errors.fileReadError(filename, err)));
    readable.on("error", (err) => abort(Errors.fileReadError(filename, err)));
  });

  if (batch.length > 0) await flush(batch);

  await flush([
    {
      key: "eof",
      value: null,
      headers: {
        source: filename,
        eof: "true",
        csvHeaders: csvHeaders.join(","),
        totalFiles: String(totalFiles),
      },
      partition: partitionIndex,
    },
  ]);

  logger.info("file_sent", {
    file: filename,
    partition: partitionIndex,
    rows: rowIndex,
  });
}

async function main() {
  await producer.connect();
  logger.info("connected", { brokers: config.brokers, topic: config.topic });

  const csvFiles = fs
    .readdirSync(config.csvDir)
    .filter((filename) => filename.endsWith(config.csvExtension))
    .sort()
    .map((filename) => path.join(config.csvDir, filename));

  if (csvFiles.length === 0) {
    const err = Errors.noFilesFound(config.csvDir, config.csvExtension);
    logger.warn("no_files_found", err.meta);
    await producer.disconnect();
    return;
  }

  const admin = kafka.admin();
  await admin.connect();
  try {
    const metadata = await admin.fetchTopicMetadata({ topics: [config.topic] });
    const topicMeta = metadata.topics.find((t) => t.name === config.topic);
    const partitionsCount = topicMeta ? topicMeta.partitions.length : 0;

    if (csvFiles.length > partitionsCount) {
      const err = Errors.insufficientPartitions(csvFiles.length, partitionsCount);
      logger.error("fatal_infrastructure_error", err.meta);
      throw err;
    }
  } finally {
    await admin.disconnect();
  }

  logger.info("processing_start", {
    files: csvFiles.map((filePath) => path.basename(filePath)),
    batchSize: config.batchSize,
    concurrencyLimit: config.concurrencyLimit,
  });

  const tasks = csvFiles.map(
    (filePath, index) => () => processCSVFile(filePath, index, csvFiles.length),
  );
  const results = await runWithConcurrencyLimit(tasks, config.concurrencyLimit);

  const failures = results
    .map((result, index) => ({
      ...result,
      file: path.basename(csvFiles[index]),
    }))
    .filter((result) => result.status === "rejected");

  if (failures.length > 0) {
    failures.forEach((failure) =>
      logger.error("file_failed", {
        file: failure.file,
        error: failure.reason.message,
      }),
    );
  }

  logger.info("all_files_complete", {
    totalFiles: csvFiles.length,
    succeeded: csvFiles.length - failures.length,
    failed: failures.length,
  });
  await producer.disconnect();
}

async function shutdown() {
  try {
    await producer.disconnect();
  } catch (err) {
    logger.error("shutdown_error", { error: err.message });
  }
}

process.on("SIGINT", async () => {
  await shutdown();
  process.exit(0);
});
process.on("SIGTERM", async () => {
  await shutdown();
  process.exit(0);
});

if (require.main === module) {
  main().catch((err) => {
    logger.error("fatal", {
      code: err.code || "UNKNOWN",
      error: err.message,
      stack: err.stack,
      ...err.meta,
    });
    process.exit(1);
  });
}

module.exports = { main, processCSVFile };
