const { Kafka } = require("kafkajs");
const fs = require("fs");
const path = require("path");
const {
  createLogger,
  config,
  Errors,
  jsonToCSVRow,
  StreamManager,
} = require("../lib");

config.outputDir = config.outputDir || __dirname;
const logger = createLogger(config.clientId);

const kafka = new Kafka({ clientId: config.clientId, brokers: config.brokers });
const consumer = kafka.consumer({
  groupId: config.groupId,
  sessionTimeout: config.sessionTimeoutMs,
  heartbeatInterval: config.heartbeatIntervalMs,
  autoCommit: false,
});
const dlqProducer = kafka.producer();

async function shutdownCleanly(state = {}) {
  logger.info("shutdown_clean", {
    filesComplete: state.filesComplete,
    totalMessages: state.totalMessages,
  });
  try {
    await consumer.disconnect();
  } catch (err) {
    logger.error("consumer_disconnect_error", { error: err.message });
  }
  try {
    await dlqProducer.disconnect();
  } catch (err) {
    logger.error("dlq_disconnect_error", { error: err.message });
  }
  process.exit(0);
}

const streamManager = new StreamManager({
  config,
  logger,
  onComplete: shutdownCleanly,
});

async function sendToDlq(message, partition, error) {
  try {
    await dlqProducer.send({
      topic: config.dlqTopic,
      messages: [
        {
          key: message.key,
          value: message.value,
          headers: {
            ...message.headers,
            "dlq-error": error.message,
            "dlq-error-code": error.code,
            "dlq-source-topic": config.topic,
            "dlq-source-partition": String(partition),
            "dlq-source-offset": String(message.offset),
            "dlq-timestamp": new Date().toISOString(),
          },
        },
      ],
    });
    logger.error("message_routed_to_dlq", {
      code: error.code,
      error: error.message,
      dlqTopic: config.dlqTopic,
      ...error.meta,
    });
  } catch (dlqErr) {
    // DLQ send failure must not crash the consumer — log and continue.
    logger.error("dlq_send_failed", {
      dlqError: dlqErr.message,
      originalError: error.message,
      partition,
      offset: String(message.offset),
    });
  }
}

async function main() {
  await consumer.connect();
  await dlqProducer.connect();
  logger.info("connected", {
    brokers: config.brokers,
    topic: config.topic,
    groupId: config.groupId,
  });

  // File cleanup and offset loading happen after a confirmed connection so a
  // broker outage doesn't result in silently deleted output with no recovery.
  if (config.fromBeginning) {
    const existingFiles = fs
      .readdirSync(config.outputDir)
      .filter((f) => f.endsWith(".csv") || f.endsWith(".offset"));
    for (const file of existingFiles) {
      fs.unlinkSync(path.join(config.outputDir, file));
    }
  } else {
    streamManager.loadOffsets();
  }

  consumer.on(consumer.events.REBALANCING, () => {
    logger.warn("rebalance_started");
    streamManager.flushOffsets();
    streamManager
      .closeAllStreams()
      .catch((err) =>
        logger.error("rebalance_stream_close_error", { error: err.message }),
      );
  });

  consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
    streamManager.reset();

    const partitions = payload.memberAssignment[config.topic] ?? [];
    logger.info("group_joined", {
      groupId: config.groupId,
      partitionsAssigned: partitions,
      expectedFiles: partitions.length,
    });
    streamManager.setExpectedFiles(partitions.length);

    if (config.fromBeginning) {
      for (const partition of partitions) {
        consumer.seek({ topic: config.topic, partition, offset: "0" });
      }
    } else {
      streamManager.loadOffsets();
    }
  });

  await consumer.subscribe({
    topic: config.topic,
    fromBeginning: config.fromBeginning,
  });

  await consumer.run({
    partitionsConsumedConcurrently: config.concurrencyLimit,
    eachMessage: async ({ topic, partition, message }) => {
      // Helper: advance Kafka's offset pointer for this partition. Called on
      // every exit path so no message is ever redelivered after processing.
      const commit = () =>
        consumer.commitOffsets([
          {
            topic,
            partition,
            offset: String(BigInt(message.offset) + 1n),
          },
        ]);

      if (!message.headers?.source) {
        await sendToDlq(
          message,
          partition,
          Errors.missingSourceHeader(partition, message.offset),
        );
        await commit();
        return;
      }

      const source = path.basename(message.headers.source.toString());
      const isEof = message.headers.eof?.toString() === "true";

      if (isEof) {
        const csvHeaders = message.headers.csvHeaders?.toString() ?? null;
        await streamManager.markEof(source, csvHeaders);
        await commit();
        return;
      }

      let value;
      try {
        value = JSON.parse(message.value.toString());
        if (typeof value !== "object" || value === null)
          throw new Error("parsed value is not an object");
      } catch (parseError) {
        await sendToDlq(
          message,
          partition,
          Errors.malformedJson(source, partition, message.offset, parseError),
        );
        await commit();
        return;
      }

      if (BigInt(message.offset) <= streamManager.getStartOffset(source)) {
        await commit();
        return;
      }

      const stream = streamManager.getWriteStream(source, value);
      const headers = streamManager.getFileHeaders(source);

      if (Object.keys(value).length !== headers.length) {
        await sendToDlq(
          message,
          partition,
          Errors.schemaMismatch(source, partition, message.offset, headers, Object.keys(value)),
        );
        await commit();
        return;
      }

      streamManager.incrementMessageCount(source);
      const ok = stream.write("\n" + jsonToCSVRow(value, headers));
      streamManager.updateHighestOffset(source, message.offset);

      if (!ok) {
        await new Promise((resolve, reject) => {
          const onDrain = () => {
            stream.off("error", onError);
            resolve();
          };
          const onError = (err) => {
            stream.off("drain", onDrain);
            reject(err);
          };
          stream.once("drain", onDrain);
          stream.once("error", onError);
        });
      }

      await commit();
    },
  });
}

async function gracefulShutdown() {
  logger.warn("interrupt_received", {
    filesComplete: streamManager.completedFiles.size,
    expectedFiles: streamManager.expectedFiles,
    openStreams: streamManager.writeStreams.size,
  });

  await streamManager.closeAllStreams();

  try {
    await consumer.disconnect();
  } catch (err) {
    logger.error("consumer_disconnect_error", { error: err.message });
  }
  try {
    await dlqProducer.disconnect();
  } catch (err) {
    logger.error("dlq_disconnect_error", { error: err.message });
  }
}

process.on("SIGINT", async () => {
  await gracefulShutdown();
  process.exit(0);
});
process.on("SIGTERM", async () => {
  await gracefulShutdown();
  process.exit(0);
});

if (require.main === module) {
  main().catch(async (err) => {
    logger.error("fatal", {
      code: err.code || "UNKNOWN",
      error: err.message,
      stack: err.stack,
      ...err.meta,
    });
    await gracefulShutdown();
    process.exit(1);
  });
}

module.exports = { main };
