function parseIntEnv(key, fallback) {
  const val = parseInt(process.env[key], 10);
  return isNaN(val) ? fallback : val;
}

const config = {
  brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
  clientId: process.env.KAFKA_CLIENT_ID || "csv-producer",
  topic: process.env.KAFKA_TOPIC || "raw-transactions",
  batchSize: parseIntEnv("BATCH_SIZE", 500),
  csvDir: process.env.CSV_DIR || null,
  csvExtension: process.env.CSV_EXTENSION || ".csv",
  groupId: process.env.KAFKA_GROUP_ID || "csv-reconstruction-group",
  dlqTopic:
    process.env.KAFKA_DLQ_TOPIC ||
    (process.env.KAFKA_TOPIC || "raw-transactions") + "-dlq",
  outputDir: process.env.OUTPUT_DIR || null,
  fromBeginning: process.env.KAFKA_FROM_BEGINNING !== "false",
  sessionTimeoutMs: parseIntEnv("KAFKA_SESSION_TIMEOUT_MS", 60000),
  heartbeatIntervalMs: parseIntEnv("KAFKA_HEARTBEAT_INTERVAL_MS", 10000),
  concurrencyLimit: parseIntEnv("CONCURRENCY_LIMIT", 50),
};

module.exports = config;
