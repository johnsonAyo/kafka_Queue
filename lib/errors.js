class AppError extends Error {
  constructor(code, message, meta = {}) {
    super(message);
    this.name = "AppError";
    this.code = code;
    this.meta = meta;
  }
}

const Errors = {
  noFilesFound: (dir, extension) =>
    new AppError("NO_FILES_FOUND", "No input files found", { dir, extension }),
  fileReadError: (file, cause) =>
    new AppError("FILE_READ_ERROR", "Failed to read CSV file", {
      file,
      cause: cause.message,
    }),
  kafkaSendError: (file, cause) =>
    new AppError("KAFKA_SEND_ERROR", "Failed to send batch to Kafka", {
      file,
      cause: cause.message,
    }),
  missingSourceHeader: (partition, offset) =>
    new AppError(
      "MISSING_SOURCE_HEADER",
      "Message missing required source header",
      { partition, offset },
    ),
  malformedJson: (source, partition, offset, cause) =>
    new AppError("MALFORMED_JSON", "Message value is not valid JSON", {
      source,
      partition,
      offset,
      cause: cause.message,
    }),
  partialFailure: (failedCount, totalCount) =>
    new AppError("PARTIAL_FAILURE", "One or more files failed to process", {
      failedCount,
      totalCount,
    }),
  insufficientPartitions: (required, available) =>
    new AppError("INSUFFICIENT_PARTITIONS", "Kafka topic has insufficient partitions for the input files", {
      required,
      available,
    }),
  schemaMismatch: (source, partition, offset, expectedHeaders, actualKeys) =>
    new AppError("SCHEMA_MISMATCH", "Row column count does not match file schema", {
      source,
      partition,
      offset,
      expectedColumns: expectedHeaders.length,
      actualColumns: actualKeys.length,
      expected: expectedHeaders.join(","),
      actual: actualKeys.join(","),
    }),
};

module.exports = { AppError, Errors };
