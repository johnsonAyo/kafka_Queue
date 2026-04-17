function createLogger(clientId) {
  function _write(level, event, meta = {}) {
    const entry = {
      timestamp: new Date().toISOString(),
      level,
      service: clientId,
      event,
      ...meta,
    };
    (level === "ERROR" || level === "WARN"
      ? process.stderr
      : process.stdout
    ).write(JSON.stringify(entry) + "\n");
  }
  return {
    info: (event, meta) => _write("INFO", event, meta),
    warn: (event, meta) => _write("WARN", event, meta),
    error: (event, meta) => _write("ERROR", event, meta),
  };
}

module.exports = { createLogger };
