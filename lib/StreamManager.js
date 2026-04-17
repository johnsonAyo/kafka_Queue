const fs = require("fs");
const path = require("path");

class StreamManager {
  constructor({ config, logger, onComplete }) {
    this.config = config;
    this.logger = logger;
    this.onComplete = onComplete;
    this.writeStreams = new Map();
    this.rowCounts = new Map();
    this.fileHeaders = new Map();
    this.completedFiles = new Set();
    this.totalMessagesReceived = 0;
    this.expectedFiles = null;
    this.highestOffsets = new Map();
    this.startOffsets = new Map();
  }

  // Eagerly loads all saved offset files from outputDir so dedup checks are
  // correct from the very first message after a crash-restart.
  loadOffsets() {
    const dir = this.config.outputDir;
    let files;
    try {
      files = fs.readdirSync(dir);
    } catch {
      return; // outputDir doesn't exist yet — nothing to load
    }
    for (const file of files) {
      if (!file.endsWith(".offset")) continue;
      const filename = file.slice(0, -".offset".length);
      try {
        const raw = fs.readFileSync(path.join(dir, file), "utf8").trim();
        this.startOffsets.set(filename, BigInt(raw));
      } catch {
        // corrupt offset file — treat as fresh start for this file
      }
    }
  }

  setExpectedFiles(count) {
    if (count > 0) this.expectedFiles = count;
    this._checkCompletion();
  }

  _checkCompletion() {
    if (
      this.expectedFiles !== null &&
      this.expectedFiles > 0 &&
      this.completedFiles.size >= this.expectedFiles
    ) {
      if (this.onComplete) {
        this.onComplete({
          filesComplete: this.completedFiles.size,
          totalMessages: this.totalMessagesReceived,
        });
      }
    }
  }

  getWriteStream(filename, firstRow) {
    if (this.writeStreams.has(filename)) return this.writeStreams.get(filename);

    const headers = Object.keys(firstRow);
    this.fileHeaders.set(filename, headers);

    const filepath = path.join(this.config.outputDir, filename);
    let fileExists = false;
    try {
      fileExists = fs.statSync(filepath).size > 0;
    } catch {
      // ENOENT — file doesn't exist yet, write headers on first open
    }

    const stream = fs.createWriteStream(filepath, { flags: "a" });
    if (!fileExists) {
      stream.write(headers.join(","));
    }

    // Fallback: populate startOffset if loadOffsets() wasn't called or this
    // file appeared after the initial load (e.g. post-rebalance).
    if (!this.startOffsets.has(filename)) {
      try {
        const raw = fs
          .readFileSync(
            path.join(this.config.outputDir, `${filename}.offset`),
            "utf8",
          )
          .trim();
        this.startOffsets.set(filename, BigInt(raw));
      } catch (err) {
        if (err.code !== "ENOENT") throw err;
        this.startOffsets.set(filename, -1n);
      }
    }

    this.writeStreams.set(filename, stream);
    this.logger.info("file_stream_opened", {
      file: filename,
      outputDir: this.config.outputDir,
    });
    return stream;
  }

  getFileHeaders(filename) {
    return this.fileHeaders.get(filename);
  }

  getStartOffset(filename) {
    return this.startOffsets.get(filename) ?? -1n;
  }

  updateHighestOffset(filename, offset) {
    this.highestOffsets.set(filename, String(offset));
  }

  getRowCount(filename) {
    return this.rowCounts.get(filename) || 0;
  }

  incrementMessageCount(filename) {
    this.totalMessagesReceived++;
    this.rowCounts.set(filename, (this.rowCounts.get(filename) || 0) + 1);
  }

  _persistOffset(filename) {
    if (!this.highestOffsets.has(filename)) return;
    const offsetFile = path.join(this.config.outputDir, `${filename}.offset`);
    try {
      fs.writeFileSync(offsetFile, this.highestOffsets.get(filename));
    } catch (err) {
      this.logger.error("offset_write_failed", {
        file: filename,
        error: err.message,
      });
    }
  }

  async closeStream(filename) {
    const stream = this.writeStreams.get(filename);
    if (!stream) return;
    await new Promise((resolve, reject) => {
      stream.once("finish", resolve);
      stream.once("error", reject);
      stream.end();
    });
    this.writeStreams.delete(filename);

    this._persistOffset(filename);

    this.logger.info("file_complete", {
      file: filename,
      rows: this.rowCounts.get(filename),
    });
  }

  async markEof(filename, csvHeaders) {
    // If no data rows arrived for this file, write a headers-only output file
    // so the reconstructed file is not silently missing.
    if (!this.writeStreams.has(filename) && csvHeaders) {
      const filepath = path.join(this.config.outputDir, filename);
      await new Promise((resolve, reject) => {
        const stream = fs.createWriteStream(filepath);
        stream.once("finish", resolve);
        stream.once("error", reject);
        stream.end(csvHeaders);
      });
      this.logger.info("file_headers_only", { file: filename });
    }
    await this.closeStream(filename);
    this.completedFiles.add(filename);
    this.logger.info("eof_received", {
      file: filename,
      filesComplete: this.completedFiles.size,
      expectedFiles: this.expectedFiles ?? "pending",
    });
    this._checkCompletion();
  }

  // Synchronously persists all in-memory offsets to disk. Called at the start
  // of the REBALANCING handler (which KafkaJS does not await) so that offset
  // files are safely on disk before reset() clears highestOffsets.
  flushOffsets() {
    for (const [filename] of this.highestOffsets) {
      this._persistOffset(filename);
    }
  }

  // Call loadOffsets() after the subsequent GROUP_JOIN to restore offset state.
  reset() {
    this.rowCounts.clear();
    this.fileHeaders.clear();
    this.completedFiles.clear();
    this.totalMessagesReceived = 0;
    this.expectedFiles = null;
    this.highestOffsets.clear();
    this.startOffsets.clear();
  }

  async closeAllStreams() {
    await Promise.all(
      Array.from(this.writeStreams.entries()).map(
        ([filename, stream]) =>
          new Promise((resolve) => {
            const persist = () => {
              this._persistOffset(filename);
              this.logger.warn("stream_force_closed", {
                file: filename,
                note: "closed before EOF — output may be incomplete",
              });
              resolve();
            };
            stream.once("finish", persist);
            stream.once("error", (err) => {
              this.logger.error("stream_close_error", {
                file: filename,
                error: err.message,
              });
              resolve();
            });
            stream.end();
          }),
      ),
    );
    this.writeStreams.clear();
  }
}

module.exports = StreamManager;
