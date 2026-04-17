# Technical Test — Implementation Journey

---

## 1. Task Brief

Improve an existing Kafka-based pipeline that reads banking transaction CSV files and reconstructs them on the other side, with the following requirements:

- **Scale:** Handle files containing tens of millions of rows without memory exhaustion.
- **Ordering:** Reconstructed output files must have identical row ordering to the input.
- **Concurrency:** Process all 3 CSV files concurrently in the producer.


---

## 2. How to Run

### Option 1: Manual Setup 

This follows the standard installation using the Kafka binary.

1.  **Download Kafka:** Extract the binary from [kafka.apache.org](https://kafka.apache.org/downloads).
2.  **Start the Server:**
    ```bash
    tar -zxvf kafka_2.13-4.1.0.tgz
    cd kafka_2.13-4.1.0
    KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
    bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c config/server.properties
    bin/kafka-server-start.sh config/server.properties
    ```
3.  **Create the Topic:**
    ```bash
    bin/kafka-topics.sh --create --topic raw-transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
    ```

### Option 2: Docker Setup (Recommended)

> [!TIP]
> **Why choose Docker?**
> We recommend this method because it is **environment-agnostic**. It eliminates issues by packaging the broker, configuration, and topic initialization into a single, reproducible container.

1.  **Start Kafka:**
    ```bash
    docker-compose up -d
    ```
    This automatically provisions the `raw-transactions` and `raw-transactions-dlq` topics with the correct partition counts.

---

### Step 2 — Install dependencies

```bash
cd local-input  && npm install && cd ..
cd local-output && npm install && cd ..
```

### Step 3 — Start the consumer

Open a terminal and run:

```bash
cd local-output
KAFKA_FROM_BEGINNING=true npm start
```

- `KAFKA_FROM_BEGINNING=true` — purges any existing output files and replays from offset 0 (recommended for a fresh run).
- `KAFKA_FROM_BEGINNING=false` — resumes from the last committed offset (for persistent/production streaming).

### Step 4 — Run the producer

In a second terminal:

```bash
cd local-input
npm start
```

The producer streams all CSV files from `local-input/` concurrently to Kafka. The consumer writes reconstructed files to `local-output/` and shuts down automatically once all EOF sentinels are received.

### Step 5 — Verify output

```bash
diff local-input/artificial-test-transactions.csv   local-output/artificial-test-transactions.csv
diff local-input/artificial-test-transactions-1.csv local-output/artificial-test-transactions-1.csv
diff local-input/artificial-test-transactions-2.csv local-output/artificial-test-transactions-2.csv
```

No output from `diff` means a byte-for-byte match.

### Stop Kafka

```bash
docker-compose down
```

---

## 3. Problem Analysis

The original codebase had three critical bottlenecks at scale:

1. **Memory Exhaustion (OOM):** `local-input` loaded entire CSV files into memory via array pushes. `local-output` aggregated all consumed records into an unbounded `Map` before writing to disk. At 50 million rows this exhausts the Node.js heap and crashes the process.
2. **Ordering:** The consumer sorted an in-memory array to restore original row alignment — impossible at scale without complex external disk-merge tooling.
3. **Sequential Processing:** Files were produced via an `await` loop, serialising all I/O and eliminating any concurrency benefit.

---

## 4. Core Strategy: Partition-Per-File

The ordering and memory problems share a single root cause: trying to hold the whole file in memory to reconstruct order. The fix is to push ordering responsibility into Kafka itself.

By mapping **one input file to exactly one Kafka partition**, Kafka's FIFO guarantee within a partition means messages always arrive at the consumer in the order they were sent. The consumer can then write each row to disk as it arrives — no buffering, no sorting, no in-memory accumulation.

This single decision unlocks constant-memory operation on both sides and is the architectural foundation everything else builds on.

---

## 5. Assumptions & Guardrails

- **Partition Alignment:** The system assumes a one-to-one mapping between input files and Kafka partitions. `docker-compose.yml` provisions **3 partitions** to match the 3 input files. Adding files requires updating both the topic config and the partition count.
- **Bootstrap Stability:** Assumes a reachable Kafka broker at `localhost:9092` within the Docker bridge network.
- **EOS Scope:** Exactly-Once Semantics cover system-level failures (crashes, restarts, rebalances). Business-level duplicates — e.g. a misconfigured producer sending the same logical row twice with different offsets — are a separate concern addressed in the roadmap via row-level hashing.

---

## 6. Implementation Details

### Infrastructure

Switched from manual binary setup to a `docker-compose.yml` running Kafka in KRaft mode. This guarantees the partition topology is correctly provisioned on every run and eliminates environment-specific setup errors.

### Producer (`local-input`)

- **Streaming with Backpressure:** Rows are streamed through `csv-parser`. `stream.pause()` / `stream.resume()` are bound to Kafka's batch `send()` callbacks, capping active memory to ~250 KB per file regardless of file size.
- **Concurrent Processing:** `Promise.allSettled()` replaces the sequential `await` loop, processing all files in parallel.
- **EMFILE Protection:** Opening thousands of concurrent file descriptors hits OS limits and crashes Node.js. A `runWithConcurrencyLimit(tasks, 50)` utility caps the number of simultaneously open streams.
- **Retry Strategy:** The producer is configured with `idempotent: true`, which delegates retry responsibility entirely to KafkaJS — it handles transient network failures and leader elections internally with sequence-number deduplication. A separate application-level retry wrapper would be redundant and was deliberately omitted.

### Consumer (`local-output`)

- **Streaming Writes:** A dedicated `fs.createWriteStream(..., { flags: 'a' })` is opened lazily per file. Disk writes are gated on `stream.once('drain')` to prevent memory buildup when disk I/O lags behind network consumption.

- **Exactly-Once Semantics via Sidecar Offset Tracking:** CSV files have no native indexing, which creates a durability gap: if the process crashes after writing a row but before Kafka commits the offset, that row will be duplicated on restart. The solution is an "offline parity" pattern using `.offset` sidecar files:
  - Every time a row is flushed to disk, the Kafka offset for that message is written to a companion `filename.csv.offset` file.
  - On startup, a single read of the `.offset` file recovers the last committed position. Any incoming message with `offset <= savedOffset` is discarded.
  - This is O(1) regardless of output file size — we never scan the CSV to find the last row.
  - Two operation modes are supported via the `KAFKA_FROM_BEGINNING` environment variable:
    - `true` — purges all existing CSVs and offsets on boot for a clean reconstruction.
    - `false` — keeps existing files and resumes from the last committed offset, designed for persistent production streaming.

- **Row-Level Schema Validation via DLQ:** After the first row establishes the file's column schema, every subsequent row is validated against it. A row with a different column count (stray comma, encoding artifact, broken export) is routed to the `raw-transactions-dlq` topic with full metadata — source file, partition, offset, expected vs actual columns — and committed so the consumer never stalls. This is the meaningful DLQ trigger: data quality problems from the source, not internal bugs.

- **Sentinel-Based Shutdown:** The producer appends a `{ eof: "true" }` sentinel as its final message per partition. The consumer shuts down gracefully once all expected EOF sentinels arrive (the expected count is carried in the `GROUP_JOIN` payload).

- **Rebalance Safety:** KafkaJS does not await async event handlers, so the REBALANCING handler is kept synchronous: `flushOffsets()` writes all in-memory offsets to disk synchronously before anything is cleared, then stream closing runs fire-and-forget. `reset()` is called at the top of the subsequent GROUP_JOIN handler, after the offset files are safely on disk, so the new assignment always starts from a clean, consistent state.

- **State Encapsulation:** All runtime state (open streams, counters, offset bookmarks) is contained within a `StreamManager` class, keeping it fully isolated from the Kafka event loop.

- **Modular Library:** Cross-cutting utilities are extracted into focused modules (`config.js`, `errors.js`, `helpers.js`, `logger.js`) and exported via a `lib/index.js` barrel, enforcing single-responsibility boundaries across the codebase.

---

## 7. Testing & Verification

Tested against all 3 provided CSV files.

**Producer output** (interleaved completion order confirms genuine parallelism):
```
Processing 3 files concurrently: [
  'artificial-test-transactions-1.csv',
  'artificial-test-transactions-2.csv',
  'artificial-test-transactions.csv'
]
Sent 20 rows from artificial-test-transactions-1.csv on partition 0
Sent 20 rows from artificial-test-transactions.csv on partition 2
Sent 20 rows from artificial-test-transactions-2.csv on partition 1
All files processed successfully
```

**Consumer output:**
```
Opened write stream for artificial-test-transactions-1.csv
Opened write stream for artificial-test-transactions-2.csv
Opened write stream for artificial-test-transactions.csv
Done. 3 files written from 60 messages.
```

**Diff verification** (byte-for-byte match against originals):
```bash
diff local-input/artificial-test-transactions.csv   local-output/artificial-test-transactions.csv   → MATCH
diff local-input/artificial-test-transactions-1.csv local-output/artificial-test-transactions-1.csv → MATCH
diff local-input/artificial-test-transactions-2.csv local-output/artificial-test-transactions-2.csv → MATCH
```

The `.csv.offset` sidecar files in `local-output/` record the highest committed Kafka offset per file and serve as the audit trail for durability. Resume behaviour was manually verified by interrupting a 5,000,000-row ingestion mid-stream and confirming zero data loss on restart with `KAFKA_FROM_BEGINNING=false`.

---

## 8. Future Roadmap

### Row-Level Idempotency
The current EOS guarantee covers system failures but not business-level duplicates (a producer sending the same logical row twice with different offsets). Destination-Layer Idempotency using Upsert (Idempotent Writes) would solve this.

### Observability
Integrate **Prometheus/Grafana** with three key metrics:
- `kafka_consumer_lag` — alert when ingestion falls behind the producer.
- `rows_processed_total` — real-time throughput tracking.
- `dlq_message_count` — upstream data quality monitoring.

### Schema Registry
The consumer already validates row column counts at runtime, routing mismatches to the DLQ. The next step is enforcing the contract at the message level: transitioning from raw JSON to **Avro** or **Protobuf** via a Schema Registry so that structurally invalid messages are rejected by the broker before the consumer ever sees them.

### Dynamic Partition Scaling
Logic to dynamically provision Kafka partitions and consumer replicas (via Kubernetes HPA) based on incoming file volume, removing the current manual one-to-one provisioning requirement.

### Compression
Enable **Snappy** or **LZ4** compression on the producer. For raw transaction JSON, this typically yields 60–80% size reduction, cutting both network overhead and storage costs at scale.

### DLQ Replay CLI
A dedicated tool to consume from `raw-transactions-dlq`, allowing engineers to correct upstream data errors and replay messages into the main pipeline without manual file editing.

### Testing
A full test suite using `testcontainers` or a mock Kafka client to assert:
- `runWithConcurrencyLimit()` strictly respects the configured concurrency bound.
- `StreamManager` correctly deduplicates messages under simulated network jitter.
- The consumer resumes correctly from a partially written or corrupted offset file.
