# Quickstart Guide: Spark-Based ETL Pipeline Framework

**Feature**: 001-build-an-application
**Date**: 2025-10-06
**Purpose**: Validate end-to-end pipeline functionality through manual testing scenarios

## Prerequisites

- Java 11 installed (`java -version`)
- Scala 2.12.18 installed (`scala -version`)
- SBT 1.9.x installed (`sbt --version`)
- Spark 3.5.6 available locally or on cluster

## Setup

### 1. Build the Project

```bash
# From repository root
sbt clean compile

# Run tests
sbt test

# Create assembly JAR for spark-submit
sbt assembly
```

**Expected Output**:
- Compilation succeeds without errors
- All tests pass (unit, integration, contract, performance)
- Assembly JAR created in `target/scala-2.12/etl-pipeline-assembly-1.0.0.jar`

### 2. Prepare Test Data

```bash
# Create test schemas directory
mkdir -p src/main/resources/schemas

# Copy Avro schemas (already in resources)
ls src/main/resources/schemas/
# Should show: user-event.avsc, transaction.avsc, user-summary.avsc

# Create test configs directory
mkdir -p src/main/resources/configs

# Example pipeline config (see below)
```

### 3. Initialize Local Vault

```bash
# Set master key environment variable
export VAULT_MASTER_KEY="test-encryption-key-for-local-dev"

# Create encrypted vault file (using vault CLI tool or test utility)
# vault.enc will contain encrypted credentials for test databases/Kafka
```

## Test Scenarios

### Scenario 1: Batch Pipeline (S3 → Aggregation → PostgreSQL)

**Objective**: Extract CSV from S3, aggregate by user, load to PostgreSQL with upsert

**Config**: `configs/s3-to-postgres-batch.json`
```json
{
  "pipelineId": "s3-to-postgres-batch",
  "name": "User Event Aggregation (Batch)",
  "extract": {
    "type": "s3",
    "path": "s3://test-bucket/user-events/*.csv",
    "schemaName": "user-event",
    "connectionParams": {
      "format": "csv",
      "header": "true"
    }
  },
  "transforms": [
    {
      "type": "aggregation",
      "parameters": {
        "groupBy": ["user_id"],
        "aggregations": {
          "event_id": "count",
          "amount": "sum",
          "timestamp": "max"
        }
      }
    }
  ],
  "load": {
    "type": "postgresql",
    "table": "user_summary",
    "writeMode": "upsert",
    "upsertKeys": ["user_id"],
    "schemaName": "user-summary",
    "connectionParams": {
      "host": "localhost",
      "port": "5432",
      "database": "etl_test"
    },
    "credentialId": "postgres.password"
  },
  "retry": {
    "maxAttempts": 3,
    "delaySeconds": 5
  }
}
```

**Execution**:
```bash
# Local testing (SBT)
sbt "runMain com.etl.Main --config configs/s3-to-postgres-batch.json --mode batch"

# Spark cluster (spark-submit)
spark-submit \
  --class com.etl.Main \
  --master spark://cluster:7077 \
  --deploy-mode cluster \
  target/scala-2.12/etl-pipeline-assembly-1.0.0.jar \
  --config configs/s3-to-postgres-batch.json \
  --mode batch
```

**Validation**:
1. Check logs for structured JSON output:
   ```json
   {"level":"INFO","message":"Pipeline started","pipelineId":"s3-to-postgres-batch","traceId":"..."}
   {"level":"INFO","message":"Extract complete","recordsExtracted":10000}
   {"level":"INFO","message":"Transform complete","recordsTransformed":1500}
   {"level":"INFO","message":"Load complete","recordsLoaded":1500,"writeMode":"upsert"}
   {"level":"INFO","message":"Pipeline success","duration":45000,"throughput":222}
   ```

2. Verify PostgreSQL data:
   ```sql
   SELECT COUNT(*) FROM user_summary; -- Should be 1500
   SELECT * FROM user_summary WHERE user_id = 'user123';
   ```

3. Verify metrics:
   - Throughput: ≥10K records/sec (complex aggregation)
   - Records extracted = records transformed (no filtering)
   - Records loaded = distinct users

**Expected Result**: ✓ Pipeline completes successfully, data in PostgreSQL, metrics logged

---

### Scenario 2: Streaming Pipeline (Kafka → Windowing → S3)

**Objective**: Stream events from Kafka, apply time windows, write to S3 as Avro files

**Config**: `configs/kafka-to-s3-streaming.json`
```json
{
  "pipelineId": "kafka-to-s3-streaming",
  "name": "Event Stream Windowing",
  "extract": {
    "type": "kafka",
    "topic": "user-events",
    "schemaName": "user-event",
    "connectionParams": {
      "bootstrap.servers": "localhost:9092",
      "group.id": "etl-pipeline",
      "startingOffsets": "latest"
    }
  },
  "transforms": [
    {
      "type": "windowing",
      "parameters": {
        "timeColumn": "timestamp",
        "windowDuration": "10 seconds",
        "slideDuration": "5 seconds"
      }
    }
  ],
  "load": {
    "type": "s3",
    "path": "s3://test-bucket/windowed-events/",
    "writeMode": "append",
    "schemaName": "user-event",
    "connectionParams": {
      "format": "avro",
      "compression": "snappy"
    }
  },
  "retry": {
    "maxAttempts": 3,
    "delaySeconds": 5
  }
}
```

**Execution**:
```bash
# Streaming mode
sbt "runMain com.etl.Main --config configs/kafka-to-s3-streaming.json --mode streaming"
```

**Validation**:
1. Generate test events to Kafka:
   ```bash
   # Using Kafka console producer or test data generator
   kafka-console-producer --broker-list localhost:9092 --topic user-events < test-events.json
   ```

2. Monitor streaming logs:
   ```json
   {"level":"INFO","message":"Streaming started","pipelineId":"kafka-to-s3-streaming"}
   {"level":"INFO","message":"Micro-batch processed","batchId":1,"recordsProcessed":1000,"latency":2500}
   {"level":"INFO","message":"Micro-batch processed","batchId":2,"recordsProcessed":1200,"latency":2800}
   ```

3. Verify S3 output:
   ```bash
   aws s3 ls s3://test-bucket/windowed-events/
   # Should show Avro files partitioned by window
   ```

4. Verify performance:
   - p95 latency: <5 seconds
   - Throughput: ≥50K events/second

**Expected Result**: ✓ Streaming pipeline processes continuously, windows applied, Avro files in S3

---

### Scenario 3: Join Pipeline (MySQL + PostgreSQL → Join → Kafka)

**Objective**: Extract from two databases, join on key, publish to Kafka

**Config**: `configs/db-join-to-kafka.json`
```json
{
  "pipelineId": "db-join-to-kafka",
  "name": "Transaction + User Join",
  "extract": {
    "type": "postgresql",
    "query": "SELECT transaction_id, user_id, amount, status FROM transactions WHERE transaction_time > CURRENT_DATE - 1",
    "schemaName": "transaction",
    "connectionParams": {
      "host": "localhost",
      "port": "5432",
      "database": "etl_test"
    },
    "credentialId": "postgres.password"
  },
  "transforms": [
    {
      "type": "join",
      "parameters": {
        "rightDataset": "users",
        "rightSource": {
          "type": "mysql",
          "query": "SELECT user_id, name, email FROM users",
          "connectionParams": {
            "host": "localhost",
            "port": "3306",
            "database": "users_db"
          },
          "credentialId": "mysql.password"
        },
        "joinType": "inner",
        "joinKeys": ["user_id"]
      }
    }
  ],
  "load": {
    "type": "kafka",
    "topic": "enriched-transactions",
    "writeMode": "append",
    "schemaName": "transaction",
    "connectionParams": {
      "bootstrap.servers": "localhost:9092"
    }
  }
}
```

**Execution**:
```bash
sbt "runMain com.etl.Main --config configs/db-join-to-kafka.json --mode batch"
```

**Validation**:
1. Verify join logic in logs:
   ```json
   {"level":"INFO","message":"Extract complete","source":"postgresql","recordsExtracted":5000}
   {"level":"INFO","message":"Join started","rightDataset":"users","joinType":"inner"}
   {"level":"INFO","message":"Transform complete","recordsTransformed":4800}
   ```

2. Consume from Kafka:
   ```bash
   kafka-console-consumer --bootstrap-server localhost:9092 --topic enriched-transactions --from-beginning
   ```

3. Verify data enrichment: messages should include user name/email

**Expected Result**: ✓ Join executed, enriched records published to Kafka

---

### Scenario 4: Retry Logic Validation

**Objective**: Simulate transient failure, verify 3 retries with 5s delay

**Test Approach**:
1. Configure pipeline to load to intentionally failing sink (invalid credentials)
2. Run pipeline
3. Observe retry attempts in logs

**Expected Logs**:
```json
{"level":"ERROR","message":"Load failed","error":"Connection refused","retryCount":0}
{"level":"WARN","message":"Retrying in 5 seconds","retryCount":1}
{"level":"ERROR","message":"Load failed","error":"Connection refused","retryCount":1}
{"level":"WARN","message":"Retrying in 5 seconds","retryCount":2}
{"level":"ERROR","message":"Load failed","error":"Connection refused","retryCount":2}
{"level":"WARN","message":"Retrying in 5 seconds","retryCount":3}
{"level":"ERROR","message":"Pipeline failed after 3 retries","finalError":"Connection refused"}
```

**Validation**:
- Exactly 3 retry attempts
- 5-second delay between attempts (verify timestamps)
- Pipeline fails gracefully with detailed error

**Expected Result**: ✓ Retry logic works, appropriate delays, clear error messages

---

### Scenario 5: Schema Validation Failure

**Objective**: Verify pipeline halts on schema mismatch

**Test Approach**:
1. Extract data with missing required field (violates Avro schema)
2. Run pipeline
3. Verify validation failure

**Expected Logs**:
```json
{"level":"ERROR","message":"Schema validation failed","schemaName":"user-event","violations":["Missing required field: user_id"]}
{"level":"ERROR","message":"Pipeline halted","stage":"extract","reason":"Schema validation"}
```

**Validation**:
- Pipeline stops immediately after extract validation
- No transform or load stages executed
- Clear error message identifies schema violation

**Expected Result**: ✓ Schema validation enforced, pipeline halts, detailed error

---

## Performance Benchmarks

### Batch Performance Test

```bash
# Generate 1M test records
sbt "runMain com.etl.TestDataGenerator --count 1000000 --output /tmp/test-data.csv"

# Run batch pipeline with performance profiling
sbt "runMain com.etl.Main --config configs/performance-test-batch.json --mode batch --profile"
```

**Expected Metrics**:
- Simple transforms (filter, map): ≥100K records/second
- Complex transforms (aggregate, join): ≥10K records/second
- Memory usage: ≤80% peak
- CPU usage: ≤60% average

### Streaming Performance Test

```bash
# Start event generator (produces 60K events/sec)
sbt "runMain com.etl.StreamEventGenerator --rate 60000 --duration 60"

# Monitor streaming pipeline
# Check logs for p95 latency
```

**Expected Metrics**:
- Throughput: ≥50K events/second sustained
- p95 latency: <5 seconds
- No backpressure warnings under target load

---

## Troubleshooting

### Issue: "Schema not found"
- **Solution**: Verify schema files in `src/main/resources/schemas/`, check `schemaName` in config

### Issue: "Credential not found"
- **Solution**: Check `VAULT_MASTER_KEY` environment variable, verify `vault.enc` file exists

### Issue: "Spark cluster connection failed"
- **Solution**: Verify `--master` URL, check cluster availability, use `local[*]` for local testing

### Issue: Low throughput
- **Solution**: Tune `shufflePartitions` in config, check Spark UI for skewed partitions, increase cluster resources

---

## Success Criteria Checklist

- [ ] All 5 test scenarios execute successfully
- [ ] Schema validation enforced (Scenario 5)
- [ ] Retry logic verified (Scenario 4)
- [ ] Performance benchmarks met (100K/10K batch, 50K streaming, 5s p95)
- [ ] Structured logs output valid JSON
- [ ] spark-submit deployment works on cluster
- [ ] No credentials logged in plaintext
- [ ] All data arrives correctly in target systems

When all scenarios pass, the framework is ready for production deployment.
