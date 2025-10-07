# Error Handling & Recovery

Comprehensive error handling and recovery system for production ETL pipelines.

## Overview

The error handling system provides:

1. **Retry Strategies**: Automatic retry with exponential backoff and jitter
2. **Circuit Breaker**: Fail-fast protection for cascading failures
3. **Dead Letter Queue (DLQ)**: Persistent storage for failed records
4. **Context Tracking**: Comprehensive metadata for debugging and recovery

## Components

### 1. Retry Strategies

Automatically retry transient failures with configurable strategies.

#### Exponential Backoff (Recommended)

Increases delay exponentially with optional jitter to prevent thundering herd:

```json
{
  "errorHandlingConfig": {
    "retryConfig": {
      "strategy": "exponential",
      "maxAttempts": 5,
      "initialDelaySeconds": 2,
      "maxDelaySeconds": 60,
      "backoffMultiplier": 2.0,
      "jitter": true
    }
  }
}
```

**Parameters:**
- `strategy`: "exponential" or "exponentialbackoff"
- `maxAttempts`: Maximum retry attempts (default: 3)
- `initialDelaySeconds`: Starting delay in seconds (default: 5)
- `maxDelaySeconds`: Maximum delay cap in seconds (default: 60)
- `backoffMultiplier`: Multiplier for each retry (default: 2.0)
- `jitter`: Add randomness to delays (default: true)

**Delay Calculation:**
```
delay = min(initialDelay * (multiplier ^ attemptNumber), maxDelay)
if jitter: delay = delay * (0.5 + random(0, 0.5))
```

**Example Timeline (2s initial, 2.0 multiplier, 60s max):**
- Attempt 1: Immediate
- Attempt 2: ~2s delay
- Attempt 3: ~4s delay
- Attempt 4: ~8s delay
- Attempt 5: ~16s delay

#### Fixed Delay

Simple fixed delay between retries:

```json
{
  "errorHandlingConfig": {
    "retryConfig": {
      "strategy": "fixed",
      "maxAttempts": 3,
      "initialDelaySeconds": 5
    }
  }
}
```

#### No Retry

Fail immediately without retries:

```json
{
  "errorHandlingConfig": {
    "retryConfig": {
      "strategy": "none",
      "maxAttempts": 1
    }
  }
}
```

### 2. Circuit Breaker

Protects against cascading failures by failing fast when error thresholds are exceeded.

```json
{
  "errorHandlingConfig": {
    "circuitBreakerConfig": {
      "enabled": true,
      "failureThreshold": 5,
      "resetTimeoutSeconds": 60,
      "halfOpenMaxAttempts": 1
    }
  }
}
```

**Parameters:**
- `enabled`: Enable circuit breaker (default: true)
- `failureThreshold`: Failures before opening circuit (default: 5)
- `resetTimeoutSeconds`: Time before attempting reset (default: 60)
- `halfOpenMaxAttempts`: Test attempts in half-open state (default: 1)

**States:**
1. **Closed**: Normal operation, requests pass through
2. **Open**: Circuit breaker triggered, requests fail immediately
3. **HalfOpen**: Testing if system recovered, limited requests allowed

**State Transitions:**
```
Closed --[threshold failures]--> Open
Open --[timeout elapsed]--> HalfOpen
HalfOpen --[success]--> Closed
HalfOpen --[failure]--> Open
```

**Metrics Available:**
```scala
val metrics = circuitBreaker.getMetrics()
println(s"State: ${metrics.state}")
println(s"Total failures: ${metrics.totalFailures}")
println(s"Current failures: ${metrics.currentFailures}")
println(s"Success rate: ${metrics.successRate}%")
```

### 3. Dead Letter Queue (DLQ)

Persistent storage for failed records with comprehensive metadata.

#### Kafka DLQ

Store failed records in Kafka topic for reprocessing:

```json
{
  "errorHandlingConfig": {
    "dlqConfig": {
      "dlqType": "kafka",
      "bootstrapServers": "localhost:9092",
      "topic": "etl-dlq",
      "producerConfig": {
        "compression.type": "gzip",
        "max.in.flight.requests.per.connection": "1"
      }
    }
  }
}
```

**Features:**
- Asynchronous publishing with callbacks
- Idempotent producer (exactly-once semantics)
- Headers for filtering (pipeline_id, stage, error_type)
- JSON serialization with full metadata

**Record Format:**
```json
{
  "timestamp": 1696896000000,
  "pipelineId": "user-pipeline",
  "stage": "load",
  "errorType": "SQLException",
  "errorMessage": "Connection refused",
  "stackTrace": "...",
  "originalRecord": "{\"id\": 123, \"name\": \"John\"}",
  "originalSchema": "struct<id:int,name:string>",
  "attemptNumber": 5,
  "context": {
    "table": "users",
    "mode": "upsert"
  }
}
```

**Consumer Example:**
```bash
# Consume all DLQ records
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic etl-dlq --from-beginning

# Filter by pipeline
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic etl-dlq --from-beginning \
  --property print.headers=true | grep "pipeline_id:user-pipeline"
```

#### S3 DLQ

Store failed records in S3 with partitioning:

```json
{
  "errorHandlingConfig": {
    "dlqConfig": {
      "dlqType": "s3",
      "bucketPath": "s3a://my-bucket/dlq/",
      "partitionBy": "date",
      "bufferSize": 100,
      "format": "parquet"
    }
  }
}
```

**Parameters:**
- `bucketPath`: S3 bucket path (e.g., s3a://my-bucket/dlq/)
- `partitionBy`: Partitioning strategy
  - `date`: Partition by date (YYYY-MM-DD)
  - `hour`: Partition by date and hour (YYYY-MM-DD/HH)
  - `pipeline`: Partition by pipeline ID
  - `stage`: Partition by pipeline stage
- `bufferSize`: Records to buffer before flushing (default: 100)
- `format`: Output format - `parquet` or `json` (default: parquet)

**Partitioning Examples:**
```
# Date partitioning
s3://my-bucket/dlq/date=2024-10-06/part-00000.parquet

# Hour partitioning
s3://my-bucket/dlq/date=2024-10-06/hour=14/part-00000.parquet

# Pipeline partitioning
s3://my-bucket/dlq/pipelineId=user-pipeline/part-00000.parquet

# Stage partitioning
s3://my-bucket/dlq/stage=load/part-00000.parquet
```

**Query DLQ Data:**
```scala
// Read all failed records
val dlqData = spark.read.parquet("s3a://my-bucket/dlq/")

// Filter by date
val todayFailures = dlqData.filter($"date" === "2024-10-06")

// Analyze error types
dlqData.groupBy("errorType", "stage")
  .count()
  .orderBy($"count".desc)
  .show()

// Extract original records for reprocessing
val recordsToRetry = dlqData
  .filter($"errorType" === "SQLException")
  .select("originalRecord", "originalSchema")
```

#### Logging DLQ (Development)

Log failed records to application logs:

```json
{
  "errorHandlingConfig": {
    "dlqConfig": {
      "dlqType": "logging"
    }
  }
}
```

#### No DLQ

Disable DLQ (errors logged only):

```json
{
  "errorHandlingConfig": {
    "dlqConfig": {
      "dlqType": "none"
    }
  }
}
```

## Configuration Examples

### Production Configuration (PostgreSQL → PostgreSQL)

```json
{
  "pipelineId": "prod-user-pipeline",
  "name": "Production User Pipeline",
  "extract": {
    "sourceType": "postgresql",
    "connectionParams": {
      "host": "prod-db.example.com",
      "port": "5432",
      "database": "prod_db",
      "user": "etl_user"
    },
    "query": "SELECT * FROM users WHERE updated_at > ?",
    "schemaName": "user_schema"
  },
  "load": {
    "sinkType": "postgresql",
    "connectionParams": {
      "host": "warehouse-db.example.com",
      "port": "5432",
      "database": "warehouse",
      "user": "loader",
      "table": "dim_users",
      "primaryKey": "user_id"
    },
    "writeMode": "upsert",
    "schemaName": "dim_user_schema"
  },
  "errorHandlingConfig": {
    "retryConfig": {
      "strategy": "exponential",
      "maxAttempts": 5,
      "initialDelaySeconds": 2,
      "maxDelaySeconds": 120,
      "backoffMultiplier": 2.0,
      "jitter": true
    },
    "circuitBreakerConfig": {
      "enabled": true,
      "failureThreshold": 5,
      "resetTimeoutSeconds": 60,
      "halfOpenMaxAttempts": 1
    },
    "dlqConfig": {
      "dlqType": "s3",
      "bucketPath": "s3a://prod-etl-dlq/user-pipeline/",
      "partitionBy": "date",
      "bufferSize": 500,
      "format": "parquet"
    },
    "failFast": false
  }
}
```

### Real-Time Streaming (Kafka → Kafka)

```json
{
  "pipelineId": "realtime-events",
  "name": "Real-Time Event Processing",
  "extract": {
    "sourceType": "kafka",
    "connectionParams": {
      "kafka.bootstrap.servers": "kafka1:9092,kafka2:9092",
      "subscribe": "raw-events",
      "startingOffsets": "latest"
    },
    "topic": "raw-events",
    "schemaName": "event_schema"
  },
  "load": {
    "sinkType": "kafka",
    "connectionParams": {
      "kafka.bootstrap.servers": "kafka1:9092,kafka2:9092",
      "topic": "processed-events",
      "checkpointLocation": "/tmp/checkpoints/events"
    },
    "writeMode": "append",
    "schemaName": "processed_event_schema"
  },
  "errorHandlingConfig": {
    "retryConfig": {
      "strategy": "exponential",
      "maxAttempts": 3,
      "initialDelaySeconds": 1,
      "maxDelaySeconds": 30,
      "backoffMultiplier": 2.0,
      "jitter": true
    },
    "circuitBreakerConfig": {
      "enabled": true,
      "failureThreshold": 10,
      "resetTimeoutSeconds": 30,
      "halfOpenMaxAttempts": 2
    },
    "dlqConfig": {
      "dlqType": "kafka",
      "bootstrapServers": "kafka1:9092,kafka2:9092",
      "topic": "events-dlq",
      "producerConfig": {
        "compression.type": "snappy",
        "linger.ms": "10"
      }
    },
    "failFast": false
  }
}
```

### Development Configuration

```json
{
  "pipelineId": "dev-test",
  "name": "Development Test Pipeline",
  "extract": {
    "sourceType": "mysql",
    "connectionParams": {
      "host": "localhost",
      "port": "3306",
      "database": "testdb",
      "user": "root"
    },
    "query": "SELECT * FROM test_table",
    "schemaName": "test_schema"
  },
  "load": {
    "sinkType": "s3",
    "connectionParams": {
      "bucket": "dev-bucket",
      "prefix": "test-output"
    },
    "path": "s3a://dev-bucket/test-output",
    "writeMode": "overwrite",
    "schemaName": "output_schema"
  },
  "errorHandlingConfig": {
    "retryConfig": {
      "strategy": "fixed",
      "maxAttempts": 2,
      "initialDelaySeconds": 3
    },
    "circuitBreakerConfig": {
      "enabled": false
    },
    "dlqConfig": {
      "dlqType": "logging"
    },
    "failFast": true
  }
}
```

## Programmatic Usage

### Creating Error Handling Context

```scala
import com.etl.config._
import com.etl.util._
import org.apache.spark.sql.SparkSession

implicit val spark: SparkSession = SparkSession.builder()
  .appName("ETL Pipeline")
  .getOrCreate()

// Create from configuration
val config = ErrorHandlingConfig(
  retryConfig = RetryConfig(
    strategy = RetryStrategyType.ExponentialBackoff,
    maxAttempts = 5,
    initialDelaySeconds = 2,
    maxDelaySeconds = 60,
    backoffMultiplier = 2.0,
    jitter = true
  ),
  circuitBreakerConfig = CircuitBreakerConfig(
    enabled = true,
    failureThreshold = 5,
    resetTimeoutSeconds = 60,
    halfOpenMaxAttempts = 1
  ),
  dlqConfig = DLQConfig(
    dlqType = DLQType.Kafka,
    bootstrapServers = Some("localhost:9092"),
    topic = Some("etl-dlq")
  )
)

val errorContext = ErrorHandlingFactory.createErrorHandlingContext(config)

// Use with loaders
val loader = new PostgreSQLLoader(Some(errorContext))
```

### Manual Error Handling

```scala
// Execute with retry and circuit breaker
val result = errorContext.execute {
  // Your operation here
  performDatabaseWrite(data)
}

result match {
  case Right(value) =>
    println(s"Operation succeeded: $value")
  case Left(error) =>
    println(s"Operation failed after retries: ${error.getMessage}")
}

// Publish to DLQ manually
val context = Map(
  "pipelineId" -> "my-pipeline",
  "stage" -> "transform",
  "operation" -> "aggregation"
)

failedRecords.foreach { record =>
  errorContext.deadLetterQueue.publish(record, error, context)
}

// Close resources when done
errorContext.close()
```

### Circuit Breaker Metrics

```scala
val circuitBreaker = errorContext.circuitBreaker.get
val metrics = circuitBreaker.getMetrics()

println(s"Circuit Breaker Status:")
println(s"  State: ${metrics.state}")
println(s"  Total Requests: ${metrics.totalRequests}")
println(s"  Total Failures: ${metrics.totalFailures}")
println(s"  Success Rate: ${metrics.successRate}%")
println(s"  Last Failure: ${new java.util.Date(metrics.lastFailureTime)}")
```

## Best Practices

### 1. Retry Strategy Selection

**Use Exponential Backoff when:**
- Dealing with rate-limited APIs
- Transient network issues
- Database connection pooling
- Cloud services with throttling

**Use Fixed Delay when:**
- Simple retry logic needed
- Predictable retry timing required
- Testing and development

**Use No Retry when:**
- Errors are never transient
- Business logic failures
- Invalid data errors

### 2. Circuit Breaker Configuration

**Conservative (Production):**
```json
{
  "failureThreshold": 5,
  "resetTimeoutSeconds": 60
}
```

**Aggressive (High-Volume):**
```json
{
  "failureThreshold": 20,
  "resetTimeoutSeconds": 30
}
```

**Tolerant (Batch Processing):**
```json
{
  "failureThreshold": 10,
  "resetTimeoutSeconds": 120
}
```

### 3. DLQ Strategy

**Kafka DLQ:**
- ✅ Real-time processing
- ✅ Immediate reprocessing needed
- ✅ Order preservation important
- ❌ High storage costs
- ❌ Retention limits

**S3 DLQ:**
- ✅ Batch processing
- ✅ Long-term retention
- ✅ Ad-hoc analysis
- ✅ Cost-effective storage
- ❌ Higher latency

**Logging DLQ:**
- ✅ Development/testing
- ✅ Low error rates
- ❌ Production use

### 4. Buffer Sizing (S3 DLQ)

**Guidelines:**
- Small (50-100): Real-time visibility, more S3 writes
- Medium (100-500): Balanced performance
- Large (500-1000): Batch efficiency, delayed visibility

### 5. Monitoring

**Key Metrics:**
```scala
// Track DLQ publish rate
dlqPublishRate = dlqRecordsPublished / totalRecords

// Circuit breaker trip frequency
cbTripRate = circuitBreakerOpens / timeWindow

// Retry success rate
retrySuccessRate = retriesSucceeded / retriesAttempted

// Alert thresholds
if (dlqPublishRate > 0.01) alert("High DLQ rate")
if (cbTripRate > 5 per hour) alert("Frequent circuit breaks")
if (retrySuccessRate < 0.5) alert("Low retry success")
```

## Troubleshooting

### Circuit Breaker Stuck Open

**Symptoms:** All requests fail immediately with `CircuitBreakerOpenException`

**Solutions:**
1. Check downstream service health
2. Increase `resetTimeoutSeconds`
3. Reduce `failureThreshold` if too sensitive
4. Manually reset: Create new circuit breaker instance

### DLQ Not Receiving Records

**Symptoms:** Errors occur but no DLQ records

**Check:**
```scala
// Verify DLQ type
println(s"DLQ Type: ${config.dlqConfig.dlqType}")

// Test DLQ connection
val testRow = spark.range(1).toDF()
errorContext.deadLetterQueue.publish(
  testRow.first(),
  new Exception("Test"),
  Map("test" -> "true")
)
```

### Retries Exhausted Too Quickly

**Symptoms:** All retry attempts fail within seconds

**Solutions:**
1. Increase `maxAttempts`
2. Increase `initialDelaySeconds`
3. Enable jitter to spread retries
4. Check if errors are actually transient

### High S3 DLQ Costs

**Symptoms:** Excessive S3 write operations

**Solutions:**
1. Increase `bufferSize` (reduce write frequency)
2. Use date partitioning instead of hour
3. Enable compression (Parquet default)
4. Consider Kafka DLQ for high-volume

## Migration Guide

### From Legacy retryConfig

**Before:**
```json
{
  "retryConfig": {
    "maxAttempts": 3,
    "delaySeconds": 5
  }
}
```

**After:**
```json
{
  "errorHandlingConfig": {
    "retryConfig": {
      "strategy": "fixed",
      "maxAttempts": 3,
      "initialDelaySeconds": 5
    },
    "circuitBreakerConfig": {
      "enabled": false
    },
    "dlqConfig": {
      "dlqType": "none"
    }
  }
}
```

**Note:** Legacy `retryConfig` field is deprecated but still supported via `pipelineConfig.retryConfig`.

## Related Documentation

- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Common issues
- [IMPROVEMENT_ROADMAP.md](IMPROVEMENT_ROADMAP.md) - Future enhancements
- [TECHNICAL_DEBT.md](TECHNICAL_DEBT.md) - Known limitations
