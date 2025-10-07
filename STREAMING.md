# Streaming Enhancements

Comprehensive streaming capabilities for the Spark ETL Pipeline Framework using Spark Structured Streaming.

**Version**: 1.0
**Feature**: Advanced streaming with watermarking, stateful aggregations, and late data handling
**Last Updated**: 2025-10-07

---

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Core Concepts](#core-concepts)
4. [Configuration](#configuration)
5. [Watermarking](#watermarking)
6. [Stateful Aggregations](#stateful-aggregations)
7. [Late Data Handling](#late-data-handling)
8. [Output Modes](#output-modes)
9. [Trigger Modes](#trigger-modes)
10. [Exactly-Once Semantics](#exactly-once-semantics)
11. [Complete Examples](#complete-examples)
12. [Best Practices](#best-practices)
13. [Troubleshooting](#troubleshooting)

---

## Overview

The Streaming Enhancements feature provides production-grade streaming capabilities built on Spark Structured Streaming:

### Key Features

- **Watermarking**: Handle late-arriving data with configurable event-time tracking
- **Stateful Aggregations**: Windowed aggregations with automatic state management
- **Late Data Strategies**: Multiple strategies for handling late data (Drop, Log, DLQ, Separate Stream)
- **Multiple Output Modes**: Append, Complete, Update for different use cases
- **Flexible Triggers**: Continuous, ProcessingTime, Once, AvailableNow
- **Exactly-Once Semantics**: Idempotent writes with checkpointing
- **Error Handling**: Integrated with retry, circuit breaker, and DLQ from Feature 1
- **Data Quality**: Streaming validation with Feature 2's data quality rules

### Architecture

```
Kafka Stream → KafkaExtractor (with watermark)
             ↓
             StatefulAggregationTransformer (windowed aggregations)
             ↓
             Data Quality Validation (streaming-aware)
             ↓
             PostgreSQLLoader (foreachBatch with exactly-once)
             ↓
             PostgreSQL Table + Late Data Handler
```

---

## Quick Start

### 1. Basic Streaming Pipeline

```json
{
  "pipelineId": "streaming-demo",
  "mode": "streaming",
  "extract": {
    "sourceType": "kafka",
    "topic": "events",
    "connectionParams": {
      "kafka.bootstrap.servers": "localhost:9092",
      "startingOffsets": "latest"
    }
  },
  "streamingConfigJson": {
    "checkpointLocation": "/tmp/checkpoints/demo",
    "queryName": "demo-query",
    "outputMode": "append",
    "triggerMode": "processingtime=10 seconds"
  }
}
```

### 2. Run Streaming Pipeline

```scala
import com.etl.core.PipelineExecutor
import com.etl.config.ConfigLoader

// Load streaming configuration
val config = ConfigLoader.loadFromFile("configs/streaming-example.json")

// Create and run pipeline
val executor = new PipelineExecutor(config)
val result = executor.execute()

// Wait for termination (streaming runs indefinitely)
result match {
  case Success(metrics) =>
    println(s"Streaming pipeline started: ${metrics.summary}")
    // Keep application running
    Thread.sleep(Long.MaxValue)
  case Failure(error) =>
    println(s"Pipeline failed: ${error.getMessage}")
}
```

---

## Core Concepts

### Event Time vs Processing Time

- **Event Time**: Timestamp when the event occurred (from data itself)
- **Processing Time**: Timestamp when Spark processes the event

**Example**:
```
Event generated: 2025-10-07 10:00:00  ← Event Time
Event arrives:   2025-10-07 10:05:00  ← Processing Time (5 min delay)
```

### Watermarking

Watermark = Latest Event Time - Delay Threshold

- Tracks how late data can arrive before being dropped
- Enables Spark to purge old state and prevent unbounded memory growth
- Required for stateful operations with append mode

**Example**:
```
Watermark Config: "10 minutes"
Latest event time: 10:30:00
Watermark:        10:20:00  (10:30 - 10 min)

Events with event_time >= 10:20:00 → Processed
Events with event_time < 10:20:00  → Dropped as late data
```

### State Management

Stateful operations (aggregations, joins) maintain state across micro-batches:

- **State Store**: RocksDB or HDFS-based storage
- **State Timeout**: Automatic cleanup after inactivity
- **Checkpointing**: Fault tolerance for state recovery

---

## Configuration

### StreamingConfig Structure

```scala
case class StreamingConfig(
  checkpointLocation: String,           // Required: Fault-tolerance checkpoint dir
  queryName: String,                    // Required: Query name for monitoring
  outputMode: OutputMode,               // append, complete, update
  triggerMode: TriggerMode,             // How often to run micro-batches
  watermark: Option[WatermarkConfig],   // Optional: Event-time watermarking
  enableIdempotence: Boolean,           // Exactly-once semantics
  maxRecordsPerTrigger: Option[Long],   // Rate limiting
  maxFilesPerTrigger: Option[Int],      // File source rate limiting
  stateTimeout: Option[String]          // State cleanup timeout
)
```

### JSON Configuration

```json
{
  "streamingConfigJson": {
    "checkpointLocation": "/path/to/checkpoints",
    "queryName": "my-streaming-query",
    "outputMode": "append",
    "triggerMode": "processingtime=5 seconds",
    "enableIdempotence": true,
    "maxRecordsPerTrigger": 10000,
    "watermark": {
      "eventTimeColumn": "event_time",
      "delayThreshold": "10 minutes"
    },
    "stateTimeout": "1 hour",
    "lateDataConfig": {
      "strategy": "sendtodlq",
      "dlqTopic": "late-events-dlq"
    }
  }
}
```

---

## Watermarking

### When to Use Watermarks

✅ **Required For**:
- Windowed aggregations with append mode
- Stream-stream joins
- Preventing unbounded state growth

❌ **Not Needed For**:
- Stateless transformations (map, filter)
- Complete mode aggregations
- Batch processing

### Configuring Watermarks

#### 1. In StreamingConfig

```json
{
  "watermark": {
    "eventTimeColumn": "timestamp",
    "delayThreshold": "10 minutes"
  }
}
```

#### 2. In KafkaExtractor

```scala
val streamingConfig = StreamingConfig(
  checkpointLocation = "/tmp/checkpoints",
  queryName = "watermarked-query",
  watermark = Some(WatermarkConfig("event_time", "10 minutes"))
)

val extractor = new KafkaExtractor(Some(streamingConfig))
```

### Watermark Delay Threshold Guidelines

| Use Case | Recommended Delay | Rationale |
|----------|------------------|-----------|
| IoT sensors | 5-15 minutes | Network latency, buffering |
| User events | 10-30 minutes | Mobile offline mode, retries |
| Financial transactions | 1-5 minutes | Real-time requirements |
| Batch ingestion | 1-2 hours | Large batch windows |

### Example: Watermarked Stream

```scala
// Data arrives with event_time column
// Kafka message: {"user_id": 123, "event_time": "2025-10-07T10:00:00", "action": "click"}

val df = kafkaExtractor.extract(config)
// Watermark automatically applied if configured

// Windowed aggregation (requires watermark for append mode)
val aggregated = df
  .groupBy(
    window(col("event_time"), "10 minutes", "5 minutes"),
    col("user_id")
  )
  .agg(count("*").as("event_count"))

// Late data handling
val lateDataHandler = new LateDataHandler(lateDataConfig, streamingConfig)
// Late events automatically managed based on watermark
```

---

## Stateful Aggregations

### StatefulAggregationTransformer

Performs windowed aggregations on streaming data with automatic state management.

### Configuration Parameters

```json
{
  "transformType": "aggregation",
  "parameters": {
    "groupByColumns": "user_id,product_id",
    "windowColumn": "event_time",
    "windowDuration": "10 minutes",
    "slideDuration": "5 minutes",
    "aggregations": "clicks:count,revenue:sum,price:avg",
    "stateTimeout": "1 hour"
  }
}
```

### Aggregation Functions

| Function | Description | Example |
|----------|-------------|---------|
| `count` | Count of records | `clicks:count` |
| `sum` | Sum of values | `revenue:sum` |
| `avg` | Average of values | `price:avg` |
| `min` | Minimum value | `temperature:min` |
| `max` | Maximum value | `temperature:max` |
| `first` | First value | `session_start:first` |
| `last` | Last value | `session_end:last` |

### Windowing Types

#### Tumbling Window (Non-overlapping)

```json
{
  "windowColumn": "timestamp",
  "windowDuration": "10 minutes"
  // No slideDuration = tumbling window
}
```

```
10:00-10:10 | 10:10-10:20 | 10:20-10:30
    [1]          [2]          [3]
```

#### Sliding Window (Overlapping)

```json
{
  "windowColumn": "timestamp",
  "windowDuration": "10 minutes",
  "slideDuration": "5 minutes"
}
```

```
10:00-10:10
     10:05-10:15
          10:10-10:20
               10:15-10:25
```

### Examples

#### Example 1: User Activity Dashboard

```json
{
  "transformType": "aggregation",
  "parameters": {
    "groupByColumns": "user_id",
    "windowColumn": "event_time",
    "windowDuration": "1 hour",
    "aggregations": "page_views:count,clicks:sum,session_duration:avg"
  }
}
```

**Input Stream**:
```
{"user_id": 123, "event_time": "2025-10-07T10:05:00", "page_views": 1, "clicks": 5}
{"user_id": 123, "event_time": "2025-10-07T10:15:00", "page_views": 1, "clicks": 3}
{"user_id": 456, "event_time": "2025-10-07T10:10:00", "page_views": 1, "clicks": 2}
```

**Output** (hourly aggregations):
```
{"window": "10:00-11:00", "user_id": 123, "page_views_count": 2, "clicks_sum": 8, ...}
{"window": "10:00-11:00", "user_id": 456, "page_views_count": 1, "clicks_sum": 2, ...}
```

#### Example 2: Real-time Sales Dashboard

```json
{
  "transformType": "aggregation",
  "parameters": {
    "groupByColumns": "store_id,product_category",
    "windowColumn": "sale_time",
    "windowDuration": "15 minutes",
    "slideDuration": "5 minutes",
    "aggregations": "quantity:sum,revenue:sum,avg_price:avg"
  }
}
```

**Use Case**: Monitor sales trends with 15-minute windows sliding every 5 minutes

#### Example 3: Simple Aggregation (No Windows)

```json
{
  "transformType": "aggregation",
  "parameters": {
    "groupByColumns": "product_id",
    "aggregations": "total_sales:sum,order_count:count"
  }
}
```

**Note**: Without windowing, this creates a running total (requires Complete or Update output mode)

---

## Late Data Handling

### Late Data Strategies

#### 1. Drop (Default)

**Strategy**: Silently drop late data

```json
{
  "lateDataConfig": {
    "strategy": "drop"
  }
}
```

**Use When**: Late data is rare and not business-critical

#### 2. LogAndDrop

**Strategy**: Log late data details before dropping

```json
{
  "lateDataConfig": {
    "strategy": "loganddrop"
  }
}
```

**Logs**:
```
WARN: Dropping 15 late-arriving record(s). Pipeline: user-events, Stage: transform
WARN: Sample late records (5): Row(user_id=123, event_time=2025-10-07 09:45:00, ...)
```

**Use When**: You want observability into late data patterns

#### 3. SendToDLQ

**Strategy**: Send late data to Kafka dead letter queue

```json
{
  "lateDataConfig": {
    "strategy": "sendtodlq",
    "dlqTopic": "late-user-events"
  }
}
```

**DLQ Message**:
```json
{
  "user_id": 123,
  "event_time": "2025-10-07T09:45:00",
  "action": "click",
  "dlq_reason": "late_arrival",
  "dlq_timestamp": "2025-10-07T10:30:00",
  "pipeline_id": "user-events-pipeline",
  "stage": "load"
}
```

**Use When**: Late data needs reprocessing or analysis

#### 4. SeparateStream

**Strategy**: Write late data to separate table/path

```json
{
  "lateDataConfig": {
    "strategy": "separatestream",
    "lateDataTable": "/data/late_user_events"
  }
}
```

**Use When**: Late data requires separate processing logic

### LateDataHandler Usage

```scala
import com.etl.streaming.{LateDataHandler, LateDataConfig, LateDataStrategy}

// Configure late data handling
val lateDataConfig = LateDataConfig(
  strategy = LateDataStrategy.SendToDLQ,
  dlqTopic = Some("late-events-dlq")
)

// Create handler
val handler = new LateDataHandler(lateDataConfig, streamingConfig)

// Separate and handle late data
val (onTimeData, lateData) = handler.separate(df)
handler.handleLateData(lateData, Map("pipelineId" -> "my-pipeline"))

// Collect metrics
val metrics = handler.collectMetrics(lateData)
println(metrics.summary)
// Output: "Late data: 142 record(s), event time range: 10:15:00 to 10:18:00"
```

---

## Output Modes

### Append Mode

**Behavior**: Only new rows added to result table are written to sink

**Use Cases**:
- Event logs
- Immutable data
- Windowed aggregations with watermark

**Requirements**:
- Aggregations require watermark
- Cannot update existing rows

**Example**:
```json
{"outputMode": "append"}
```

**Compatible With**: Stateless ops, windowed aggregations

### Complete Mode

**Behavior**: Entire result table written to sink after every trigger

**Use Cases**:
- Small aggregation results
- Dashboards requiring full refresh
- Running totals

**Requirements**:
- Only for aggregation queries
- Result must fit in memory

**Example**:
```json
{"outputMode": "complete"}
```

**Compatible With**: Aggregations only

### Update Mode

**Behavior**: Only updated rows written to sink

**Use Cases**:
- Stateful aggregations
- Incremental updates
- Running counts/sums

**Requirements**:
- Aggregation queries
- Sink must support updates

**Example**:
```json
{"outputMode": "update"}
```

**Compatible With**: Aggregations, joins

### Comparison Table

| Output Mode | Writes | State | Use Case |
|------------|--------|-------|----------|
| Append | New rows only | Bounded (with watermark) | Event logs, immutable data |
| Complete | Full table | Entire result | Small dashboards, totals |
| Update | Changed rows | Updated rows | Incremental aggregations |

---

## Trigger Modes

### Continuous (Experimental)

**Behavior**: Process data as soon as it arrives (minimal latency)

```json
{"triggerMode": "continuous"}
```

**Latency**: ~1 ms
**Use Case**: Ultra-low latency requirements
**Note**: Limited operation support, experimental

### ProcessingTime

**Behavior**: Trigger micro-batch at fixed intervals

```json
{"triggerMode": "processingtime=10 seconds"}
```

**Supported Intervals**: `"5 seconds"`, `"1 minute"`, `"10 minutes"`, etc.

**Use Case**: Most streaming use cases (balance latency vs throughput)

### Once

**Behavior**: Process all available data once and stop

```json
{"triggerMode": "once"}
```

**Use Case**: Testing, scheduled batch processing on streaming sources

### AvailableNow

**Behavior**: Process all available data in multiple batches

```json
{"triggerMode": "availablenow"}
```

**Use Case**: Large backfills, faster than Once trigger

### Selection Guide

| Latency Requirement | Trigger Mode | Interval |
|---------------------|--------------|----------|
| Real-time (ms) | Continuous | N/A |
| Near real-time (seconds) | ProcessingTime | 1-10 seconds |
| Low latency (minutes) | ProcessingTime | 1-5 minutes |
| Batch processing | Once or AvailableNow | N/A |

---

## Exactly-Once Semantics

### Enabling Exactly-Once

```json
{
  "streamingConfigJson": {
    "enableIdempotence": true,
    "checkpointLocation": "/checkpoints/my-query"
  }
}
```

### How It Works

1. **Checkpointing**: Track processed offsets and state
2. **Idempotent Writes**: Use foreachBatch with transactional writes
3. **Source Tracking**: Kafka offsets, file paths tracked in checkpoint

### PostgreSQL Exactly-Once Example

```scala
// PostgreSQLLoader automatically uses foreachBatch for streaming
val loader = new PostgreSQLLoader(
  errorHandlingContext = Some(errorCtx),
  streamingConfig = Some(streamingCfg)
)

// Each micro-batch is written atomically
loader.load(streamingDf, loadConfig, WriteMode.Append)
```

### Checkpoint Location Guidelines

- **Use persistent storage**: HDFS, S3, Azure Blob
- **Unique per query**: Different queries need different checkpoints
- **Never delete while running**: Causes full reprocessing
- **Backup periodically**: For disaster recovery

**Example Paths**:
```
/checkpoints/production/user-events-aggregation
/checkpoints/production/sales-dashboard
s3://my-bucket/checkpoints/streaming-etl
```

---

## Complete Examples

### Example 1: Real-Time User Analytics

**Scenario**: Track user activity with 10-minute windows, handle late events

**Configuration**:
```json
{
  "pipelineId": "user-analytics-streaming",
  "mode": "streaming",
  "extract": {
    "sourceType": "kafka",
    "topic": "user-events",
    "connectionParams": {
      "kafka.bootstrap.servers": "localhost:9092",
      "startingOffsets": "latest",
      "maxOffsetsPerTrigger": "10000"
    }
  },
  "transforms": [
    {
      "transformType": "aggregation",
      "parameters": {
        "groupByColumns": "user_id",
        "windowColumn": "event_time",
        "windowDuration": "10 minutes",
        "aggregations": "page_views:count,clicks:sum,revenue:sum"
      }
    }
  ],
  "load": {
    "sinkType": "postgresql",
    "table": "user_activity_10min",
    "writeMode": "append",
    "connectionParams": {
      "host": "localhost",
      "database": "analytics",
      "user": "etl"
    }
  },
  "streamingConfigJson": {
    "checkpointLocation": "/checkpoints/user-analytics",
    "queryName": "user-activity-10min-windows",
    "outputMode": "append",
    "triggerMode": "processingtime=30 seconds",
    "watermark": {
      "eventTimeColumn": "event_time",
      "delayThreshold": "15 minutes"
    },
    "lateDataConfig": {
      "strategy": "sendtodlq",
      "dlqTopic": "late-user-events"
    }
  }
}
```

**Expected Behavior**:
- Every 30 seconds, process new Kafka messages
- Aggregate user events into 10-minute windows
- Events up to 15 minutes late are accepted
- Later events sent to `late-user-events` topic
- Results appended to PostgreSQL table

### Example 2: Sliding Window Sales Dashboard

**Scenario**: 1-hour sales aggregations, sliding every 15 minutes

**Configuration**:
```json
{
  "pipelineId": "sales-dashboard",
  "mode": "streaming",
  "extract": {
    "sourceType": "kafka",
    "topic": "sales-transactions",
    "connectionParams": {
      "kafka.bootstrap.servers": "kafka:9092",
      "startingOffsets": "earliest"
    }
  },
  "transforms": [
    {
      "transformType": "aggregation",
      "parameters": {
        "groupByColumns": "store_id,product_category",
        "windowColumn": "transaction_time",
        "windowDuration": "1 hour",
        "slideDuration": "15 minutes",
        "aggregations": "quantity:sum,revenue:sum,avg_price:avg,transaction_count:count"
      }
    }
  ],
  "load": {
    "sinkType": "postgresql",
    "table": "sales_dashboard",
    "writeMode": "append"
  },
  "streamingConfigJson": {
    "checkpointLocation": "/checkpoints/sales-dashboard",
    "queryName": "sales-sliding-window",
    "outputMode": "append",
    "triggerMode": "processingtime=1 minute",
    "watermark": {
      "eventTimeColumn": "transaction_time",
      "delayThreshold": "5 minutes"
    }
  },
  "dataQualityConfig": {
    "enabled": true,
    "onFailure": "warn",
    "rules": [
      {
        "ruleType": "not_null",
        "columns": ["store_id", "product_category", "transaction_time"]
      },
      {
        "ruleType": "range",
        "columns": ["revenue"],
        "parameters": {"min": 0, "max": 1000000}
      }
    ]
  }
}
```

### Example 3: Continuous Monitoring (Low Latency)

**Scenario**: Real-time fraud detection with continuous processing

```json
{
  "pipelineId": "fraud-detection",
  "mode": "streaming",
  "streamingConfigJson": {
    "triggerMode": "continuous",
    "outputMode": "append",
    "queryName": "fraud-detection-realtime"
  }
}
```

---

## Best Practices

### 1. Watermark Configuration

✅ **DO**:
- Set watermark based on actual lateness distribution
- Monitor late data metrics to tune threshold
- Use generous delays for unreliable networks

❌ **DON'T**:
- Set watermark too short (drops valid data)
- Set watermark too long (memory issues)
- Change watermark without clearing checkpoint

### 2. State Management

✅ **DO**:
- Configure `stateTimeout` for long-running aggregations
- Use RocksDB state store for large state
- Monitor state size in Spark UI

❌ **DON'T**:
- Run stateful queries without watermark (unbounded state)
- Use Complete mode with large result sets
- Ignore state cleanup configuration

### 3. Checkpointing

✅ **DO**:
- Use reliable storage (HDFS, S3)
- Backup checkpoints for critical pipelines
- Use descriptive checkpoint paths

❌ **DON'T**:
- Share checkpoints between queries
- Delete checkpoints of running queries
- Use local filesystem in production

### 4. Performance Tuning

✅ **DO**:
```json
{
  "maxRecordsPerTrigger": 10000,
  "performanceConfig": {
    "shufflePartitions": 200
  }
}
```

❌ **DON'T**:
- Process unlimited data per trigger
- Use default shuffle partitions for large streams

### 5. Monitoring

✅ **DO**:
- Monitor processing lag (Spark UI)
- Track late data rates
- Set up alerts for query failures

**Metrics to Monitor**:
- Input rate (records/sec)
- Processing rate (records/sec)
- Batch duration
- State memory usage
- Late data count

---

## Troubleshooting

### Problem: State Growing Unbounded

**Symptoms**: Memory errors, OOM, slow queries

**Solutions**:
1. Add watermark to enable state cleanup
2. Set `stateTimeout` configuration
3. Reduce window duration
4. Increase cluster resources

```json
{
  "watermark": {
    "eventTimeColumn": "timestamp",
    "delayThreshold": "10 minutes"
  },
  "stateTimeout": "1 hour"
}
```

### Problem: Data Being Dropped as Late

**Symptoms**: Missing data, gaps in aggregations

**Solutions**:
1. Increase watermark delay threshold
2. Check event time column correctness
3. Monitor late data with LogAndDrop strategy

```json
{
  "watermark": {
    "delayThreshold": "30 minutes"  // Increase from 10 minutes
  },
  "lateDataConfig": {
    "strategy": "loganddrop"  // Monitor late data
  }
}
```

### Problem: Query Falling Behind

**Symptoms**: Increasing processing lag, old data

**Solutions**:
1. Reduce trigger interval
2. Increase parallelism (shuffle partitions)
3. Limit data per trigger
4. Scale cluster

```json
{
  "triggerMode": "processingtime=5 minutes",  // Longer interval
  "maxRecordsPerTrigger": 50000,  // Rate limiting
  "performanceConfig": {
    "shufflePartitions": 400  // More parallelism
  }
}
```

### Problem: Checkpoint Corruption

**Symptoms**: Query fails to start, metadata errors

**Solutions**:
1. For development: Delete checkpoint and restart
2. For production: Restore from backup
3. Prevention: Regular checkpoint backups

```bash
# Delete corrupt checkpoint (DEV ONLY)
rm -rf /checkpoints/my-query

# Restore from backup (PRODUCTION)
hadoop fs -cp /checkpoints-backup/my-query /checkpoints/my-query
```

### Problem: Duplicate Data

**Symptoms**: Higher than expected counts

**Solutions**:
1. Verify exactly-once is enabled
2. Check for multiple query instances
3. Verify sink idempotence

```json
{
  "streamingConfigJson": {
    "enableIdempotence": true,  // Enable exactly-once
    "queryName": "unique-query-name"  // Unique per query
  }
}
```

### Common Error Messages

#### "Cannot define watermark on non-streaming Dataset"

**Cause**: Applying watermark to batch DataFrame
**Fix**: Only use watermark in streaming mode

#### "Append output mode not supported when there are streaming aggregations"

**Cause**: Using append mode without watermark
**Fix**: Add watermark configuration or use Update mode

#### "Query [name] terminated with exception"

**Cause**: Error in processing (schema mismatch, null values)
**Fix**: Check logs, enable data quality validation

---

## Summary

### Quick Decision Guide

| Requirement | Solution |
|------------|----------|
| Low latency (seconds) | `triggerMode: "processingtime=5 seconds"` |
| Handle late data | Add watermark + late data handler |
| Windowed analytics | StatefulAggregationTransformer + watermark |
| Exactly-once delivery | `enableIdempotence: true` + checkpointing |
| Monitor late events | `lateDataConfig.strategy: "loganddrop"` |
| Reprocess late events | `lateDataConfig.strategy: "sendtodlq"` |

### Files Created

- `StreamingConfig.scala` - Configuration models
- `KafkaExtractor.scala` - Enhanced with watermark support
- `StatefulAggregationTransformer.scala` - Windowed aggregations
- `LateDataHandler.scala` - Late data management
- `PostgreSQLLoader.scala` - Streaming writes with foreachBatch
- `streaming-example.json` - Complete configuration example

### References

- **Spark Structured Streaming Guide**: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- **Watermarking Details**: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking
- **Output Modes**: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes

---

**Next Steps**: See `IMPORTANT_FEATURES_PLAN.md` for additional features and `IMPLEMENTATION_STATUS.md` for project progress.
