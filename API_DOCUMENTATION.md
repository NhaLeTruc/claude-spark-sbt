# ETL Pipeline Framework - API Documentation

Complete API reference for the Spark-based ETL framework.

**Version**: 1.0.0
**Last Updated**: 2025-10-08

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Core Components](#core-components)
3. [Configuration](#configuration)
4. [Extractors](#extractors)
5. [Transformers](#transformers)
6. [Loaders](#loaders)
7. [Data Quality](#data-quality)
8. [Error Handling](#error-handling)
9. [Monitoring](#monitoring)
10. [Streaming](#streaming)
11. [Examples](#examples)

---

## Quick Start

### Minimal Batch Pipeline

```scala
import com.etl.config._
import com.etl.core.{ETLPipeline, ExecutionContext}
import com.etl.extract.S3Extractor
import com.etl.load.PostgreSQLLoader
import com.etl.model.{ExecutionMetrics, WriteMode}
import com.etl.transform.AggregationTransformer
import org.apache.spark.sql.SparkSession

object MinimalExample {
  def main(args: Array[String]): Unit = {
    // 1. Create SparkSession
    val spark = SparkSession.builder()
      .appName("ETL Pipeline")
      .master("local[*]")
      .getOrCreate()

    // 2. Configure pipeline
    val config = PipelineConfig(
      pipelineId = "my-first-pipeline",
      name = "My First Pipeline",
      mode = PipelineMode.Batch,
      extract = ExtractConfig(
        sourceType = SourceType.S3,
        path = Some("s3://bucket/input"),
        format = Some("parquet")
      ),
      transforms = Seq(
        TransformConfig(
          transformType = TransformType.Aggregation,
          parameters = Map(
            "groupByColumns" -> "category",
            "aggregations" -> "total:sum(amount)"
          )
        )
      ),
      load = LoadConfig(
        sinkType = SinkType.PostgreSQL,
        table = Some("aggregated_results"),
        writeMode = WriteMode.Overwrite,
        connectionParams = Map(
          "host" -> "localhost",
          "port" -> "5432",
          "database" -> "analytics",
          "user" -> "etl_user"
        ),
        credentialId = Some("postgres-password")
      )
    )

    // 3. Create pipeline
    val pipeline = ETLPipeline(
      extractor = new S3Extractor(),
      transformers = Seq(new AggregationTransformer()),
      loader = new PostgreSQLLoader()
    )

    // 4. Create execution context
    val vault = InMemoryVault("postgres-password" -> "secret")
    val metrics = ExecutionMetrics.initial(config.pipelineId, java.util.UUID.randomUUID().toString)
    val context = ExecutionContext(spark, config, vault, metrics)

    // 5. Run pipeline
    val result = pipeline.run(context)

    // 6. Check result
    if (result.isSuccess) {
      println(s"Success! Processed ${result.metrics.recordsLoaded} records")
    } else {
      println(s"Failed: ${result.metrics.errors.mkString(", ")}")
    }

    spark.stop()
  }
}
```

---

## Core Components

### Pipeline

The `Pipeline` trait defines the contract for all ETL pipelines.

```scala
trait Pipeline {
  def run(context: ExecutionContext): PipelineResult
}
```

**Implementations**:
- `ETLPipeline` - Standard batch/streaming pipeline
- `StreamingPipeline` - Specialized streaming pipeline (future)

### ETLPipeline

Orchestrates extract, transform, and load stages.

```scala
case class ETLPipeline(
  extractor: Extractor,
  transformers: Seq[Transformer],
  loader: Loader
) extends Pipeline
```

**Methods**:

```scala
def run(context: ExecutionContext): PipelineResult
```

Executes the pipeline with the given context.

**Returns**: `PipelineResult` (either `PipelineSuccess` or `PipelineFailure`)

**Example**:

```scala
val pipeline = ETLPipeline(
  extractor = new KafkaExtractor(),
  transformers = Seq(new FilterTransformer(), new AggregationTransformer()),
  loader = new PostgreSQLLoader()
)

val result = pipeline.run(context)
```

### ExecutionContext

Encapsulates all runtime dependencies for pipeline execution.

```scala
case class ExecutionContext(
  spark: SparkSession,
  config: PipelineConfig,
  vault: CredentialVault,
  var metrics: ExecutionMetrics,
  traceId: String = UUID.randomUUID().toString
)
```

**Fields**:
- `spark` - SparkSession for distributed processing
- `config` - Pipeline configuration
- `vault` - Credential storage (encrypted)
- `metrics` - Execution telemetry
- `traceId` - Unique identifier for tracing

**Methods**:

```scala
def updateMetrics(newMetrics: ExecutionMetrics): Unit
def getMDCContext: Map[String, String]
```

**Example**:

```scala
val context = ExecutionContext.create(
  spark = spark,
  config = pipelineConfig,
  vault = myVault
)
```

### PipelineResult

Result of pipeline execution (sealed trait).

```scala
sealed trait PipelineResult {
  def metrics: ExecutionMetrics
  def isSuccess: Boolean
}

case class PipelineSuccess(metrics: ExecutionMetrics) extends PipelineResult {
  def isSuccess: Boolean = true
}

case class PipelineFailure(metrics: ExecutionMetrics, error: Throwable) extends PipelineResult {
  def isSuccess: Boolean = false
}
```

**Usage**:

```scala
val result = pipeline.run(context)

result match {
  case PipelineSuccess(metrics) =>
    println(s"Success! Duration: ${metrics.duration}ms")

  case PipelineFailure(metrics, error) =>
    println(s"Failed: ${error.getMessage}")
    println(s"Errors: ${metrics.errors.mkString(", ")}")
}
```

---

## Configuration

### PipelineConfig

Root configuration for a pipeline.

```scala
case class PipelineConfig(
  pipelineId: String,
  name: String,
  mode: PipelineMode = PipelineMode.Batch,
  extract: ExtractConfig,
  transforms: Seq[TransformConfig] = Seq.empty,
  load: LoadConfig,
  dataQualityConfig: DataQualityConfig = DataQualityConfig.disabled,
  errorHandlingConfig: ErrorHandlingConfig = ErrorHandlingConfig.default,
  performanceConfig: PerformanceConfig = PerformanceConfig.default,
  loggingConfig: LoggingConfig = LoggingConfig.default,
  streamingConfigJson: Option[Map[String, Any]] = None
)
```

**Key Methods**:

```scala
def isStreaming: Boolean
def hasStreamingConfig: Boolean
def getStreamingConfig: Option[StreamingConfig]
```

**Example**:

```scala
val config = PipelineConfig(
  pipelineId = "sales-analytics",
  name = "Daily Sales Analytics",
  mode = PipelineMode.Batch,
  extract = ExtractConfig(...),
  transforms = Seq(
    TransformConfig(...),
    TransformConfig(...)
  ),
  load = LoadConfig(...),
  dataQualityConfig = DataQualityConfig(
    enabled = true,
    validateAfterExtract = true,
    onFailure = "fail",
    rules = Seq(...)
  )
)
```

### ExtractConfig

Configuration for data extraction.

```scala
case class ExtractConfig(
  sourceType: SourceType,
  path: Option[String] = None,
  topic: Option[String] = None,
  table: Option[String] = None,
  query: Option[String] = None,
  format: Option[String] = None,
  schemaName: String = "",
  connectionParams: Map[String, String] = Map.empty,
  credentialId: Option[String] = None
)
```

**Source Types**: `S3`, `Kafka`, `PostgreSQL`, `MySQL`

**Examples**:

```scala
// S3 Extraction
ExtractConfig(
  sourceType = SourceType.S3,
  path = Some("s3://my-bucket/data/2024/01/"),
  format = Some("parquet"),
  connectionParams = Map.empty,
  credentialId = Some("aws-s3-access")
)

// Kafka Extraction
ExtractConfig(
  sourceType = SourceType.Kafka,
  topic = Some("user-events"),
  schemaName = "user-event-schema",
  connectionParams = Map(
    "kafka.bootstrap.servers" -> "kafka-1:9092,kafka-2:9092",
    "subscribe" -> "user-events",
    "startingOffsets" -> "latest"
  ),
  credentialId = Some("kafka-sasl-credentials")
)

// PostgreSQL Extraction
ExtractConfig(
  sourceType = SourceType.PostgreSQL,
  query = Some("SELECT * FROM users WHERE created_at >= '2024-01-01'"),
  connectionParams = Map(
    "host" -> "postgres.example.com",
    "port" -> "5432",
    "database" -> "production"
  ),
  credentialId = Some("postgres-readonly")
)
```

### TransformConfig

Configuration for data transformation.

```scala
case class TransformConfig(
  transformType: TransformType,
  parameters: Map[String, String]
)
```

**Transform Types**: `Filter`, `Aggregation`, `Join`, `Window`

**Examples**:

```scala
// Filter
TransformConfig(
  transformType = TransformType.Filter,
  parameters = Map(
    "condition" -> "status = 'active' AND amount > 100"
  )
)

// Aggregation
TransformConfig(
  transformType = TransformType.Aggregation,
  parameters = Map(
    "groupByColumns" -> "category,region",
    "aggregations" -> "total_sales:sum(amount),count:count(*),avg_sale:avg(amount)"
  )
)

// Join
TransformConfig(
  transformType = TransformType.Join,
  parameters = Map(
    "joinType" -> "left",
    "leftColumns" -> "user_id",
    "rightColumns" -> "id",
    "rightPath" -> "s3://bucket/users",
    "rightFormat" -> "parquet"
  )
)

// Window
TransformConfig(
  transformType = TransformType.Window,
  parameters = Map(
    "windowColumn" -> "timestamp",
    "windowDuration" -> "1 hour",
    "slideDuration" -> "10 minutes",
    "watermark" -> "10 minutes",
    "aggregations" -> "event_count:count(*),total_value:sum(value)"
  )
)
```

### LoadConfig

Configuration for data loading.

```scala
case class LoadConfig(
  sinkType: SinkType,
  path: Option[String] = None,
  topic: Option[String] = None,
  table: Option[String] = None,
  format: Option[String] = None,
  schemaName: String = "",
  writeMode: WriteMode = WriteMode.Append,
  connectionParams: Map[String, String] = Map.empty,
  credentialId: Option[String] = None
)
```

**Sink Types**: `S3`, `Kafka`, `PostgreSQL`, `MySQL`
**Write Modes**: `Append`, `Overwrite`, `Upsert`

**Examples**:

```scala
// S3 Load (Parquet)
LoadConfig(
  sinkType = SinkType.S3,
  path = Some("s3://analytics-bucket/aggregated/"),
  format = Some("parquet"),
  writeMode = WriteMode.Overwrite
)

// PostgreSQL Load (Upsert)
LoadConfig(
  sinkType = SinkType.PostgreSQL,
  table = Some("user_aggregations"),
  writeMode = WriteMode.Upsert,
  connectionParams = Map(
    "host" -> "postgres.example.com",
    "port" -> "5432",
    "database" -> "analytics",
    "primaryKey" -> "user_id,date" // For upsert
  ),
  credentialId = Some("postgres-writer")
)

// Kafka Load (JSON)
LoadConfig(
  sinkType = SinkType.Kafka,
  topic = Some("processed-events"),
  connectionParams = Map(
    "kafka.bootstrap.servers" -> "kafka-1:9092",
    "topic" -> "processed-events"
  ),
  credentialId = Some("kafka-producer")
)
```

---

## Extractors

Extractors implement the `Extractor` trait to read data from sources.

### Extractor Trait

```scala
trait Extractor {
  def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame

  def extractWithVault(config: ExtractConfig, vault: CredentialVault)
                      (implicit spark: SparkSession): DataFrame = {
    extract(config)(spark)  // Default delegates to extract
  }
}
```

### S3Extractor

Reads data from Amazon S3.

```scala
class S3Extractor extends Extractor
```

**Supported Formats**: `parquet`, `json`, `csv`, `avro`, `orc`

**Configuration Parameters**:
- `path` (required) - S3 path (e.g., `s3://bucket/prefix`)
- `format` (required) - File format
- AWS credentials via `credentialId` or IAM roles

**Example**:

```scala
val extractor = new S3Extractor()

val config = ExtractConfig(
  sourceType = SourceType.S3,
  path = Some("s3://data-lake/events/2024/01/"),
  format = Some("parquet"),
  credentialId = Some("aws-s3-reader")
)

val df = extractor.extractWithVault(config, vault)(spark)
```

### DeltaLakeExtractor

Reads data from Delta Lake tables with time travel and change data feed support.

```scala
class DeltaLakeExtractor extends Extractor
```

**Supported Features**:
- ✅ **Time Travel**: Query historical versions by version number or timestamp
- ✅ **Change Data Feed**: Read CDC changes (inserts/updates/deletes)
- ✅ **ACID Reads**: Consistent snapshot isolation
- ✅ **Schema Evolution**: Handle schema changes across versions

**Configuration Parameters**:

| Parameter | Type | Description |
|-----------|------|-------------|
| `path` | String | Delta table path (e.g., `s3a://bucket/delta-tables/users`) |
| `versionAsOf` | Long | Read specific version number (time travel) |
| `timestampAsOf` | String | Read as of timestamp (time travel, format: `yyyy-MM-dd HH:mm:ss`) |
| `readChangeFeed` | Boolean | Read change data feed instead of current state |
| `startingVersion` | Long | Starting version for CDC (inclusive) |
| `startingTimestamp` | String | Starting timestamp for CDC |
| `endingVersion` | Long | Ending version for CDC (inclusive) |
| `endingTimestamp` | String | Ending timestamp for CDC |
| `ignoreDeletes` | Boolean | Ignore deleted rows in change feed |
| `ignoreChanges` | Boolean | Ignore updated rows in change feed |

**Example 1: Read Latest Version**

```scala
val extractor = new DeltaLakeExtractor()

val config = ExtractConfig(
  sourceType = SourceType.DeltaLake,
  connectionParams = Map(
    "path" -> "s3a://data-lake/delta/users"
  ),
  credentialId = Some("s3-credentials")
)

val df = extractor.extractWithVault(config, vault)(spark)
```

**Example 2: Time Travel by Version**

```scala
// Read data as it was at version 42
val config = ExtractConfig(
  sourceType = SourceType.DeltaLake,
  connectionParams = Map(
    "path" -> "s3a://data-lake/delta/users",
    "versionAsOf" -> "42"
  ),
  credentialId = Some("s3-credentials")
)

val historicalDf = extractor.extractWithVault(config, vault)(spark)
```

**Example 3: Time Travel by Timestamp**

```scala
// Read data as of specific timestamp
val config = ExtractConfig(
  sourceType = SourceType.DeltaLake,
  connectionParams = Map(
    "path" -> "s3a://data-lake/delta/users",
    "timestampAsOf" -> "2024-01-01 12:00:00"
  ),
  credentialId = Some("s3-credentials")
)

val snapshotDf = extractor.extractWithVault(config, vault)(spark)
```

**Example 4: Change Data Feed (CDC)**

```scala
// Read all changes from version 100 onwards
val config = ExtractConfig(
  sourceType = SourceType.DeltaLake,
  connectionParams = Map(
    "path" -> "s3a://data-lake/delta/users",
    "readChangeFeed" -> "true",
    "startingVersion" -> "100"
  ),
  credentialId = Some("s3-credentials")
)

val changesDf = extractor.extractWithVault(config, vault)(spark)

// changesDf has CDC columns: _change_type, _commit_version, _commit_timestamp
// _change_type: "insert", "update_preimage", "update_postimage", "delete"
```

**Example 5: Change Data Feed with Time Range**

```scala
// Read changes between two timestamps
val config = ExtractConfig(
  sourceType = SourceType.DeltaLake,
  connectionParams = Map(
    "path" -> "s3a://data-lake/delta/users",
    "readChangeFeed" -> "true",
    "startingTimestamp" -> "2024-01-01 00:00:00",
    "endingTimestamp" -> "2024-01-02 00:00:00"
  ),
  credentialId = Some("s3-credentials")
)

val dailyChangesDf = extractor.extractWithVault(config, vault)(spark)
```

**Example 6: Incremental Processing with CDC**

```scala
// Only read inserts (ignore updates and deletes)
val config = ExtractConfig(
  sourceType = SourceType.DeltaLake,
  connectionParams = Map(
    "path" -> "s3a://data-lake/delta/users",
    "readChangeFeed" -> "true",
    "startingVersion" -> "100",
    "ignoreDeletes" -> "true",
    "ignoreChanges" -> "true"
  ),
  credentialId = Some("s3-credentials")
)

val insertsDf = extractor.extractWithVault(config, vault)(spark)
```

**Use Cases**:

1. **Rollback/Recovery**: Read previous versions to recover from bad data
```scala
// Restore to last known good version
val goodData = extractor.extractWithVault(
  ExtractConfig(
    sourceType = SourceType.DeltaLake,
    connectionParams = Map("path" -> path, "versionAsOf" -> "10")
  ),
  vault
)(spark)
```

2. **Audit/Compliance**: Query historical state for audits
```scala
// "What did the data look like on 2024-01-01?"
val auditSnapshot = extractor.extractWithVault(
  ExtractConfig(
    sourceType = SourceType.DeltaLake,
    connectionParams = Map("path" -> path, "timestampAsOf" -> "2024-01-01 00:00:00")
  ),
  vault
)(spark)
```

3. **Incremental ETL**: Process only changes since last run
```scala
// Read changes since last checkpoint
val lastVersion = getLastProcessedVersion()
val changes = extractor.extractWithVault(
  ExtractConfig(
    sourceType = SourceType.DeltaLake,
    connectionParams = Map(
      "path" -> path,
      "readChangeFeed" -> "true",
      "startingVersion" -> lastVersion.toString
    )
  ),
  vault
)(spark)
```

4. **ML Model Training**: Reproduce exact dataset used for training
```scala
// Recreate training dataset from specific version
val trainingData = extractor.extractWithVault(
  ExtractConfig(
    sourceType = SourceType.DeltaLake,
    connectionParams = Map("path" -> path, "versionAsOf" -> modelMetadata.dataVersion.toString)
  ),
  vault
)(spark)
```

### KafkaExtractor

Reads data from Apache Kafka (batch or streaming).

```scala
class KafkaExtractor(streamingConfig: Option[StreamingConfig] = None) extends Extractor
```

**Configuration Parameters**:
- `topic` (required) - Kafka topic name
- `kafka.bootstrap.servers` (required) - Broker list
- `startingOffsets` - `earliest`, `latest`, or JSON offset spec
- `maxOffsetsPerTrigger` - Rate limiting
- SASL credentials via `credentialId`

**Example (Batch)**:

```scala
val extractor = new KafkaExtractor()

val config = ExtractConfig(
  sourceType = SourceType.Kafka,
  topic = Some("user-events"),
  connectionParams = Map(
    "kafka.bootstrap.servers" -> "localhost:9092",
    "subscribe" -> "user-events",
    "startingOffsets" -> "earliest",
    "endingOffsets" -> "latest"
  )
)

val df = extractor.extractWithVault(config, vault)(spark)
```

**Example (Streaming)**:

```scala
val streamingConfig = StreamingConfig(
  checkpointLocation = "/tmp/checkpoints/kafka",
  queryName = "kafka-stream",
  outputMode = OutputMode.Append,
  watermark = Some(WatermarkConfig("event_time", "10 minutes"))
)

val extractor = new KafkaExtractor(Some(streamingConfig))
val streamingDF = extractor.extractWithVault(config, vault)(spark)

// streamingDF.isStreaming == true
```

### PostgreSQLExtractor

Reads data from PostgreSQL.

```scala
class PostgreSQLExtractor extends Extractor
```

**Configuration Parameters**:
- `host` (required) - PostgreSQL hostname
- `port` (optional, default: 5432)
- `database` (required) - Database name
- `table` or `query` (required) - Table name or SQL query
- `user` (required) - Username
- Password via `credentialId`

**Example (Table)**:

```scala
val extractor = new PostgreSQLExtractor()

val config = ExtractConfig(
  sourceType = SourceType.PostgreSQL,
  table = Some("users"),
  connectionParams = Map(
    "host" -> "postgres.example.com",
    "port" -> "5432",
    "database" -> "production",
    "user" -> "readonly_user"
  ),
  credentialId = Some("postgres-password")
)

val df = extractor.extractWithVault(config, vault)(spark)
```

**Example (Query)**:

```scala
val config = ExtractConfig(
  sourceType = SourceType.PostgreSQL,
  query = Some("""
    SELECT u.*, o.order_count
    FROM users u
    LEFT JOIN (
      SELECT user_id, COUNT(*) as order_count
      FROM orders
      GROUP BY user_id
    ) o ON u.id = o.user_id
    WHERE u.created_at >= '2024-01-01'
  """),
  connectionParams = Map(
    "host" -> "postgres.example.com",
    "database" -> "production",
    "user" -> "readonly_user"
  ),
  credentialId = Some("postgres-password")
)

val df = extractor.extractWithVault(config, vault)(spark)
```

### MySQLExtractor

Similar to PostgreSQLExtractor but for MySQL databases.

```scala
class MySQLExtractor extends Extractor
```

---

## Transformers

Transformers implement the `Transformer` trait to process data.

### Transformer Trait

```scala
trait Transformer {
  def transform(df: DataFrame, config: TransformConfig): DataFrame
}
```

### FilterTransformer

Filters rows based on a condition.

```scala
class FilterTransformer extends Transformer
```

**Parameters**:
- `condition` (required) - SQL WHERE clause condition

**Example**:

```scala
val transformer = new FilterTransformer()

val config = TransformConfig(
  transformType = TransformType.Filter,
  parameters = Map(
    "condition" -> "status = 'active' AND amount > 1000 AND category IN ('A', 'B')"
  )
)

val filtered = transformer.transform(inputDF, config)
```

### AggregationTransformer

Performs grouping and aggregation.

```scala
class AggregationTransformer extends Transformer
```

**Parameters**:
- `groupByColumns` (required) - Comma-separated column names
- `aggregations` (required) - `alias:function(column)` format

**Supported Functions**:
- `sum(column)` - Sum values
- `avg(column)` - Average values
- `count(*)` - Count rows
- `count(column)` - Count non-null values
- `min(column)` - Minimum value
- `max(column)` - Maximum value
- `first(column)` - First value
- `last(column)` - Last value

**Example**:

```scala
val transformer = new AggregationTransformer()

val config = TransformConfig(
  transformType = TransformType.Aggregation,
  parameters = Map(
    "groupByColumns" -> "category,region",
    "aggregations" -> "total_sales:sum(amount),order_count:count(*),avg_order:avg(amount),max_order:max(amount)"
  )
)

val aggregated = transformer.transform(inputDF, config)

// Result columns: category, region, total_sales, order_count, avg_order, max_order
```

### JoinTransformer

Joins with another dataset.

```scala
class JoinTransformer extends Transformer
```

**Parameters**:
- `joinType` (required) - `inner`, `left`, `right`, `outer`, `cross`
- `leftColumns` (required) - Join columns from left (current) DataFrame
- `rightColumns` (required) - Join columns from right DataFrame
- `rightPath` (required) - Path to right DataFrame data
- `rightFormat` (required) - Format of right data (parquet, json, csv)

**Example**:

```scala
val transformer = new JoinTransformer()

val config = TransformConfig(
  transformType = TransformType.Join,
  parameters = Map(
    "joinType" -> "left",
    "leftColumns" -> "user_id",
    "rightColumns" -> "id",
    "rightPath" -> "s3://bucket/users/",
    "rightFormat" -> "parquet"
  )
)

val joined = transformer.transform(ordersDF, config)
```

### WindowTransformer

Performs windowed aggregations (for streaming).

```scala
class WindowTransformer extends Transformer
```

**Parameters**:
- `windowColumn` (required) - Timestamp column for windowing
- `windowDuration` (required) - Window size (e.g., "1 hour", "10 minutes")
- `slideDuration` (optional) - Slide interval (default: same as windowDuration)
- `watermark` (optional) - Late data tolerance (e.g., "10 minutes")
- `aggregations` (required) - Aggregation expressions

**Example**:

```scala
val transformer = new WindowTransformer()

val config = TransformConfig(
  transformType = TransformType.Window,
  parameters = Map(
    "windowColumn" -> "event_time",
    "windowDuration" -> "1 hour",
    "slideDuration" -> "10 minutes",
    "watermark" -> "15 minutes",
    "aggregations" -> "event_count:count(*),total_value:sum(value),unique_users:count_distinct(user_id)"
  )
)

val windowed = transformer.transform(eventsDF, config)
```

---

## Loaders

Loaders implement the `Loader` trait to write data to sinks.

### Loader Trait

```scala
trait Loader {
  def load(df: DataFrame, config: LoadConfig, mode: WriteMode): LoadResult

  def loadWithVault(df: DataFrame, config: LoadConfig, mode: WriteMode, vault: CredentialVault): LoadResult = {
    load(df, config, mode)  // Default delegates to load
  }
}
```

### LoadResult

```scala
case class LoadResult(
  recordsLoaded: Long,
  recordsFailed: Long,
  errors: Seq[String] = Seq.empty
) {
  def isSuccess: Boolean = errors.isEmpty
}

object LoadResult {
  def success(recordsLoaded: Long): LoadResult
  def failure(recordsLoaded: Long, recordsFailed: Long, errorMessage: String): LoadResult
}
```

### S3Loader

Writes data to Amazon S3.

```scala
class S3Loader extends Loader
```

**Supported Formats**: `parquet`, `json`, `csv`, `avro`, `orc`
**Write Modes**: `Append`, `Overwrite`

**Example**:

```scala
val loader = new S3Loader()

val config = LoadConfig(
  sinkType = SinkType.S3,
  path = Some("s3://output-bucket/processed/"),
  format = Some("parquet"),
  writeMode = WriteMode.Overwrite,
  credentialId = Some("aws-s3-writer")
)

val result = loader.loadWithVault(df, config, WriteMode.Overwrite, vault)

if (result.isSuccess) {
  println(s"Loaded ${result.recordsLoaded} records")
}
```

### DeltaLakeLoader

Writes data to Delta Lake tables with ACID transactions, upsert/merge, and time travel support.

```scala
class DeltaLakeLoader extends Loader
```

**Supported Features**:
- ✅ **ACID Transactions**: All-or-nothing writes (no partial failures)
- ✅ **Upsert/Merge**: Update existing + insert new records atomically
- ✅ **Schema Evolution**: Automatic schema merging
- ✅ **Time Travel**: Version history for rollback and audit
- ✅ **Optimizations**: Auto-compaction, Z-ordering, data skipping

**Write Modes**: `Append`, `Overwrite` (full/partition), `Upsert`

**Configuration Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `path` | String | Yes | Delta table path (e.g., `s3a://bucket/delta-tables/users`) |
| `mergeKeys` | JSON Array | For Upsert | Columns for merge condition (e.g., `["id"]` or `["user_id", "date"]`) |
| `updateCondition` | String | No | Condition for updates (e.g., `source.timestamp > target.timestamp`) |
| `deleteCondition` | String | No | Condition for deletes (e.g., `source.deleted = true`) |
| `mergeSchema` | Boolean | No | Enable schema evolution (`true`/`false`) |
| `partitionBy` | JSON Array | No | Partition columns (e.g., `["date", "region"]`) |
| `replaceWhere` | String | For Overwrite | Partition filter for targeted overwrite (e.g., `date >= '2024-01-01'`) |
| `optimizeWrite` | Boolean | No | Enable auto-optimization during write |
| `autoCompact` | Boolean | No | Enable auto-compaction of small files |
| `optimizeAfterWrite` | Boolean | No | Run OPTIMIZE after write completes |
| `zOrderColumns` | JSON Array | No | Columns for Z-ordering (e.g., `["date", "user_id"]`) |
| `vacuumRetentionHours` | Integer | No | Hours to retain old versions (default: 168 = 7 days) |
| `enableChangeDataFeed` | Boolean | No | Track all changes for CDC (`true`/`false`) |

**Example 1: Append with Schema Evolution**

```scala
val loader = new DeltaLakeLoader()

val config = LoadConfig(
  sinkType = SinkType.DeltaLake,
  connectionParams = Map(
    "path" -> "s3a://data-lake/delta/users",
    "mergeSchema" -> "true",
    "partitionBy" -> "[\"country\", \"created_date\"]"
  ),
  credentialId = Some("s3-credentials")
)

val result = loader.load(df, config, WriteMode.Append)
```

**Example 2: Upsert (Merge)**

```scala
// Upsert: Update existing records (matching on 'id'), insert new ones
val config = LoadConfig(
  sinkType = SinkType.DeltaLake,
  connectionParams = Map(
    "path" -> "s3a://data-lake/delta/users",
    "mergeKeys" -> "[\"id\"]"  // Primary key
  ),
  credentialId = Some("s3-credentials")
)

val result = loader.load(updatesDf, config, WriteMode.Upsert)
```

Generates SQL:
```sql
MERGE INTO target
USING source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Example 3: Conditional Upsert (Update Only If Newer)**

```scala
val config = LoadConfig(
  sinkType = SinkType.DeltaLake,
  connectionParams = Map(
    "path" -> "s3a://data-lake/delta/users",
    "mergeKeys" -> "[\"id\"]",
    "updateCondition" -> "source.timestamp > target.timestamp"  // Only update if newer
  ),
  credentialId = Some("s3-credentials")
)

val result = loader.load(updatesDf, config, WriteMode.Upsert)
```

**Example 4: Partition Overwrite**

```scala
// Overwrite only specific partitions
val config = LoadConfig(
  sinkType = SinkType.DeltaLake,
  connectionParams = Map(
    "path" -> "s3a://data-lake/delta/events",
    "replaceWhere" -> "event_date = '2024-01-01'",  // Only replace this partition
    "partitionBy" -> "[\"event_date\"]"
  ),
  credentialId = Some("s3-credentials")
)

val result = loader.load(df, config, WriteMode.Overwrite)
```

**Example 5: Optimized Write with Z-Ordering**

```scala
val config = LoadConfig(
  sinkType = SinkType.DeltaLake,
  connectionParams = Map(
    "path" -> "s3a://data-lake/delta/transactions",
    "optimizeWrite" -> "true",           // Auto-optimize during write
    "autoCompact" -> "true",             // Auto-compact small files
    "optimizeAfterWrite" -> "true",      // Run OPTIMIZE after write
    "zOrderColumns" -> "[\"date\", \"user_id\"]"  // Z-order for faster queries
  ),
  credentialId = Some("s3-credentials")
)

val result = loader.load(df, config, WriteMode.Append)
```

**Example 6: Upsert with Deletes (CDC Pattern)**

```scala
val config = LoadConfig(
  sinkType = SinkType.DeltaLake,
  connectionParams = Map(
    "path" -> "s3a://data-lake/delta/users",
    "mergeKeys" -> "[\"id\"]",
    "deleteCondition" -> "source.deleted = true"  // Delete if marked deleted
  ),
  credentialId = Some("s3-credentials")
)

// DataFrame with deleted flag
val changesDf = Seq(
  (1, "Alice", false),  // Update
  (2, "Bob", true),     // Delete
  (3, "Charlie", false) // Insert
).toDF("id", "name", "deleted")

val result = loader.load(changesDf, config, WriteMode.Upsert)
```

**Performance Tuning**:

```scala
// For high-throughput writes
val config = LoadConfig(
  sinkType = SinkType.DeltaLake,
  connectionParams = Map(
    "path" -> "s3a://data-lake/delta/high-volume",
    "optimizeWrite" -> "true",           // Fewer, larger files
    "autoCompact" -> "true",             // Prevent small file problem
    "partitionBy" -> "[\"date\"]"        // Partition for parallelism
  )
)

// For low-latency reads (optimize after write)
val config = LoadConfig(
  sinkType = SinkType.DeltaLake,
  connectionParams = Map(
    "path" -> "s3a://data-lake/delta/analytics",
    "optimizeAfterWrite" -> "true",
    "zOrderColumns" -> "[\"user_id\", \"timestamp\"]"  // Co-locate related data
  )
)
```

**Storage Management**:

```scala
// Vacuum old versions (reclaim storage)
val config = LoadConfig(
  sinkType = SinkType.DeltaLake,
  connectionParams = Map(
    "path" -> "s3a://data-lake/delta/users",
    "vacuumRetentionHours" -> "168"  // 7 days retention for time travel
  )
)
```

### PostgreSQLLoader

Writes data to PostgreSQL.

```scala
class PostgreSQLLoader(
  errorHandlingContext: Option[ErrorHandlingContext] = None,
  streamingConfig: Option[StreamingConfig] = None
) extends Loader
```

**Write Modes**: `Append`, `Overwrite`, `Upsert`
**Features**: Streaming support, exactly-once semantics, transaction support

**Example (Batch Append)**:

```scala
val loader = new PostgreSQLLoader()

val config = LoadConfig(
  sinkType = SinkType.PostgreSQL,
  table = Some("analytics_results"),
  writeMode = WriteMode.Append,
  connectionParams = Map(
    "host" -> "postgres.example.com",
    "port" -> "5432",
    "database" -> "analytics",
    "user" -> "writer",
    "batchsize" -> "5000"
  ),
  credentialId = Some("postgres-password")
)

val result = loader.loadWithVault(df, config, WriteMode.Append, vault)
```

**Example (Upsert)**:

```scala
val config = LoadConfig(
  sinkType = SinkType.PostgreSQL,
  table = Some("user_summaries"),
  writeMode = WriteMode.Upsert,
  connectionParams = Map(
    "host" -> "postgres.example.com",
    "database" -> "analytics",
    "user" -> "writer",
    "primaryKey" -> "user_id,date" // Composite primary key
  ),
  credentialId = Some("postgres-password")
)

val result = loader.loadWithVault(df, config, WriteMode.Upsert, vault)
```

**Example (Streaming with Exactly-Once)**:

```scala
val streamingConfig = StreamingConfig(
  checkpointLocation = "/tmp/checkpoints/postgres",
  queryName = "user-events-to-postgres",
  outputMode = OutputMode.Append,
  enableIdempotence = true  // Exactly-once semantics
)

val loader = new PostgreSQLLoader(streamingConfig = Some(streamingConfig))

val result = loader.loadWithVault(streamingDF, config, WriteMode.Append, vault)
```

---

## Data Quality

### DataQualityConfig

```scala
case class DataQualityConfig(
  enabled: Boolean = false,
  validateAfterExtract: Boolean = false,
  validateAfterTransform: Boolean = false,
  onFailure: String = "warn",
  rules: Seq[DataQualityRuleConfig] = Seq.empty
)
```

**OnFailure Actions**:
- `"warn"` - Log warnings but continue
- `"fail"` - Fail pipeline on validation errors
- `"skip"` - Skip invalid records (not yet implemented)

### DataQualityRuleConfig

```scala
case class DataQualityRuleConfig(
  ruleType: RuleType,
  name: String,
  columns: Seq[String],
  severity: Severity,
  parameters: Map[String, String] = Map.empty
)
```

**Rule Types**:
- `RuleType.NotNull` - Check for null values
- `RuleType.Range` - Check numeric range
- `RuleType.Pattern` - Check regex pattern
- `RuleType.Uniqueness` - Check for duplicates
- `RuleType.Custom` - Custom SQL expression

**Severity Levels**:
- `Severity.Info` - Informational only
- `Severity.Warning` - Warning (continue)
- `Severity.Error` - Error (behavior depends on onFailure)

### Examples

**Not-Null Validation**:

```scala
DataQualityRuleConfig(
  ruleType = RuleType.NotNull,
  name = "RequiredFields",
  columns = Seq("user_id", "email", "created_at"),
  severity = Severity.Error
)
```

**Range Validation**:

```scala
DataQualityRuleConfig(
  ruleType = RuleType.Range,
  name = "ValidAmount",
  columns = Seq("amount"),
  severity = Severity.Error,
  parameters = Map(
    "min" -> "0",
    "max" -> "1000000"
  )
)
```

**Pattern Validation** (Email):

```scala
DataQualityRuleConfig(
  ruleType = RuleType.Pattern,
  name = "ValidEmail",
  columns = Seq("email"),
  severity = Severity.Warning,
  parameters = Map(
    "pattern" -> "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$"
  )
)
```

**Uniqueness Check**:

```scala
DataQualityRuleConfig(
  ruleType = RuleType.Uniqueness,
  name = "UniqueUserEmails",
  columns = Seq("user_id", "email"),
  severity = Severity.Error
)
```

**Custom Rule**:

```scala
DataQualityRuleConfig(
  ruleType = RuleType.Custom,
  name = "ValidTransactionDate",
  columns = Seq.empty,
  severity = Severity.Error,
  parameters = Map(
    "expression" -> "transaction_date >= start_date AND transaction_date <= end_date"
  )
)
```

---

## Error Handling

### ErrorHandlingConfig

```scala
case class ErrorHandlingConfig(
  retryConfig: RetryConfig = RetryConfig.default,
  circuitBreakerConfig: CircuitBreakerConfig = CircuitBreakerConfig.default,
  dlqConfig: DLQConfig = DLQConfig.disabled,
  failFast: Boolean = false
)
```

### RetryConfig

```scala
case class RetryConfig(
  strategy: RetryStrategy = RetryStrategy.Exponential,
  maxAttempts: Int = 3,
  initialDelaySeconds: Int = 5,
  maxDelaySeconds: Int = 60,
  backoffMultiplier: Double = 2.0,
  jitter: Boolean = true
)
```

**Example**:

```scala
val retryConfig = RetryConfig(
  strategy = RetryStrategy.Exponential,
  maxAttempts = 5,
  initialDelaySeconds = 10,
  maxDelaySeconds = 300,
  backoffMultiplier = 2.0,
  jitter = true
)
```

### CircuitBreakerConfig

```scala
case class CircuitBreakerConfig(
  enabled: Boolean = false,
  failureThreshold: Int = 5,
  resetTimeoutSeconds: Int = 60,
  halfOpenMaxAttempts: Int = 1
)
```

**States**: `Closed` → `Open` → `HalfOpen` → `Closed`

**Example**:

```scala
val circuitBreakerConfig = CircuitBreakerConfig(
  enabled = true,
  failureThreshold = 10,
  resetTimeoutSeconds = 120,
  halfOpenMaxAttempts = 3
)
```

### DLQConfig

```scala
case class DLQConfig(
  enabled: Boolean = false,
  dlqType: String = "kafka",
  bootstrapServers: Option[String] = None,
  topic: Option[String] = None,
  path: Option[String] = None,
  bufferSize: Int = 100
)
```

**DLQ Types**: `"kafka"`, `"s3"`, `"file"`

**Example (Kafka DLQ)**:

```scala
val dlqConfig = DLQConfig(
  enabled = true,
  dlqType = "kafka",
  bootstrapServers = Some("kafka-1:9092,kafka-2:9092"),
  topic = Some("failed-events-dlq"),
  bufferSize = 1000
)
```

---

## Monitoring

### Starting Metrics Server

```scala
import com.etl.monitoring.MetricsHttpServer

val metricsServer = MetricsHttpServer(port = 9090)

// Metrics available at http://localhost:9090/metrics

metricsServer.stop()
```

### Using Metrics

```scala
import com.etl.monitoring.ETLMetrics

// Counters
ETLMetrics.recordsExtractedTotal.inc(
  labels = Map("pipeline_id" -> "my-pipeline", "source_type" -> "S3")
)

// Gauges
ETLMetrics.activePipelines.set(5.0)

// Histograms
ETLMetrics.pipelineDurationSeconds.observe(
  12.5,
  labels = Map("pipeline_id" -> "my-pipeline", "status" -> "success")
)
```

### Available Metrics

See [FEATURE_3_MONITORING_COMPLETED.md](FEATURE_3_MONITORING_COMPLETED.md) for complete metrics reference.

---

## Streaming

### StreamingConfig

```scala
case class StreamingConfig(
  checkpointLocation: String,
  queryName: String,
  outputMode: OutputMode = OutputMode.Append,
  triggerMode: TriggerMode = TriggerMode.Continuous,
  watermark: Option[WatermarkConfig] = None,
  enableIdempotence: Boolean = true,
  lateDataConfig: Option[LateDataConfig] = None
)
```

**Output Modes**:
- `OutputMode.Append` - Only new rows
- `OutputMode.Update` - Updated rows
- `OutputMode.Complete` - Full result

**Trigger Modes**:
- `TriggerMode.Continuous` - Continuous processing
- `TriggerMode.ProcessingTime("10 seconds")` - Micro-batch every 10s
- `TriggerMode.Once` - Single batch
- `TriggerMode.AvailableNow` - All available data

### Example: Kafka → Window → PostgreSQL

```scala
val streamingConfig = StreamingConfig(
  checkpointLocation = "/checkpoints/user-events",
  queryName = "user-events-hourly",
  outputMode = OutputMode.Update,
  triggerMode = TriggerMode.ProcessingTime("30 seconds"),
  watermark = Some(WatermarkConfig("event_time", "15 minutes")),
  enableIdempotence = true
)

val config = PipelineConfig(
  pipelineId = "streaming-analytics",
  mode = PipelineMode.Streaming,
  extract = ExtractConfig(...),
  transforms = Seq(
    TransformConfig(
      transformType = TransformType.Window,
      parameters = Map(
        "windowColumn" -> "event_time",
        "windowDuration" -> "1 hour",
        "slideDuration" -> "10 minutes",
        "watermark" -> "15 minutes",
        "aggregations" -> "event_count:count(*),unique_users:count_distinct(user_id)"
      )
    )
  ),
  load = LoadConfig(...),
  streamingConfigJson = Some(Map(
    "checkpointLocation" -> "/checkpoints/user-events",
    "queryName" -> "user-events-hourly",
    ...
  ))
)
```

---

## Examples

### Complete Production Pipeline

See [examples/](examples/) directory for complete examples:

- `BatchS3ToPostgres.scala` - S3 → Transform → PostgreSQL
- `StreamingKafkaToPostgres.scala` - Kafka → Window → PostgreSQL
- `DataQualityPipeline.scala` - Pipeline with quality checks
- `MonitoredPipeline.scala` - Pipeline with full monitoring

---

## Best Practices

1. **Always use CredentialVault** - Never hardcode passwords
2. **Enable Data Quality** - Validate data early
3. **Configure Error Handling** - Use retries and DLQ
4. **Monitor Everything** - Enable metrics from day one
5. **Test Pipelines** - Write integration tests
6. **Use Exactly-Once** - Enable for streaming to PostgreSQL
7. **Optimize Partitioning** - Configure shuffle partitions
8. **Handle Late Data** - Configure watermarks properly

---

## Support

- **Documentation**: [API_DOCUMENTATION.md](API_DOCUMENTATION.md)
- **Deployment**: [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md)
- **Troubleshooting**: [TROUBLESHOOTING.md](TROUBLESHOOTING.md)
- **Monitoring**: [prometheus/README.md](prometheus/README.md)

**Version**: 1.0.0
**Last Updated**: 2025-10-08
