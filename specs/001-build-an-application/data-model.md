# Data Model: Spark-Based ETL Pipeline Framework

**Feature**: 001-build-an-application
**Date**: 2025-10-06
**Phase**: 1 - Design

## Core Domain Entities

### 1. Pipeline

**Purpose**: Represents an executable ETL workflow composed of extract, transform, and load stages.

**Fields**:
- `pipelineId: String` - Unique identifier for the pipeline
- `name: String` - Human-readable pipeline name
- `extractor: Extractor` - Strategy implementation for data extraction
- `transformers: Seq[Transformer]` - Ordered sequence of transformation steps
- `loader: Loader` - Strategy implementation for data loading
- `config: PipelineConfig` - Runtime configuration
- `state: PipelineState` - Current execution state

**State Transitions**:
```
Created → Running → (Success | Failed)
                ↓
            Retrying (up to 3 times)
                ↓
            (Success | Failed)
```

**Validation Rules**:
- `pipelineId` must be non-empty and unique
- Must have at least one extractor and one loader
- `transformers` can be empty (pass-through pipeline)
- Retry count cannot exceed configured `maxAttempts`

**Scala Representation**:
```scala
case class Pipeline(
  pipelineId: String,
  name: String,
  extractor: Extractor,
  transformers: Seq[Transformer],
  loader: Loader,
  config: PipelineConfig
) {
  def run(context: ExecutionContext): PipelineResult
}

sealed trait PipelineState
object PipelineState {
  case object Created extends PipelineState
  case object Running extends PipelineState
  case object Retrying extends PipelineState
  case object Success extends PipelineState
  case class Failed(error: Throwable, retryCount: Int) extends PipelineState
}
```

### 2. ExtractConfig

**Purpose**: Configuration for data extraction from a source system.

**Fields**:
- `sourceType: SourceType` - Kafka | PostgreSQL | MySQL | S3
- `connectionParams: Map[String, String]` - Source-specific connection details
- `query: Option[String]` - SQL query for JDBC sources
- `topic: Option[String]` - Kafka topic name
- `path: Option[String]` - S3 path or file pattern
- `schemaName: String` - Reference to Avro schema for validation
- `credentialId: Option[String]` - Vault credential identifier

**Validation Rules**:
- `sourceType` must match one of the enum values
- JDBC sources require `query` or table name in `connectionParams`
- Kafka sources require `topic` and `bootstrap.servers` in `connectionParams`
- S3 sources require `path`
- `schemaName` must exist in schema registry

**Scala Representation**:
```scala
case class ExtractConfig(
  sourceType: SourceType,
  connectionParams: Map[String, String],
  query: Option[String] = None,
  topic: Option[String] = None,
  path: Option[String] = None,
  schemaName: String,
  credentialId: Option[String] = None
)

sealed trait SourceType
object SourceType {
  case object Kafka extends SourceType
  case object PostgreSQL extends SourceType
  case object MySQL extends SourceType
  case object S3 extends SourceType
}
```

### 3. TransformConfig

**Purpose**: Configuration for a data transformation operation.

**Fields**:
- `transformType: TransformType` - Aggregation | Join | Windowing
- `parameters: Map[String, Any]` - Transform-specific parameters
  - Aggregation: `groupBy: Seq[String]`, `aggregations: Map[String, String]`
  - Join: `rightDataset: String`, `joinType: String`, `joinKeys: Seq[String]`
  - Windowing: `timeColumn: String`, `windowDuration: String`, `slideDuration: String`

**Validation Rules**:
- `transformType` determines required parameters
- Aggregation requires `groupBy` (non-empty) and `aggregations` (non-empty)
- Join requires `rightDataset`, `joinType` (inner|left|right|outer), `joinKeys` (non-empty)
- Windowing requires `timeColumn`, `windowDuration`, and `slideDuration` (valid durations like "10 seconds")

**Scala Representation**:
```scala
case class TransformConfig(
  transformType: TransformType,
  parameters: Map[String, Any]
)

sealed trait TransformType
object TransformType {
  case object Aggregation extends TransformType
  case object Join extends TransformType
  case object Windowing extends TransformType
}
```

### 4. LoadConfig

**Purpose**: Configuration for data loading to a target system.

**Fields**:
- `sinkType: SinkType` - Kafka | PostgreSQL | MySQL | S3
- `connectionParams: Map[String, String]` - Sink-specific connection details
- `table: Option[String]` - Database table name
- `topic: Option[String]` - Kafka topic name
- `path: Option[String]` - S3 output path
- `writeMode: WriteMode` - Append | Overwrite | Upsert
- `upsertKeys: Option[Seq[String]]` - Primary keys for upsert mode
- `schemaName: String` - Expected output schema
- `credentialId: Option[String]` - Vault credential identifier

**Validation Rules**:
- `sinkType` must match one of the enum values
- JDBC sinks require `table`
- Kafka sinks require `topic` and `bootstrap.servers`
- S3 sinks require `path`
- Upsert mode requires `upsertKeys` (non-empty)
- `schemaName` must match output schema

**Scala Representation**:
```scala
case class LoadConfig(
  sinkType: SinkType,
  connectionParams: Map[String, String],
  table: Option[String] = None,
  topic: Option[String] = None,
  path: Option[String] = None,
  writeMode: WriteMode,
  upsertKeys: Option[Seq[String]] = None,
  schemaName: String,
  credentialId: Option[String] = None
)

sealed trait SinkType
object SinkType {
  case object Kafka extends SinkType
  case object PostgreSQL extends SinkType
  case object MySQL extends SinkType
  case object S3 extends SinkType
}
```

### 5. WriteMode

**Purpose**: Enumeration of write strategies for data loading.

**Values**:
- `Append` - Add new records to existing data
- `Overwrite` - Replace all existing data
- `Upsert` - Insert new records, update existing records based on keys

**Scala Representation**:
```scala
sealed trait WriteMode
object WriteMode {
  case object Append extends WriteMode
  case object Overwrite extends WriteMode
  case object Upsert extends WriteMode
}
```

### 6. PipelineConfig

**Purpose**: Runtime configuration for pipeline execution.

**Fields**:
- `retryConfig: RetryConfig` - Retry behavior settings
- `performanceConfig: PerformanceConfig` - Tuning parameters
- `loggingConfig: LoggingConfig` - Observability settings

**Scala Representation**:
```scala
case class PipelineConfig(
  retryConfig: RetryConfig,
  performanceConfig: PerformanceConfig,
  loggingConfig: LoggingConfig
)

case class RetryConfig(
  maxAttempts: Int = 3,
  delaySeconds: Int = 5
)

case class PerformanceConfig(
  shufflePartitions: Option[Int] = None,
  broadcastThreshold: Option[Long] = None
)

case class LoggingConfig(
  logLevel: String = "INFO",
  includeMetrics: Boolean = true
)
```

### 7. AvroSchema

**Purpose**: Metadata for Avro schema definitions.

**Fields**:
- `name: String` - Schema name (unique identifier)
- `namespace: String` - Schema namespace
- `fields: Seq[AvroField]` - Schema field definitions
- `jsonDefinition: String` - JSON Avro schema content

**Validation Rules**:
- `name` must be non-empty and follow Avro naming rules
- `fields` must be non-empty
- `jsonDefinition` must be valid Avro schema JSON

**Scala Representation**:
```scala
case class AvroSchema(
  name: String,
  namespace: String,
  fields: Seq[AvroField],
  jsonDefinition: String
)

case class AvroField(
  name: String,
  fieldType: String, // "string", "int", "long", "double", etc.
  nullable: Boolean = false
)
```

### 8. ExecutionMetrics

**Purpose**: Telemetry data collected during pipeline execution.

**Fields**:
- `pipelineId: String` - Associated pipeline
- `executionId: String` - Unique execution identifier
- `startTime: Long` - Execution start timestamp (epoch millis)
- `endTime: Option[Long]` - Execution end timestamp
- `recordsExtracted: Long` - Count of records extracted
- `recordsTransformed: Long` - Count of records after transformations
- `recordsLoaded: Long` - Count of records successfully loaded
- `recordsFailed: Long` - Count of records that failed validation or loading
- `retryCount: Int` - Number of retry attempts
- `errors: Seq[String]` - Error messages encountered

**Validation Rules**:
- `startTime` must be set
- `endTime` must be >= `startTime` when set
- Record counts must be non-negative
- `retryCount` must be between 0 and `maxAttempts`

**Scala Representation**:
```scala
case class ExecutionMetrics(
  pipelineId: String,
  executionId: String,
  startTime: Long,
  endTime: Option[Long],
  recordsExtracted: Long,
  recordsTransformed: Long,
  recordsLoaded: Long,
  recordsFailed: Long,
  retryCount: Int,
  errors: Seq[String]
) {
  def duration: Option[Long] = endTime.map(_ - startTime)
  def successRate: Double = if (recordsExtracted > 0) recordsLoaded.toDouble / recordsExtracted else 0.0
}
```

### 9. PipelineResult

**Purpose**: Outcome of a pipeline execution.

**Fields**:
- `success: Boolean` - Whether pipeline completed successfully
- `metrics: ExecutionMetrics` - Execution telemetry
- `error: Option[Throwable]` - Error if failed

**Scala Representation**:
```scala
sealed trait PipelineResult {
  def metrics: ExecutionMetrics
}

case class PipelineSuccess(metrics: ExecutionMetrics) extends PipelineResult

case class PipelineFailure(
  metrics: ExecutionMetrics,
  error: Throwable
) extends PipelineResult
```

## Entity Relationships

```
Pipeline 1 --> 1 PipelineConfig
Pipeline 1 --> 1 ExtractConfig
Pipeline 1 --> 0..N TransformConfig
Pipeline 1 --> 1 LoadConfig
Pipeline 1 --> 1 PipelineResult (after execution)

PipelineResult 1 --> 1 ExecutionMetrics

ExtractConfig N --> 1 AvroSchema (via schemaName)
LoadConfig N --> 1 AvroSchema (via schemaName)

PipelineConfig 1 --> 1 RetryConfig
PipelineConfig 1 --> 1 PerformanceConfig
PipelineConfig 1 --> 1 LoggingConfig
```

## Schema Evolution Strategy

- Avro schemas support backward and forward compatibility
- Schema registry maintains versioned schemas
- Pipeline validates input/output data against registered schemas
- Schema mismatches halt execution with detailed error messages
- New optional fields can be added without breaking existing pipelines

## Summary

Data model defines 9 core entities with clear responsibilities and relationships. Pipeline composes Extract → Transform → Load stages via Strategy pattern. Configurations are immutable case classes loaded from JSON. Avro schemas externalized for validation. Metrics track execution telemetry. All entities support type-safe operations and exhaustive pattern matching for state management.
