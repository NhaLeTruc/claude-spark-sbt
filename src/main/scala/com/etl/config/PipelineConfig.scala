package com.etl.config

/**
 * Source and sink type enumerations for strategy pattern implementations.
 */
sealed trait SourceType
object SourceType {
  case object Kafka extends SourceType
  case object PostgreSQL extends SourceType
  case object MySQL extends SourceType
  case object S3 extends SourceType

  def fromString(s: String): SourceType = s.toLowerCase match {
    case "kafka"      => Kafka
    case "postgresql" => PostgreSQL
    case "mysql"      => MySQL
    case "s3"         => S3
    case _            => throw new IllegalArgumentException(s"Unknown source type: $s")
  }
}

sealed trait SinkType
object SinkType {
  case object Kafka extends SinkType
  case object PostgreSQL extends SinkType
  case object MySQL extends SinkType
  case object S3 extends SinkType

  def fromString(s: String): SinkType = s.toLowerCase match {
    case "kafka"      => Kafka
    case "postgresql" => PostgreSQL
    case "mysql"      => MySQL
    case "s3"         => S3
    case _            => throw new IllegalArgumentException(s"Unknown sink type: $s")
  }
}

sealed trait TransformType
object TransformType {
  case object Aggregation extends TransformType
  case object Join extends TransformType
  case object Windowing extends TransformType

  def fromString(s: String): TransformType = s.toLowerCase match {
    case "aggregation" => Aggregation
    case "join"        => Join
    case "windowing"   => Windowing
    case _             => throw new IllegalArgumentException(s"Unknown transform type: $s")
  }
}

/**
 * Configuration for data extraction.
 *
 * @param sourceType Type of data source (Kafka, PostgreSQL, MySQL, S3)
 * @param connectionParams Source-specific connection parameters
 * @param query SQL query for JDBC sources
 * @param topic Kafka topic name
 * @param path S3 path or file pattern
 * @param schemaName Reference to Avro schema for validation
 * @param credentialId Vault credential identifier for authentication
 */
case class ExtractConfig(
  sourceType: SourceType,
  connectionParams: Map[String, String],
  query: Option[String] = None,
  topic: Option[String] = None,
  path: Option[String] = None,
  schemaName: String,
  credentialId: Option[String] = None
)

/**
 * Configuration for data transformation.
 *
 * @param transformType Type of transformation (Aggregation, Join, Windowing)
 * @param parameters Transform-specific parameters as key-value map
 */
case class TransformConfig(
  transformType: TransformType,
  parameters: Map[String, Any]
)

/**
 * Configuration for data loading.
 *
 * @param sinkType Type of data sink (Kafka, PostgreSQL, MySQL, S3)
 * @param connectionParams Sink-specific connection parameters
 * @param table Database table name
 * @param topic Kafka topic name
 * @param path S3 output path
 * @param writeMode Write strategy (Append, Overwrite, Upsert)
 * @param upsertKeys Primary keys for upsert mode
 * @param schemaName Expected output schema
 * @param credentialId Vault credential identifier for authentication
 */
case class LoadConfig(
  sinkType: SinkType,
  connectionParams: Map[String, String],
  table: Option[String] = None,
  topic: Option[String] = None,
  path: Option[String] = None,
  writeMode: String, // Will be converted to WriteMode enum
  upsertKeys: Option[Seq[String]] = None,
  schemaName: String,
  credentialId: Option[String] = None
)

/**
 * Retry strategy type for error handling.
 */
sealed trait RetryStrategyType
object RetryStrategyType {
  case object ExponentialBackoff extends RetryStrategyType
  case object FixedDelay extends RetryStrategyType
  case object NoRetry extends RetryStrategyType

  def fromString(s: String): RetryStrategyType = s.toLowerCase match {
    case "exponential" | "exponentialbackoff" => ExponentialBackoff
    case "fixed" | "fixeddelay"               => FixedDelay
    case "none" | "noretry"                   => NoRetry
    case _ => throw new IllegalArgumentException(s"Unknown retry strategy type: $s")
  }
}

/**
 * Retry configuration for pipeline execution.
 *
 * @param strategy Type of retry strategy (ExponentialBackoff, FixedDelay, NoRetry)
 * @param maxAttempts Maximum number of retry attempts (default: 3)
 * @param initialDelaySeconds Initial delay between retry attempts in seconds (default: 5)
 * @param maxDelaySeconds Maximum delay for exponential backoff in seconds (default: 60)
 * @param backoffMultiplier Multiplier for exponential backoff (default: 2.0)
 * @param jitter Whether to add random jitter to delays (default: true)
 */
case class RetryConfig(
  strategy: RetryStrategyType = RetryStrategyType.ExponentialBackoff,
  maxAttempts: Int = 3,
  initialDelaySeconds: Int = 5,
  maxDelaySeconds: Int = 60,
  backoffMultiplier: Double = 2.0,
  jitter: Boolean = true
)

/**
 * Performance tuning configuration.
 *
 * @param shufflePartitions Number of partitions for shuffle operations
 * @param broadcastThreshold Size threshold for broadcast joins
 */
case class PerformanceConfig(
  shufflePartitions: Option[Int] = None,
  broadcastThreshold: Option[Long] = None
)

/**
 * Circuit breaker configuration.
 *
 * @param enabled Whether circuit breaker is enabled (default: true)
 * @param failureThreshold Number of failures before opening circuit (default: 5)
 * @param resetTimeoutSeconds Time in seconds before attempting reset (default: 60)
 * @param halfOpenMaxAttempts Max test attempts in half-open state (default: 1)
 */
case class CircuitBreakerConfig(
  enabled: Boolean = true,
  failureThreshold: Int = 5,
  resetTimeoutSeconds: Int = 60,
  halfOpenMaxAttempts: Int = 1
)

/**
 * Dead letter queue type.
 */
sealed trait DLQType
object DLQType {
  case object Kafka extends DLQType
  case object S3 extends DLQType
  case object Logging extends DLQType
  case object None extends DLQType

  def fromString(s: String): DLQType = s.toLowerCase match {
    case "kafka"   => Kafka
    case "s3"      => S3
    case "logging" => Logging
    case "none"    => None
    case _ => throw new IllegalArgumentException(s"Unknown DLQ type: $s")
  }
}

/**
 * Dead letter queue configuration.
 *
 * @param dlqType Type of DLQ implementation (Kafka, S3, Logging, None)
 * @param bootstrapServers Kafka bootstrap servers (for Kafka DLQ)
 * @param topic Kafka DLQ topic name (for Kafka DLQ)
 * @param bucketPath S3 bucket path (for S3 DLQ, e.g., s3a://my-bucket/dlq/)
 * @param partitionBy S3 partitioning strategy: date, hour, pipeline, stage (default: date)
 * @param bufferSize Number of records to buffer before flushing (default: 100)
 * @param format Output format for S3: parquet, json (default: parquet)
 * @param producerConfig Additional Kafka producer configuration
 */
case class DLQConfig(
  dlqType: DLQType = DLQType.None,
  bootstrapServers: Option[String] = None,
  topic: Option[String] = None,
  bucketPath: Option[String] = None,
  partitionBy: String = "date",
  bufferSize: Int = 100,
  format: String = "parquet",
  producerConfig: Map[String, String] = Map.empty
) {
  // Validation
  if (dlqType == DLQType.Kafka) {
    require(bootstrapServers.isDefined, "bootstrapServers is required for Kafka DLQ")
    require(topic.isDefined, "topic is required for Kafka DLQ")
  }
  if (dlqType == DLQType.S3) {
    require(bucketPath.isDefined, "bucketPath is required for S3 DLQ")
  }
}

/**
 * Error handling configuration.
 *
 * @param retryConfig Retry strategy configuration
 * @param circuitBreakerConfig Circuit breaker configuration
 * @param dlqConfig Dead letter queue configuration
 * @param failFast Whether to fail immediately on errors (default: false)
 */
case class ErrorHandlingConfig(
  retryConfig: RetryConfig = RetryConfig(),
  circuitBreakerConfig: CircuitBreakerConfig = CircuitBreakerConfig(),
  dlqConfig: DLQConfig = DLQConfig(),
  failFast: Boolean = false
)

/**
 * Data quality rule type.
 */
sealed trait DataQualityRuleType
object DataQualityRuleType {
  case object NotNull extends DataQualityRuleType
  case object Range extends DataQualityRuleType
  case object Unique extends DataQualityRuleType

  def fromString(s: String): DataQualityRuleType = s.toLowerCase match {
    case "not_null" | "notnull" => NotNull
    case "range"                => Range
    case "unique"               => Unique
    case _ => throw new IllegalArgumentException(s"Unknown data quality rule type: $s")
  }
}

/**
 * Data quality rule configuration.
 *
 * @param ruleType Type of rule (NotNull, Range, Unique)
 * @param name Optional custom name for the rule
 * @param columns Columns to validate
 * @param severity Severity level: error, warning, info (default: error)
 * @param parameters Rule-specific parameters (e.g., min/max for range)
 */
case class DataQualityRuleConfig(
  ruleType: DataQualityRuleType,
  name: Option[String] = None,
  columns: Seq[String],
  severity: String = "error",
  parameters: Map[String, Any] = Map.empty
) {
  // Validation
  require(columns.nonEmpty, "Data quality rule requires at least one column")
  require(
    Seq("error", "warning", "info").contains(severity.toLowerCase),
    s"Invalid severity: $severity. Must be error, warning, or info"
  )
}

/**
 * Data quality validation configuration.
 *
 * @param enabled Whether data quality validation is enabled (default: false)
 * @param rules List of data quality rules to execute
 * @param onFailure Action on failure: abort, continue, warn (default: abort)
 * @param validateAfterExtract Validate data after extraction (default: false)
 * @param validateAfterTransform Validate data after transformation (default: true)
 */
case class DataQualityConfig(
  enabled: Boolean = false,
  rules: Seq[DataQualityRuleConfig] = Seq.empty,
  onFailure: String = "abort",
  validateAfterExtract: Boolean = false,
  validateAfterTransform: Boolean = true
) {
  // Validation
  require(
    Seq("abort", "continue", "warn").contains(onFailure.toLowerCase),
    s"Invalid onFailure: $onFailure. Must be abort, continue, or warn"
  )
}

/**
 * Logging configuration.
 *
 * @param logLevel Logging level (INFO, DEBUG, WARN, ERROR)
 * @param includeMetrics Whether to include metrics in logs
 */
case class LoggingConfig(
  logLevel: String = "INFO",
  includeMetrics: Boolean = true
)

/**
 * Streaming configuration type for pipeline mode.
 */
sealed trait PipelineMode
object PipelineMode {
  case object Batch extends PipelineMode
  case object Streaming extends PipelineMode

  def fromString(s: String): PipelineMode = s.toLowerCase match {
    case "batch"     => Batch
    case "streaming" => Streaming
    case _ => throw new IllegalArgumentException(s"Unknown pipeline mode: $s. Valid values: batch, streaming")
  }
}

/**
 * Complete pipeline configuration.
 *
 * @param pipelineId Unique pipeline identifier
 * @param name Human-readable pipeline name
 * @param extract Extraction configuration
 * @param transforms Ordered sequence of transformations
 * @param load Loading configuration
 * @param errorHandlingConfig Error handling and recovery settings
 * @param dataQualityConfig Data quality validation settings
 * @param performanceConfig Performance tuning parameters
 * @param loggingConfig Observability settings
 * @param mode Pipeline execution mode (Batch or Streaming)
 * @param streamingConfigJson Optional streaming configuration (JSON string or map)
 */
case class PipelineConfig(
  pipelineId: String,
  name: String,
  extract: ExtractConfig,
  transforms: Seq[TransformConfig] = Seq.empty,
  load: LoadConfig,
  errorHandlingConfig: ErrorHandlingConfig = ErrorHandlingConfig(),
  dataQualityConfig: DataQualityConfig = DataQualityConfig(),
  performanceConfig: PerformanceConfig = PerformanceConfig(),
  loggingConfig: LoggingConfig = LoggingConfig(),
  mode: PipelineMode = PipelineMode.Batch,
  streamingConfigJson: Option[Map[String, Any]] = None
) {
  // Backward compatibility: expose retry config for existing code
  @deprecated("Use errorHandlingConfig.retryConfig instead", "1.1.0")
  def retryConfig: RetryConfig = errorHandlingConfig.retryConfig

  /**
   * Check if this is a streaming pipeline.
   */
  def isStreaming: Boolean = mode == PipelineMode.Streaming

  /**
   * Check if streaming configuration is present.
   */
  def hasStreamingConfig: Boolean = streamingConfigJson.isDefined

  /**
   * Get parsed StreamingConfig from JSON configuration.
   *
   * @return Some(StreamingConfig) if present and parseable, None otherwise
   * @throws IllegalArgumentException if JSON is malformed
   */
  def getStreamingConfig: Option[com.etl.streaming.StreamingConfig] = {
    streamingConfigJson.map { json =>
      com.etl.streaming.StreamingConfigFactory.fromJson(json)
    }
  }
}
