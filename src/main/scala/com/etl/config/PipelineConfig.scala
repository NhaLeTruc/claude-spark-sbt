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
 * Retry configuration for pipeline execution.
 *
 * @param maxAttempts Maximum number of retry attempts (default: 3)
 * @param delaySeconds Delay between retry attempts in seconds (default: 5)
 */
case class RetryConfig(
  maxAttempts: Int = 3,
  delaySeconds: Int = 5
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
 * Complete pipeline configuration.
 *
 * @param pipelineId Unique pipeline identifier
 * @param name Human-readable pipeline name
 * @param extract Extraction configuration
 * @param transforms Ordered sequence of transformations
 * @param load Loading configuration
 * @param retryConfig Retry behavior settings
 * @param performanceConfig Performance tuning parameters
 * @param loggingConfig Observability settings
 */
case class PipelineConfig(
  pipelineId: String,
  name: String,
  extract: ExtractConfig,
  transforms: Seq[TransformConfig] = Seq.empty,
  load: LoadConfig,
  retryConfig: RetryConfig = RetryConfig(),
  performanceConfig: PerformanceConfig = PerformanceConfig(),
  loggingConfig: LoggingConfig = LoggingConfig()
)
