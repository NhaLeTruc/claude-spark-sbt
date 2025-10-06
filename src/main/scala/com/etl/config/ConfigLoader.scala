package com.etl.config

import org.slf4j.LoggerFactory
import play.api.libs.json._

import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * Configuration loader for parsing pipeline configs from JSON.
 * Uses play-json for parsing and validation.
 */
object ConfigLoader {
  private val logger = LoggerFactory.getLogger(getClass)

  // JSON Format instances for case classes
  implicit val sourceTypeFormat: Format[SourceType] = new Format[SourceType] {
    def reads(json: JsValue): JsResult[SourceType] = json match {
      case JsString(s) =>
        Try(SourceType.fromString(s)) match {
          case Success(sourceType) => JsSuccess(sourceType)
          case Failure(e)          => JsError(s"Invalid source type: $s - ${e.getMessage}")
        }
      case _ => JsError("Expected string for source type")
    }
    def writes(o: SourceType): JsValue = JsString(o.toString.toLowerCase)
  }

  implicit val sinkTypeFormat: Format[SinkType] = new Format[SinkType] {
    def reads(json: JsValue): JsResult[SinkType] = json match {
      case JsString(s) =>
        Try(SinkType.fromString(s)) match {
          case Success(sinkType) => JsSuccess(sinkType)
          case Failure(e)        => JsError(s"Invalid sink type: $s - ${e.getMessage}")
        }
      case _ => JsError("Expected string for sink type")
    }
    def writes(o: SinkType): JsValue = JsString(o.toString.toLowerCase)
  }

  implicit val transformTypeFormat: Format[TransformType] = new Format[TransformType] {
    def reads(json: JsValue): JsResult[TransformType] = json match {
      case JsString(s) =>
        Try(TransformType.fromString(s)) match {
          case Success(transformType) => JsSuccess(transformType)
          case Failure(e)             => JsError(s"Invalid transform type: $s - ${e.getMessage}")
        }
      case _ => JsError("Expected string for transform type")
    }
    def writes(o: TransformType): JsValue = JsString(o.toString.toLowerCase)
  }

  implicit val extractConfigFormat: Format[ExtractConfig] = Json.format[ExtractConfig]
  implicit val transformConfigFormat: Format[TransformConfig] = Json.format[TransformConfig]
  implicit val loadConfigFormat: Format[LoadConfig] = Json.format[LoadConfig]
  implicit val retryConfigFormat: Format[RetryConfig] = Json.format[RetryConfig]
  implicit val performanceConfigFormat: Format[PerformanceConfig] = Json.format[PerformanceConfig]
  implicit val loggingConfigFormat: Format[LoggingConfig] = Json.format[LoggingConfig]
  implicit val pipelineConfigFormat: Format[PipelineConfig] = Json.format[PipelineConfig]

  /**
   * Parse pipeline configuration from JSON string.
   *
   * @param jsonString JSON configuration string
   * @return Either error message or PipelineConfig
   */
  def parseJson(jsonString: String): Either[String, PipelineConfig] = {
    Try {
      val json = Json.parse(jsonString)
      json.validate[PipelineConfig] match {
        case JsSuccess(config, _) =>
          logger.info(s"Successfully parsed config for pipeline: ${config.pipelineId}")
          Right(config)
        case JsError(errors) =>
          val errorMsg = errors
            .map { case (path, validationErrors) =>
              s"${path.toString()}: ${validationErrors.map(_.message).mkString(", ")}"
            }
            .mkString("; ")
          Left(s"Config validation failed: $errorMsg")
      }
    } match {
      case Success(result) => result
      case Failure(e)      => Left(s"JSON parse error: ${e.getMessage}")
    }
  }

  /**
   * Load pipeline configuration from file.
   *
   * @param filePath Path to JSON config file
   * @return Either error message or PipelineConfig
   */
  def loadFromFile(filePath: String): Either[String, PipelineConfig] = {
    Try {
      val source = Source.fromFile(filePath)
      try {
        val jsonString = source.mkString
        parseJson(jsonString)
      } finally {
        source.close()
      }
    } match {
      case Success(result) => result
      case Failure(e)      => Left(s"Failed to read file $filePath: ${e.getMessage}")
    }
  }

  /**
   * Load pipeline configuration from classpath resource.
   *
   * @param resourcePath Resource path (e.g., "configs/pipeline.json")
   * @return Either error message or PipelineConfig
   */
  def loadFromResource(resourcePath: String): Either[String, PipelineConfig] = {
    Try {
      val inputStream = getClass.getClassLoader.getResourceAsStream(resourcePath)
      if (inputStream == null) {
        throw new RuntimeException(s"Resource not found: $resourcePath")
      }
      try {
        val jsonString = Source.fromInputStream(inputStream).mkString
        parseJson(jsonString)
      } finally {
        inputStream.close()
      }
    } match {
      case Success(result) => result
      case Failure(e)      => Left(s"Failed to read resource $resourcePath: ${e.getMessage}")
    }
  }

  /**
   * Validate configuration for common issues.
   *
   * @param config Pipeline configuration
   * @return List of validation warnings (empty if all OK)
   */
  def validate(config: PipelineConfig): Seq[String] = {
    val warnings = scala.collection.mutable.ArrayBuffer[String]()

    // Check extract config
    config.extract.sourceType match {
      case SourceType.Kafka =>
        if (config.extract.topic.isEmpty) {
          warnings += "Kafka source requires 'topic' field"
        }
      case SourceType.PostgreSQL | SourceType.MySQL =>
        if (config.extract.query.isEmpty) {
          warnings += "JDBC source should have 'query' field"
        }
      case SourceType.S3 =>
        if (config.extract.path.isEmpty) {
          warnings += "S3 source requires 'path' field"
        }
    }

    // Check load config
    config.load.sinkType match {
      case SinkType.Kafka =>
        if (config.load.topic.isEmpty) {
          warnings += "Kafka sink requires 'topic' field"
        }
      case SinkType.PostgreSQL | SinkType.MySQL =>
        if (config.load.table.isEmpty) {
          warnings += "JDBC sink requires 'table' field"
        }
      case SinkType.S3 =>
        if (config.load.path.isEmpty) {
          warnings += "S3 sink requires 'path' field"
        }
    }

    // Check upsert mode
    if (config.load.writeMode.toLowerCase == "upsert" && config.load.upsertKeys.isEmpty) {
      warnings += "Upsert write mode requires 'upsertKeys' field"
    }

    // Check retry config
    if (config.retryConfig.maxAttempts < 0) {
      warnings += "Retry maxAttempts must be non-negative"
    }

    warnings.toSeq
  }
}
