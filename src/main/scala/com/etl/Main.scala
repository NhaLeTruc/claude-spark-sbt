package com.etl

import com.etl.config._
import com.etl.core.{ETLPipeline, PipelineExecutor}
import com.etl.extract._
import com.etl.load._
import com.etl.transform._
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Main entry point for ETL pipeline execution.
 *
 * Usage:
 *   spark-submit --class com.etl.Main \
 *     etl-pipeline-assembly.jar \
 *     --config /path/to/pipeline-config.json \
 *     --mode batch
 *
 * Arguments:
 *   --config: Path to pipeline configuration JSON file
 *   --mode: Execution mode (batch or streaming)
 */
object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Starting ETL Pipeline Application")

    try {
      // Parse command-line arguments
      val parsedArgs = parseArguments(args)
      val configPath = parsedArgs.getOrElse("config", {
        logger.error("Missing required argument: --config")
        printUsage()
        System.exit(1)
        ""
      })

      val mode = parsedArgs.getOrElse("mode", "batch")

      logger.info(s"Configuration path: $configPath")
      logger.info(s"Execution mode: $mode")

      // Load pipeline configuration
      val config = loadConfiguration(configPath)
      logger.info(s"Loaded pipeline configuration: ${config.pipelineId}")

      // Initialize SparkSession
      val spark = initializeSparkSession(config, mode)

      try {
        // Build pipeline from configuration
        val pipeline = buildPipeline(config)
        logger.info("Pipeline constructed successfully")

        // Execute pipeline
        val executor = new PipelineExecutor()
        val result = executor.execute(pipeline, config)(spark)

        // Log final result
        if (result.isSuccess) {
          logger.info(
            s"Pipeline execution completed successfully. " +
              s"Records: extracted=${result.metrics.recordsExtracted}, " +
              s"transformed=${result.metrics.recordsTransformed}, " +
              s"loaded=${result.metrics.recordsLoaded}, " +
              s"duration=${result.metrics.duration}ms"
          )
          System.exit(0)
        } else {
          logger.error(
            s"Pipeline execution failed. " +
              s"Errors: ${result.metrics.errors.mkString(", ")}"
          )
          System.exit(1)
        }
      } finally {
        spark.stop()
        logger.info("SparkSession stopped")
      }

    } catch {
      case e: Exception =>
        logger.error(s"Fatal error: ${e.getMessage}", e)
        System.exit(1)
    }
  }

  /**
   * Parse command-line arguments.
   *
   * @param args Command-line arguments
   * @return Map of argument key-value pairs
   */
  private def parseArguments(args: Array[String]): Map[String, String] = {
    args.sliding(2, 2).collect {
      case Array(key, value) if key.startsWith("--") =>
        key.stripPrefix("--") -> value
    }.toMap
  }

  /**
   * Load pipeline configuration from JSON file.
   *
   * @param configPath Path to configuration file
   * @return PipelineConfig
   */
  private def loadConfiguration(configPath: String): PipelineConfig = {
    logger.info(s"Loading configuration from: $configPath")

    ConfigLoader.loadFromFile(configPath) match {
      case Right(config) =>
        logger.info("Configuration loaded successfully")

        // Validate configuration
        val validationErrors = ConfigLoader.validate(config)
        if (validationErrors.nonEmpty) {
          val errorMsg = s"Configuration validation failed: ${validationErrors.mkString(", ")}"
          logger.error(errorMsg)
          throw new IllegalArgumentException(errorMsg)
        }

        config

      case Left(error) =>
        val errorMsg = s"Failed to load configuration: $error"
        logger.error(errorMsg)
        throw new IllegalArgumentException(errorMsg)
    }
  }

  /**
   * Initialize SparkSession based on mode and configuration.
   *
   * @param config Pipeline configuration
   * @param mode Execution mode (batch or streaming)
   * @return SparkSession
   */
  private def initializeSparkSession(config: PipelineConfig, mode: String): SparkSession = {
    logger.info(s"Initializing SparkSession for mode: $mode")

    val builder = SparkSession
      .builder()
      .appName(s"ETL-Pipeline-${config.pipelineId}")

    // Configure for streaming if needed
    if (mode.toLowerCase == "streaming") {
      builder.config("spark.streaming.stopGracefullyOnShutdown", "true")
    }

    // Apply performance configuration
    builder
      .config("spark.sql.shuffle.partitions", config.performance.parallelism.toString)
      .config("spark.default.parallelism", config.performance.parallelism.toString)

    val spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel(config.logging.level)
    logger.info(s"SparkSession initialized: ${spark.version}")

    spark
  }

  /**
   * Build ETL pipeline from configuration using factory pattern.
   *
   * @param config Pipeline configuration
   * @return ETLPipeline
   */
  private def buildPipeline(config: PipelineConfig): ETLPipeline = {
    logger.info("Building pipeline from configuration")

    // Create extractor based on source type
    val extractor = createExtractor(config.extract)
    logger.info(s"Created extractor: ${config.extract.sourceType}")

    // Create transformers from transform configs
    val transformers = config.transforms.map(createTransformer)
    logger.info(s"Created ${transformers.size} transformer(s)")

    // Create loader based on sink type
    val loader = createLoader(config.load)
    logger.info(s"Created loader: ${config.load.sinkType}")

    // Build pipeline
    ETLPipeline(extractor, transformers, loader)
  }

  /**
   * Factory method to create Extractor based on source type.
   *
   * @param config Extract configuration
   * @return Extractor instance
   */
  private def createExtractor(config: ExtractConfig): Extractor = {
    config.sourceType match {
      case SourceType.Kafka =>
        new KafkaExtractor()

      case SourceType.PostgreSQL =>
        new PostgreSQLExtractor()

      case SourceType.MySQL =>
        new MySQLExtractor()

      case SourceType.S3 =>
        new S3Extractor()

      case other =>
        throw new IllegalArgumentException(s"Unsupported source type: $other")
    }
  }

  /**
   * Factory method to create Transformer based on transform type.
   *
   * @param config Transform configuration
   * @return Transformer instance
   */
  private def createTransformer(config: TransformConfig): Transformer = {
    config.transformType match {
      case TransformType.Aggregation =>
        new AggregationTransformer()

      case TransformType.Join =>
        // For join, we need a right DataFrame - this would be loaded from config
        // For now, throw an error indicating join needs special handling
        throw new IllegalArgumentException(
          "Join transformer requires special initialization with right DataFrame. " +
            "Use pipeline builder with explicit right dataset."
        )

      case TransformType.Window =>
        new WindowTransformer()

      case other =>
        throw new IllegalArgumentException(s"Unsupported transform type: $other")
    }
  }

  /**
   * Factory method to create Loader based on sink type.
   *
   * @param config Load configuration
   * @return Loader instance
   */
  private def createLoader(config: LoadConfig): Loader = {
    config.sinkType match {
      case SinkType.Kafka =>
        new KafkaLoader()

      case SinkType.PostgreSQL =>
        new PostgreSQLLoader()

      case SinkType.MySQL =>
        new MySQLLoader()

      case SinkType.S3 =>
        new S3Loader()

      case other =>
        throw new IllegalArgumentException(s"Unsupported sink type: $other")
    }
  }

  /**
   * Print usage information.
   */
  private def printUsage(): Unit = {
    println(
      """
        |Usage: spark-submit --class com.etl.Main etl-pipeline-assembly.jar [OPTIONS]
        |
        |Options:
        |  --config PATH    Path to pipeline configuration JSON file (required)
        |  --mode MODE      Execution mode: batch or streaming (default: batch)
        |
        |Example:
        |  spark-submit --class com.etl.Main \
        |    --master local[4] \
        |    etl-pipeline-assembly.jar \
        |    --config /path/to/pipeline-config.json \
        |    --mode batch
        |""".stripMargin
    )
  }
}
