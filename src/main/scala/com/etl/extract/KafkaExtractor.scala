package com.etl.extract

import com.etl.config.ExtractConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Kafka extractor for reading from Kafka topics.
 * Supports both streaming and batch modes.
 *
 * Connection parameters:
 * - kafka.bootstrap.servers (required): Kafka broker addresses
 * - subscribe or topic (required): Topic(s) to subscribe to
 * - startingOffsets (optional): earliest, latest, or JSON offset spec (default: latest)
 * - endingOffsets (optional): For batch mode - latest or JSON offset spec
 * - maxOffsetsPerTrigger (optional): Rate limit for streaming
 * - kafka.* (optional): Additional Kafka consumer properties
 */
class KafkaExtractor extends Extractor {
  private val logger = LoggerFactory.getLogger(getClass)

  override def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Extracting from Kafka with config: ${config.connectionParams}")

    // Validate required parameters
    val bootstrapServers = config.connectionParams.getOrElse(
      "kafka.bootstrap.servers",
      throw new IllegalArgumentException(
        "kafka.bootstrap.servers is required for Kafka source. " +
          "Provide it in connectionParams."
      )
    )

    // Get topic - either from subscribe param or from topic field
    val topic = config.connectionParams.get("subscribe")
      .orElse(config.topic)
      .getOrElse(
        throw new IllegalArgumentException(
          "Either 'subscribe' in connectionParams or 'topic' field is required for Kafka source"
        )
      )

    // Check if batch mode (endingOffsets present)
    val isBatch = config.connectionParams.contains("endingOffsets")

    // Build Kafka reader
    val reader = if (isBatch) {
      logger.info(s"Reading from Kafka in batch mode: topic=$topic, bootstrap.servers=$bootstrapServers")
      spark.read.format("kafka")
    } else {
      logger.info(s"Reading from Kafka in streaming mode: topic=$topic, bootstrap.servers=$bootstrapServers")
      spark.readStream.format("kafka")
    }

    // Apply all Kafka options
    var configuredReader = reader
      .option("kafka.bootstrap.servers", bootstrapServers)
      .option("subscribe", topic)

    // Apply optional parameters
    config.connectionParams.get("startingOffsets").foreach { offset =>
      configuredReader = configuredReader.option("startingOffsets", offset)
    }

    config.connectionParams.get("endingOffsets").foreach { offset =>
      configuredReader = configuredReader.option("endingOffsets", offset)
    }

    config.connectionParams.get("maxOffsetsPerTrigger").foreach { limit =>
      configuredReader = configuredReader.option("maxOffsetsPerTrigger", limit)
    }

    // Apply any additional kafka.* parameters
    config.connectionParams.foreach {
      case (key, value) if key.startsWith("kafka.") && key != "kafka.bootstrap.servers" =>
        configuredReader = configuredReader.option(key, value)
      case _ => // Skip non-kafka parameters
    }

    // Load DataFrame
    val df = configuredReader.load()

    logger.info(
      s"Successfully created Kafka DataFrame. " +
        s"Mode: ${if (isBatch) "batch" else "streaming"}, " +
        s"Schema: ${df.schema.fieldNames.mkString(", ")}"
    )

    df
  }
}
