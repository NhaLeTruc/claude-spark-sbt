package com.etl.load

import com.etl.config.LoadConfig
import com.etl.model.{LoadResult, WriteMode}
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
 * Kafka loader for writing to Kafka topics.
 * Supports both batch and streaming writes.
 *
 * Connection parameters:
 * - kafka.bootstrap.servers (required): Kafka broker addresses
 * - topic: Target topic (can also use 'topic' field in LoadConfig)
 * - checkpointLocation (optional): For streaming writes
 * - kafka.* (optional): Additional Kafka producer properties
 *
 * Note: Kafka only supports Append mode. Overwrite and Upsert are not supported.
 * DataFrame must contain 'key' and 'value' columns.
 */
class KafkaLoader extends Loader {
  private val logger = LoggerFactory.getLogger(getClass)

  override def load(df: DataFrame, config: LoadConfig, mode: WriteMode): LoadResult = {
    logger.info(s"Loading to Kafka with config: ${config.connectionParams}, mode: $mode")

    // Validate write mode - Kafka only supports Append
    if (mode != WriteMode.Append) {
      throw new IllegalArgumentException(
        s"Kafka only supports Append mode. Requested mode: $mode"
      )
    }

    // Validate required parameters
    val bootstrapServers = config.connectionParams.getOrElse(
      "kafka.bootstrap.servers",
      throw new IllegalArgumentException(
        "kafka.bootstrap.servers is required for Kafka sink"
      )
    )

    val topic = config.connectionParams.get("topic")
      .orElse(config.topic)
      .getOrElse(
        throw new IllegalArgumentException(
          "Either 'topic' in connectionParams or 'topic' field is required for Kafka sink"
        )
      )

    // Validate DataFrame has required columns
    val schema = df.schema
    val hasKey = schema.fieldNames.contains("key")
    val hasValue = schema.fieldNames.contains("value")

    if (!hasKey || !hasValue) {
      throw new IllegalArgumentException(
        s"DataFrame must contain 'key' and 'value' columns for Kafka write. " +
          s"Found columns: ${schema.fieldNames.mkString(", ")}"
      )
    }

    logger.info(s"Writing to Kafka: topic=$topic, bootstrap.servers=$bootstrapServers")

    try {
      if (df.isStreaming) {
        // Streaming write
        val checkpointLocation = config.connectionParams.getOrElse(
          "checkpointLocation",
          throw new IllegalArgumentException(
            "checkpointLocation is required for streaming Kafka writes"
          )
        )

        logger.info(s"Starting streaming write to Kafka with checkpoint: $checkpointLocation")

        var writer = df.writeStream
          .format("kafka")
          .option("kafka.bootstrap.servers", bootstrapServers)
          .option("topic", topic)
          .option("checkpointLocation", checkpointLocation)

        // Apply additional Kafka options
        config.connectionParams.foreach {
          case (key, value) if key.startsWith("kafka.") && key != "kafka.bootstrap.servers" =>
            writer = writer.option(key, value)
          case _ => // Skip non-kafka parameters
        }

        // Start streaming query
        val query = writer.start()

        logger.info(s"Streaming query started for topic: $topic")

        // Note: For streaming, we return success immediately
        // Actual records loaded would be monitored via query metrics
        LoadResult.success(0L) // Streaming - count not available immediately

      } else {
        // Batch write
        logger.info(s"Starting batch write to Kafka")

        val recordCount = df.count()

        var writer = df.write
          .format("kafka")
          .option("kafka.bootstrap.servers", bootstrapServers)
          .option("topic", topic)

        // Apply additional Kafka options
        config.connectionParams.foreach {
          case (key, value) if key.startsWith("kafka.") && key != "kafka.bootstrap.servers" =>
            writer = writer.option(key, value)
          case _ => // Skip non-kafka parameters
        }

        // Execute write
        writer.save()

        logger.info(s"Successfully wrote $recordCount records to Kafka topic: $topic")

        LoadResult.success(recordCount)
      }
    } catch {
      case e: Exception =>
        val errorMsg = s"Failed to write to Kafka topic $topic: ${e.getMessage}"
        logger.error(errorMsg, e)
        LoadResult.failure(0L, df.count(), errorMsg)
    }
  }
}
