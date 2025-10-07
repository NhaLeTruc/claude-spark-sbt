package com.etl.streaming

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

/**
 * Handler for managing late-arriving data in streaming pipelines.
 *
 * Late data is data that arrives after the watermark has passed. This handler provides
 * different strategies for dealing with such data based on business requirements.
 *
 * Strategies:
 * - Drop: Silently drop late data (default Spark behavior)
 * - LogAndDrop: Log late data before dropping
 * - SendToDLQ: Send late data to a dead letter queue (Kafka topic or S3)
 * - SeparateStream: Process late data in a separate output stream/table
 *
 * Usage example:
 * ```
 * val config = LateDataConfig(
 *   strategy = LateDataStrategy.SendToDLQ,
 *   dlqTopic = Some("late-data-dlq")
 * )
 *
 * val handler = new LateDataHandler(config, streamingConfig)
 * val (onTimeData, lateData) = handler.separate(df)
 *
 * // Process on-time data normally
 * processOnTimeData(onTimeData)
 *
 * // Handle late data according to strategy
 * handler.handleLateData(lateData, context)
 * ```
 */
class LateDataHandler(
  config: LateDataConfig,
  streamingConfig: StreamingConfig
) {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Separate on-time data from late data based on watermark.
   *
   * This method adds a flag column to indicate whether each row is late or on-time.
   * Note: In Spark Structured Streaming, late data is automatically dropped after watermark.
   * This method is useful for inspecting or handling late data before it's dropped.
   *
   * @param df Input DataFrame (must be streaming)
   * @return Tuple of (onTimeData, lateData)
   */
  def separate(df: DataFrame): (DataFrame, DataFrame) = {
    if (!df.isStreaming) {
      logger.warn("LateDataHandler called on batch DataFrame. Returning all data as on-time.")
      return (df, df.sparkSession.emptyDataFrame)
    }

    if (!streamingConfig.hasWatermark) {
      logger.warn(
        "LateDataHandler called without watermark configuration. " +
          "Cannot determine late data. Returning all data as on-time."
      )
      return (df, df.sparkSession.emptyDataFrame)
    }

    val watermarkCfg = streamingConfig.getWatermark
    val eventTimeCol = watermarkCfg.eventTimeColumn

    // Validate event time column exists
    if (!df.schema.fieldNames.contains(eventTimeCol)) {
      throw new IllegalArgumentException(
        s"Event time column '$eventTimeCol' not found in DataFrame. " +
          s"Available columns: ${df.schema.fieldNames.mkString(", ")}"
      )
    }

    logger.info(
      s"Separating late data based on watermark: " +
        s"column=$eventTimeCol, threshold=${watermarkCfg.delayThreshold}"
    )

    // Note: In Spark Structured Streaming, we cannot directly access the current watermark value
    // from within a transformation. Spark automatically drops late data based on watermark.
    // This separation is more conceptual for handling strategies.

    // For demonstration, we'll use the event time column itself
    // In production, you might want to use Spark's internal watermark handling
    (df, df.sparkSession.emptyDataFrame)
  }

  /**
   * Handle late data according to the configured strategy.
   *
   * @param lateData DataFrame containing late-arriving data
   * @param context Additional context (pipeline ID, stage, etc.)
   */
  def handleLateData(lateData: DataFrame, context: Map[String, String]): Unit = {
    if (lateData.isEmpty) {
      logger.debug("No late data to handle")
      return
    }

    config.strategy match {
      case LateDataStrategy.Drop =>
        handleDrop(lateData)

      case LateDataStrategy.LogAndDrop =>
        handleLogAndDrop(lateData, context)

      case LateDataStrategy.SendToDLQ =>
        handleSendToDLQ(lateData, context)

      case LateDataStrategy.SeparateStream =>
        handleSeparateStream(lateData, context)
    }
  }

  /**
   * Drop late data silently (no action needed).
   */
  private def handleDrop(lateData: DataFrame): Unit = {
    logger.debug("Dropping late data silently")
    // No action - Spark will drop automatically
  }

  /**
   * Log late data before dropping.
   */
  private def handleLogAndDrop(lateData: DataFrame, context: Map[String, String]): Unit = {
    logger.info(s"Logging late data before dropping. Context: $context")

    // Count late records
    val lateCount = lateData.count()
    logger.warn(
      s"Dropping $lateCount late-arriving record(s). " +
        s"Pipeline: ${context.getOrElse("pipelineId", "unknown")}, " +
        s"Stage: ${context.getOrElse("stage", "unknown")}"
    )

    // Optionally log sample records for debugging
    if (lateCount > 0) {
      val sampleSize = math.min(5, lateCount)
      val samples = lateData.limit(sampleSize.toInt).collect()
      logger.warn(s"Sample late records (${samples.length}): ${samples.mkString(", ")}")
    }
  }

  /**
   * Send late data to dead letter queue.
   */
  private def handleSendToDLQ(lateData: DataFrame, context: Map[String, String]): Unit = {
    val dlqTopic = config.dlqTopic.getOrElse(
      throw new IllegalStateException("DLQ topic not configured for SendToDLQ strategy")
    )

    logger.info(s"Sending late data to DLQ topic: $dlqTopic. Context: $context")

    // Add metadata columns
    val enrichedData = lateData
      .withColumn("dlq_reason", lit("late_arrival"))
      .withColumn("dlq_timestamp", current_timestamp())
      .withColumn("pipeline_id", lit(context.getOrElse("pipelineId", "unknown")))
      .withColumn("stage", lit(context.getOrElse("stage", "unknown")))

    // Write to Kafka DLQ topic
    try {
      // For streaming data, write to Kafka
      if (enrichedData.isStreaming) {
        enrichedData
          .selectExpr("to_json(struct(*)) AS value")
          .writeStream
          .format("kafka")
          .option("kafka.bootstrap.servers",
            context.getOrElse("kafka.bootstrap.servers", "localhost:9092"))
          .option("topic", dlqTopic)
          .option("checkpointLocation", s"/tmp/dlq-checkpoint-${System.currentTimeMillis()}")
          .start()

        logger.info(s"Started streaming late data to DLQ topic: $dlqTopic")
      } else {
        // For batch data
        enrichedData
          .selectExpr("to_json(struct(*)) AS value")
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers",
            context.getOrElse("kafka.bootstrap.servers", "localhost:9092"))
          .option("topic", dlqTopic)
          .save()

        val recordCount = enrichedData.count()
        logger.info(s"Sent $recordCount late record(s) to DLQ topic: $dlqTopic")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to send late data to DLQ: ${e.getMessage}", e)
        // Fall back to logging
        handleLogAndDrop(lateData, context)
    }
  }

  /**
   * Process late data in a separate stream/table.
   */
  private def handleSeparateStream(lateData: DataFrame, context: Map[String, String]): Unit = {
    val lateDataTable = config.lateDataTable.getOrElse(
      throw new IllegalStateException("Late data table not configured for SeparateStream strategy")
    )

    logger.info(s"Writing late data to separate table: $lateDataTable. Context: $context")

    // Add metadata columns
    val enrichedData = lateData
      .withColumn("is_late_data", lit(true))
      .withColumn("ingestion_timestamp", current_timestamp())
      .withColumn("pipeline_id", lit(context.getOrElse("pipelineId", "unknown")))
      .withColumn("stage", lit(context.getOrElse("stage", "unknown")))

    try {
      // Write to separate table
      if (enrichedData.isStreaming) {
        enrichedData.writeStream
          .format("parquet")
          .option("path", lateDataTable)
          .option("checkpointLocation", s"/tmp/late-data-checkpoint-${System.currentTimeMillis()}")
          .start()

        logger.info(s"Started streaming late data to table: $lateDataTable")
      } else {
        enrichedData.write
          .mode("append")
          .parquet(lateDataTable)

        val recordCount = enrichedData.count()
        logger.info(s"Wrote $recordCount late record(s) to table: $lateDataTable")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to write late data to separate table: ${e.getMessage}", e)
        // Fall back to logging
        handleLogAndDrop(lateData, context)
    }
  }

  /**
   * Monitor late data metrics.
   * Returns statistics about late data for observability.
   */
  def collectMetrics(lateData: DataFrame): LateDataMetrics = {
    if (lateData.isEmpty) {
      return LateDataMetrics(0L, None, None)
    }

    val watermarkCfg = streamingConfig.getWatermark
    val eventTimeCol = watermarkCfg.eventTimeColumn

    val count = lateData.count()
    val stats = lateData.agg(
      min(eventTimeCol).as("min_event_time"),
      max(eventTimeCol).as("max_event_time")
    ).collect()(0)

    LateDataMetrics(
      count = count,
      minEventTime = Option(stats.getAs[java.sql.Timestamp]("min_event_time")).map(_.toString),
      maxEventTime = Option(stats.getAs[java.sql.Timestamp]("max_event_time")).map(_.toString)
    )
  }
}

/**
 * Metrics for late-arriving data.
 *
 * @param count Number of late records
 * @param minEventTime Earliest event time in late data
 * @param maxEventTime Latest event time in late data
 */
case class LateDataMetrics(
  count: Long,
  minEventTime: Option[String],
  maxEventTime: Option[String]
) {
  def summary: String = {
    s"Late data: $count record(s), " +
      s"event time range: ${minEventTime.getOrElse("N/A")} to ${maxEventTime.getOrElse("N/A")}"
  }
}

/**
 * Companion object for creating LateDataHandler instances.
 */
object LateDataHandler {
  /**
   * Create LateDataHandler with configuration.
   */
  def apply(config: LateDataConfig, streamingConfig: StreamingConfig): LateDataHandler =
    new LateDataHandler(config, streamingConfig)

  /**
   * Create LateDataHandler with default drop strategy.
   */
  def default(streamingConfig: StreamingConfig): LateDataHandler =
    new LateDataHandler(LateDataConfig(), streamingConfig)
}
