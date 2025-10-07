package com.etl.load

import com.etl.config.LoadConfig
import com.etl.model.{LoadResult, WriteMode}
import com.etl.util.{DeadLetterQueue, ErrorHandlingContext, NoOpDeadLetterQueue}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery
import org.slf4j.LoggerFactory
import scala.util.{Failure, Success, Try}

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
 * Integrates with error handling components for retries, circuit breaker, and DLQ.
 */
class KafkaLoader(
  errorHandlingContext: Option[ErrorHandlingContext] = None
) extends Loader {
  private val logger = LoggerFactory.getLogger(getClass)
  private val dlq: DeadLetterQueue = errorHandlingContext
    .map(_.deadLetterQueue)
    .getOrElse(new NoOpDeadLetterQueue())

  // Track streaming queries for proper lifecycle management
  private val activeQueries = scala.collection.mutable.Map[String, StreamingQuery]()

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

    if (df.isStreaming) {
      // Streaming write - handled differently due to async nature
      handleStreamingWrite(df, bootstrapServers, topic, config)
    } else {
      // Batch write with error handling
      handleBatchWrite(df, bootstrapServers, topic, config)
    }
  }

  private def handleBatchWrite(
    df: DataFrame,
    bootstrapServers: String,
    topic: String,
    config: LoadConfig
  ): LoadResult = {
    logger.info(s"Starting batch write to Kafka")

    val recordCount = df.count()

    val result = errorHandlingContext match {
      case Some(ctx) =>
        ctx.execute {
          executeBatchWrite(df, bootstrapServers, topic, config)
        }
      case None =>
        Try(executeBatchWrite(df, bootstrapServers, topic, config)).toEither
    }

    result match {
      case Right(_) =>
        logger.info(s"Successfully wrote $recordCount records to Kafka topic: $topic")
        LoadResult.success(recordCount)

      case Left(error) =>
        val errorMsg = s"Failed to write to Kafka topic $topic after retries: ${error.getMessage}"
        logger.error(errorMsg, error)

        // Publish failed records to DLQ
        val context = Map(
          "pipelineId" -> config.sinkType.toString,
          "stage" -> "load",
          "topic" -> topic,
          "mode" -> "batch"
        )

        Try {
          df.collect().foreach { row =>
            dlq.publish(row, error, context)
          }
        } match {
          case Success(_) =>
            logger.info(s"Published $recordCount failed records to DLQ")
          case Failure(dlqError) =>
            logger.error(s"Failed to publish to DLQ: ${dlqError.getMessage}", dlqError)
        }

        LoadResult.failure(0L, recordCount, errorMsg)
    }
  }

  private def executeBatchWrite(
    df: DataFrame,
    bootstrapServers: String,
    topic: String,
    config: LoadConfig
  ): Unit = {
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
  }

  private def handleStreamingWrite(
    df: DataFrame,
    bootstrapServers: String,
    topic: String,
    config: LoadConfig
  ): LoadResult = {
    val checkpointLocation = config.connectionParams.getOrElse(
      "checkpointLocation",
      throw new IllegalArgumentException(
        "checkpointLocation is required for streaming Kafka writes"
      )
    )

    logger.info(s"Starting streaming write to Kafka with checkpoint: $checkpointLocation")

    try {
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

      // Store query reference for lifecycle management
      val queryId = s"${topic}_${System.currentTimeMillis()}"
      activeQueries.put(queryId, query)

      logger.info(s"Streaming query started for topic: $topic (queryId: $queryId)")

      // Note: For streaming, we return success immediately
      // Actual records loaded would be monitored via query metrics
      LoadResult.success(0L) // Streaming - count not available immediately

    } catch {
      case e: Exception =>
        val errorMsg = s"Failed to start streaming write to Kafka topic $topic: ${e.getMessage}"
        logger.error(errorMsg, e)
        LoadResult.failure(0L, 0L, errorMsg)
    }
  }

  /**
   * Get all active streaming queries managed by this loader.
   */
  def getActiveQueries: Map[String, StreamingQuery] = activeQueries.toMap

  /**
   * Stop a specific streaming query by ID.
   */
  def stopQuery(queryId: String): Unit = {
    activeQueries.get(queryId).foreach { query =>
      logger.info(s"Stopping streaming query: $queryId")
      query.stop()
      activeQueries.remove(queryId)
    }
  }

  /**
   * Stop all active streaming queries.
   */
  def stopAllQueries(): Unit = {
    logger.info(s"Stopping ${activeQueries.size} active streaming queries")
    activeQueries.foreach { case (queryId, query) =>
      Try(query.stop()) match {
        case Success(_) =>
          logger.info(s"Successfully stopped query: $queryId")
        case Failure(e) =>
          logger.error(s"Failed to stop query $queryId: ${e.getMessage}", e)
      }
    }
    activeQueries.clear()
  }
}
