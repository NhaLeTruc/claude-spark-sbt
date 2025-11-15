package com.etl.streaming

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.functions._

import scala.concurrent.duration._

/**
 * Utilities for managing state in Spark Structured Streaming.
 *
 * Provides helper methods for:
 * - Stateful aggregations with automatic state expiration
 * - Late data handling
 * - Watermark management
 * - State cleanup
 * - Session windows
 *
 * Example usage:
 * {{{
 *   val stateManager = new StateManager()
 *
 *   // Stateful aggregation with timeout
 *   val result = stateManager.withStatefulAggregation(
 *     streamDF,
 *     keyColumns = Seq("userId"),
 *     stateTimeout = 1.hour,
 *     updateFunction = myUpdateFunction
 *   )
 *
 *   // Add watermark for late data handling
 *   val watermarkedDF = stateManager.withWatermark(
 *     streamDF,
 *     timestampColumn = "eventTime",
 *     delayThreshold = 10.minutes
 *   )
 * }}}
 */
class StateManager {

  /**
   * Add watermark to streaming DataFrame for late data handling.
   *
   * @param df Streaming DataFrame
   * @param timestampColumn Column containing event timestamps
   * @param delayThreshold Maximum delay threshold for late data
   * @return DataFrame with watermark
   */
  def withWatermark(
    df: DataFrame,
    timestampColumn: String,
    delayThreshold: FiniteDuration
  ): DataFrame = {
    df.withWatermark(timestampColumn, s"${delayThreshold.toSeconds} seconds")
  }

  /**
   * Apply stateful aggregation with automatic state cleanup.
   *
   * @param df Streaming DataFrame
   * @param keyColumns Columns to group by
   * @param stateTimeout Timeout for state expiration
   * @param updateFunction Function to update state
   * @tparam K Key type
   * @tparam V Value type
   * @tparam S State type
   * @return DataFrame with aggregated results
   */
  def withStatefulAggregation[K, V, S](
    df: Dataset[V],
    keyColumns: Seq[String],
    stateTimeout: FiniteDuration,
    updateFunction: (K, Iterator[V], GroupState[S]) => S
  )(implicit keyEncoder: org.apache.spark.sql.Encoder[K]): Dataset[S] = {
    import df.sparkSession.implicits._

    val keyedDS = df.selectExpr(keyColumns.mkString(", "), "*")
      .as[(K, V)]

    keyedDS
      .groupByKey(_._1)
      .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout)(updateFunction)
  }

  /**
   * Create session window with gap duration.
   *
   * @param df Streaming DataFrame
   * @param timestampColumn Column containing event timestamps
   * @param sessionGap Gap duration to define session boundaries
   * @return DataFrame with session windows
   */
  def withSessionWindow(
    df: DataFrame,
    timestampColumn: String,
    sessionGap: FiniteDuration
  ): DataFrame = {
    df.groupBy(
      session_window(col(timestampColumn), s"${sessionGap.toSeconds} seconds")
    )
  }

  /**
   * Create tumbling window (fixed-size, non-overlapping).
   *
   * @param df Streaming DataFrame
   * @param timestampColumn Column containing event timestamps
   * @param windowDuration Window size
   * @return DataFrame with tumbling windows
   */
  def withTumblingWindow(
    df: DataFrame,
    timestampColumn: String,
    windowDuration: FiniteDuration
  ): DataFrame = {
    df.groupBy(
      window(col(timestampColumn), s"${windowDuration.toSeconds} seconds")
    )
  }

  /**
   * Create sliding window (fixed-size, overlapping).
   *
   * @param df Streaming DataFrame
   * @param timestampColumn Column containing event timestamps
   * @param windowDuration Window size
   * @param slideDuration Slide interval
   * @return DataFrame with sliding windows
   */
  def withSlidingWindow(
    df: DataFrame,
    timestampColumn: String,
    windowDuration: FiniteDuration,
    slideDuration: FiniteDuration
  ): DataFrame = {
    df.groupBy(
      window(
        col(timestampColumn),
        s"${windowDuration.toSeconds} seconds",
        s"${slideDuration.toSeconds} seconds"
      )
    )
  }

  /**
   * Handle late data by outputting to separate stream.
   *
   * @param df Streaming DataFrame with watermark
   * @param timestampColumn Column containing event timestamps
   * @param watermarkColumn Watermark column name (default: "_watermark")
   * @return Tuple of (on-time data, late data)
   */
  def partitionLateData(
    df: DataFrame,
    timestampColumn: String,
    watermarkColumn: String = "_watermark"
  ): (DataFrame, DataFrame) = {
    // On-time data
    val onTimeData = df.filter(col(timestampColumn) >= col(watermarkColumn))

    // Late data
    val lateData = df.filter(col(timestampColumn) < col(watermarkColumn))

    (onTimeData, lateData)
  }
}

/**
 * Companion object with common state management patterns.
 */
object StateManager {

  /**
   * State data for session aggregation.
   */
  case class SessionState(
    sessionStart: Long,
    sessionEnd: Long,
    eventCount: Long,
    lastUpdate: Long
  )

  /**
   * State data for running aggregation.
   */
  case class AggregationState(
    count: Long,
    sum: Double,
    min: Double,
    max: Double,
    lastUpdate: Long
  )

  /**
   * Update function for session-based aggregation.
   *
   * Example: Track user sessions with automatic timeout.
   */
  def sessionUpdateFunction[K, V](
    key: K,
    values: Iterator[V],
    state: GroupState[SessionState],
    extractTimestamp: V => Long,
    timeoutDuration: FiniteDuration
  ): Option[SessionState] = {
    val currentTime = System.currentTimeMillis()

    // Set timeout
    state.setTimeoutDuration(timeoutDuration.toMillis)

    if (state.hasTimedOut) {
      // Session expired, emit final state
      val finalState = state.get
      state.remove()
      Some(finalState)
    } else {
      val currentState = state.getOption.getOrElse(
        SessionState(
          sessionStart = currentTime,
          sessionEnd = currentTime,
          eventCount = 0,
          lastUpdate = currentTime
        )
      )

      // Process new events
      val newEvents = values.toSeq
      val timestamps = newEvents.map(extractTimestamp)

      val updatedState = currentState.copy(
        sessionStart = math.min(currentState.sessionStart, timestamps.min),
        sessionEnd = math.max(currentState.sessionEnd, timestamps.max),
        eventCount = currentState.eventCount + newEvents.size,
        lastUpdate = currentTime
      )

      state.update(updatedState)
      None // Don't emit until session ends
    }
  }

  /**
   * Update function for running aggregation.
   *
   * Example: Calculate running statistics (count, sum, min, max).
   */
  def aggregationUpdateFunction[K](
    key: K,
    values: Iterator[Double],
    state: GroupState[AggregationState],
    timeoutDuration: FiniteDuration
  ): AggregationState = {
    val currentTime = System.currentTimeMillis()

    state.setTimeoutDuration(timeoutDuration.toMillis)

    if (state.hasTimedOut) {
      // State expired, reset
      state.remove()
      AggregationState(0, 0.0, Double.MaxValue, Double.MinValue, currentTime)
    } else {
      val currentState = state.getOption.getOrElse(
        AggregationState(0, 0.0, Double.MaxValue, Double.MinValue, currentTime)
      )

      // Process new values
      val newValues = values.toSeq
      if (newValues.isEmpty) {
        currentState
      } else {
        val updatedState = AggregationState(
          count = currentState.count + newValues.size,
          sum = currentState.sum + newValues.sum,
          min = math.min(currentState.min, newValues.min),
          max = math.max(currentState.max, newValues.max),
          lastUpdate = currentTime
        )

        state.update(updatedState)
        updatedState
      }
    }
  }

  /**
   * Create state manager instance.
   */
  def apply(): StateManager = new StateManager()
}

/**
 * Checkpoint management utilities.
 */
object CheckpointManager {

  /**
   * Configuration for checkpoint management.
   */
  case class CheckpointConfig(
    location: String,
    cleanupEnabled: Boolean = true,
    retentionDuration: FiniteDuration = 7.days,
    compressionEnabled: Boolean = false
  )

  /**
   * Validate checkpoint location.
   */
  def validateCheckpointLocation(location: String): Either[String, Unit] = {
    if (location == null || location.trim.isEmpty) {
      Left("Checkpoint location cannot be empty")
    } else if (!location.startsWith("s3://") &&
               !location.startsWith("hdfs://") &&
               !location.startsWith("/") &&
               !location.startsWith("file://")) {
      Left(s"Invalid checkpoint location: $location. Must start with s3://, hdfs://, file://, or /")
    } else {
      Right(())
    }
  }

  /**
   * Generate checkpoint location path.
   */
  def generateCheckpointPath(
    baseLocation: String,
    pipelineId: String,
    componentType: String
  ): String = {
    val sanitizedPipelineId = pipelineId.replaceAll("[^a-zA-Z0-9-_]", "_")
    s"$baseLocation/$sanitizedPipelineId/$componentType"
  }

  /**
   * Clean old checkpoints based on retention policy.
   *
   * Note: This is a placeholder. Actual implementation would use
   * Hadoop FileSystem API or cloud storage SDK to delete old files.
   */
  def cleanOldCheckpoints(
    location: String,
    retentionDuration: FiniteDuration
  ): Unit = {
    // Placeholder for checkpoint cleanup logic
    // In production, this would:
    // 1. List checkpoint directories
    // 2. Check modification times
    // 3. Delete checkpoints older than retention duration
    println(s"Checkpoint cleanup: $location (retention: $retentionDuration)")
  }
}

/**
 * Watermark utilities for late data handling.
 */
object WatermarkHelper {

  /**
   * Calculate appropriate watermark delay based on data characteristics.
   *
   * @param expectedLatenessPct Expected percentage of late events (0.0-1.0)
   * @param avgProcessingDelay Average processing delay
   * @return Recommended watermark delay
   */
  def calculateWatermarkDelay(
    expectedLatenessPct: Double,
    avgProcessingDelay: FiniteDuration
  ): FiniteDuration = {
    val safetyMultiplier = 1.0 + (expectedLatenessPct * 2.0)
    val delaySeconds = (avgProcessingDelay.toSeconds * safetyMultiplier).toLong
    delaySeconds.seconds
  }

  /**
   * Validate watermark configuration.
   */
  def validateWatermark(
    watermarkDelay: FiniteDuration,
    windowDuration: Option[FiniteDuration] = None
  ): Either[String, Unit] = {
    if (watermarkDelay.toSeconds <= 0) {
      Left("Watermark delay must be positive")
    } else if (windowDuration.exists(_ <= watermarkDelay)) {
      Left(s"Watermark delay ($watermarkDelay) should be less than window duration (${windowDuration.get})")
    } else {
      Right(())
    }
  }

  /**
   * Get recommended watermark delay for common scenarios.
   */
  def getRecommendedDelay(scenario: String): FiniteDuration = scenario.toLowerCase match {
    case "iot" | "sensors" => 5.minutes // IoT sensors may have network delays
    case "clickstream" | "web" => 2.minutes // Web events are typically near real-time
    case "mobile" => 10.minutes // Mobile apps may have connectivity issues
    case "financial" => 1.minute // Financial data needs strict timeliness
    case "logs" | "metrics" => 5.minutes // Log collection has some delay
    case _ => 5.minutes // Default conservative estimate
  }
}

/**
 * State store utilities for managing stateful operations.
 */
object StateStoreHelper {

  /**
   * Configuration for state store.
   */
  case class StateStoreConfig(
    stateTimeout: FiniteDuration,
    maxRecordsPerKey: Option[Int] = None,
    enableCompaction: Boolean = true,
    compressionCodec: String = "snappy"
  )

  /**
   * Estimate state store memory requirements.
   *
   * @param avgStateSize Average size of state per key (bytes)
   * @param expectedKeys Expected number of unique keys
   * @param overheadFactor Overhead factor (1.5x recommended)
   * @return Estimated memory in MB
   */
  def estimateMemoryRequirement(
    avgStateSize: Long,
    expectedKeys: Long,
    overheadFactor: Double = 1.5
  ): Long = {
    val baseSizeMB = (avgStateSize * expectedKeys) / (1024 * 1024)
    (baseSizeMB * overheadFactor).toLong
  }

  /**
   * Validate state store configuration.
   */
  def validateStateStoreConfig(config: StateStoreConfig): Either[String, Unit] = {
    if (config.stateTimeout.toSeconds <= 0) {
      Left("State timeout must be positive")
    } else if (config.maxRecordsPerKey.exists(_ <= 0)) {
      Left("Max records per key must be positive")
    } else {
      Right(())
    }
  }

  /**
   * Get recommended timeout for common scenarios.
   */
  def getRecommendedTimeout(scenario: String): FiniteDuration = scenario.toLowerCase match {
    case "user_session" => 30.minutes
    case "shopping_cart" => 24.hours
    case "fraud_detection" => 1.hour
    case "metrics_aggregation" => 5.minutes
    case "daily_summary" => 1.day
    case _ => 1.hour
  }
}
