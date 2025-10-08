package com.etl.streaming

import scala.concurrent.duration._

/**
 * Watermark configuration for handling late-arriving data.
 *
 * Watermarks allow Spark Structured Streaming to track event time and handle
 * late-arriving data gracefully. The watermark defines how late data can arrive
 * before being dropped.
 *
 * @param eventTimeColumn Column containing event timestamp (must be timestamp type)
 * @param delayThreshold Maximum delay allowed for late data (e.g., "10 minutes", "1 hour")
 */
case class WatermarkConfig(
  eventTimeColumn: String,
  delayThreshold: String
) {
  require(eventTimeColumn.nonEmpty, "Event time column cannot be empty")
  require(delayThreshold.nonEmpty, "Delay threshold cannot be empty")

  /**
   * Validate delay threshold format.
   * Supported formats: "X seconds", "X minutes", "X hours"
   */
  def validateDelayThreshold(): Unit = {
    val pattern = """^(\d+)\s+(second|seconds|minute|minutes|hour|hours|day|days)$""".r
    if (!pattern.matches(delayThreshold)) {
      throw new IllegalArgumentException(
        s"Invalid delay threshold format: '$delayThreshold'. " +
        "Expected format: '<number> <unit>' (e.g., '10 minutes', '1 hour')"
      )
    }
  }
}

/**
 * Output mode for streaming queries.
 */
sealed trait OutputMode {
  def value: String
}

object OutputMode {
  /**
   * Append mode: Only new rows added to result table will be written to sink.
   * Works with all query types but doesn't support aggregations without watermarks.
   */
  case object Append extends OutputMode {
    override def value: String = "append"
  }

  /**
   * Complete mode: Entire result table will be written to sink after every trigger.
   * Supported only for aggregation queries.
   */
  case object Complete extends OutputMode {
    override def value: String = "complete"
  }

  /**
   * Update mode: Only rows that were updated in result table will be written.
   * Supported for aggregation queries.
   */
  case object Update extends OutputMode {
    override def value: String = "update"
  }

  def fromString(s: String): OutputMode = s.toLowerCase match {
    case "append"   => Append
    case "complete" => Complete
    case "update"   => Update
    case _ => throw new IllegalArgumentException(
      s"Unknown output mode: $s. Valid values: append, complete, update"
    )
  }
}

/**
 * Trigger mode for streaming queries.
 */
sealed trait TriggerMode

object TriggerMode {
  /**
   * Continuous processing (experimental): Process data as soon as it arrives.
   */
  case object Continuous extends TriggerMode

  /**
   * Processing time trigger: Process data at fixed intervals.
   *
   * @param interval Processing interval (e.g., "5 seconds", "1 minute")
   */
  case class ProcessingTime(interval: String) extends TriggerMode

  /**
   * One-time trigger: Process all available data and stop.
   */
  case object Once extends TriggerMode

  /**
   * Available now trigger: Process all available data in multiple batches.
   */
  case object AvailableNow extends TriggerMode

  def fromString(s: String): TriggerMode = {
    if (s.toLowerCase == "continuous") {
      Continuous
    } else if (s.toLowerCase == "once") {
      Once
    } else if (s.toLowerCase == "availablenow") {
      AvailableNow
    } else if (s.toLowerCase.startsWith("processingtime=")) {
      val interval = s.substring("processingtime=".length)
      ProcessingTime(interval)
    } else {
      throw new IllegalArgumentException(
        s"Unknown trigger mode: $s. Valid values: continuous, once, availablenow, processingtime=<interval>"
      )
    }
  }
}

/**
 * Comprehensive streaming configuration for Structured Streaming queries.
 *
 * @param checkpointLocation Directory for checkpointing (required for fault tolerance)
 * @param queryName Name for the streaming query (for monitoring)
 * @param outputMode Output mode (append, complete, update)
 * @param triggerMode Trigger mode (continuous, processing time, once, available now)
 * @param watermark Optional watermark configuration for event time processing
 * @param enableIdempotence Enable idempotent writes (exactly-once semantics)
 * @param maxRecordsPerTrigger Optional limit on records processed per trigger
 * @param maxFilesPerTrigger Optional limit on files processed per trigger (for file sources)
 * @param stateTimeout Optional timeout for stateful operations (e.g., "1 hour")
 */
case class StreamingConfig(
  checkpointLocation: String,
  queryName: String,
  outputMode: OutputMode = OutputMode.Append,
  triggerMode: TriggerMode = TriggerMode.Continuous,
  watermark: Option[WatermarkConfig] = None,
  enableIdempotence: Boolean = true,
  maxRecordsPerTrigger: Option[Long] = None,
  maxFilesPerTrigger: Option[Int] = None,
  stateTimeout: Option[String] = None,
  lateDataConfig: Option[LateDataConfig] = None
) {
  require(checkpointLocation.nonEmpty, "Checkpoint location cannot be empty")
  require(queryName.nonEmpty, "Query name cannot be empty")

  // Validate watermark if present
  watermark.foreach(_.validateDelayThreshold())

  /**
   * Check if watermark is configured.
   */
  def hasWatermark: Boolean = watermark.isDefined

  /**
   * Get watermark configuration or throw exception.
   */
  def getWatermark: WatermarkConfig = watermark.getOrElse(
    throw new IllegalStateException("Watermark not configured")
  )

  /**
   * Check if this is a batch trigger (once or available now).
   */
  def isBatchTrigger: Boolean = triggerMode match {
    case TriggerMode.Once | TriggerMode.AvailableNow => true
    case _ => false
  }

  /**
   * Get processing time interval if applicable.
   */
  def getProcessingTimeInterval: Option[String] = triggerMode match {
    case TriggerMode.ProcessingTime(interval) => Some(interval)
    case _ => None
  }
}

/**
 * Late data handling strategy.
 */
sealed trait LateDataStrategy

object LateDataStrategy {
  /**
   * Drop late data silently.
   */
  case object Drop extends LateDataStrategy

  /**
   * Log late data and drop.
   */
  case object LogAndDrop extends LateDataStrategy

  /**
   * Send late data to dead letter queue.
   */
  case object SendToDLQ extends LateDataStrategy

  /**
   * Process late data in separate table/stream.
   */
  case object SeparateStream extends LateDataStrategy

  def fromString(s: String): LateDataStrategy = s.toLowerCase match {
    case "drop"           => Drop
    case "loganddrop"     => LogAndDrop
    case "sendtodlq"      => SendToDLQ
    case "separatestream" => SeparateStream
    case _ => throw new IllegalArgumentException(
      s"Unknown late data strategy: $s. Valid values: drop, loganddrop, sendtodlq, separatestream"
    )
  }
}

/**
 * Configuration for handling late-arriving data.
 *
 * @param strategy How to handle late data
 * @param dlqTopic Optional DLQ topic for late data (required if strategy = SendToDLQ)
 * @param lateDataTable Optional table name for late data (required if strategy = SeparateStream)
 */
case class LateDataConfig(
  strategy: LateDataStrategy = LateDataStrategy.Drop,
  dlqTopic: Option[String] = None,
  lateDataTable: Option[String] = None
) {
  // Validate configuration
  if (strategy == LateDataStrategy.SendToDLQ) {
    require(dlqTopic.isDefined, "DLQ topic required when strategy is SendToDLQ")
  }
  if (strategy == LateDataStrategy.SeparateStream) {
    require(lateDataTable.isDefined, "Late data table required when strategy is SeparateStream")
  }
}
