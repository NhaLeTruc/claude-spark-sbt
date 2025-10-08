package com.etl.streaming

import org.slf4j.LoggerFactory

/**
 * Factory for creating StreamingConfig from JSON configuration.
 * Handles parsing of complex sealed trait types and validates configuration.
 */
object StreamingConfigFactory {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Create StreamingConfig from JSON map.
   *
   * @param json JSON configuration as Map[String, Any]
   * @return StreamingConfig instance
   * @throws IllegalArgumentException if required fields missing or invalid
   */
  def fromJson(json: Map[String, Any]): StreamingConfig = {
    logger.debug(s"Parsing StreamingConfig from JSON: $json")

    // Required fields
    val checkpointLocation = json.get("checkpointLocation")
      .map(_.toString)
      .getOrElse(
        throw new IllegalArgumentException("checkpointLocation is required in streamingConfigJson")
      )

    val queryName = json.get("queryName")
      .map(_.toString)
      .getOrElse(
        throw new IllegalArgumentException("queryName is required in streamingConfigJson")
      )

    // Optional fields with defaults
    val outputMode = json.get("outputMode")
      .map(v => parseOutputMode(v.toString))
      .getOrElse(OutputMode.Append)

    val triggerMode = json.get("triggerMode")
      .map(v => parseTriggerMode(v.toString))
      .getOrElse(TriggerMode.Continuous)

    val watermark = json.get("watermark")
      .map(w => parseWatermark(w.asInstanceOf[Map[String, Any]]))

    val enableIdempotence = json.get("enableIdempotence")
      .map {
        case b: Boolean => b
        case s: String => s.toLowerCase == "true"
        case _ => true
      }
      .getOrElse(true)

    val maxRecordsPerTrigger = json.get("maxRecordsPerTrigger")
      .map {
        case l: Long => Some(l)
        case i: Int => Some(i.toLong)
        case s: String => Some(s.toLong)
        case _ => None
      }
      .flatten

    val maxFilesPerTrigger = json.get("maxFilesPerTrigger")
      .map {
        case i: Int => Some(i)
        case s: String => Some(s.toInt)
        case _ => None
      }
      .flatten

    val stateTimeout = json.get("stateTimeout")
      .map(_.toString)

    val lateDataConfig = json.get("lateDataConfig")
      .map(ld => parseLateDataConfig(ld.asInstanceOf[Map[String, Any]]))

    val config = StreamingConfig(
      checkpointLocation = checkpointLocation,
      queryName = queryName,
      outputMode = outputMode,
      triggerMode = triggerMode,
      watermark = watermark,
      enableIdempotence = enableIdempotence,
      maxRecordsPerTrigger = maxRecordsPerTrigger,
      maxFilesPerTrigger = maxFilesPerTrigger,
      stateTimeout = stateTimeout,
      lateDataConfig = lateDataConfig
    )

    logger.info(s"Parsed StreamingConfig: queryName=${config.queryName}, outputMode=${config.outputMode}, triggerMode=${config.triggerMode}")

    config
  }

  /**
   * Parse OutputMode from string.
   * Supported values: "append", "complete", "update"
   */
  private def parseOutputMode(value: String): OutputMode = {
    value.toLowerCase.trim match {
      case "append" => OutputMode.Append
      case "complete" => OutputMode.Complete
      case "update" => OutputMode.Update
      case other =>
        throw new IllegalArgumentException(
          s"Invalid outputMode: '$other'. Valid values: append, complete, update"
        )
    }
  }

  /**
   * Parse TriggerMode from string.
   * Supported formats:
   * - "continuous"
   * - "processingtime=5 seconds"
   * - "processingtime=1 minute"
   * - "once"
   * - "availablenow"
   */
  private def parseTriggerMode(value: String): TriggerMode = {
    val normalized = value.toLowerCase.trim

    normalized match {
      case "continuous" =>
        TriggerMode.Continuous

      case "once" =>
        TriggerMode.Once

      case "availablenow" =>
        TriggerMode.AvailableNow

      case pt if pt.startsWith("processingtime=") || pt.startsWith("processing_time=") =>
        val interval = pt.split("=")(1).trim
        TriggerMode.ProcessingTime(interval)

      case other =>
        throw new IllegalArgumentException(
          s"Invalid triggerMode: '$other'. Valid values: continuous, processingtime=<interval>, once, availablenow"
        )
    }
  }

  /**
   * Parse WatermarkConfig from JSON map.
   * Required fields: eventTimeColumn, delayThreshold
   */
  private def parseWatermark(json: Map[String, Any]): WatermarkConfig = {
    val eventTimeColumn = json.get("eventTimeColumn")
      .map(_.toString)
      .getOrElse(
        throw new IllegalArgumentException("eventTimeColumn is required in watermark config")
      )

    val delayThreshold = json.get("delayThreshold")
      .map(_.toString)
      .getOrElse(
        throw new IllegalArgumentException("delayThreshold is required in watermark config")
      )

    val config = WatermarkConfig(eventTimeColumn, delayThreshold)

    // Validate delay threshold format
    config.validateDelayThreshold()

    config
  }

  /**
   * Parse LateDataConfig from JSON map.
   * Optional fields: strategy, dlqTopic, lateDataTable
   */
  private def parseLateDataConfig(json: Map[String, Any]): LateDataConfig = {
    val strategy = json.get("strategy")
      .map(s => parseLateDataStrategy(s.toString))
      .getOrElse(LateDataStrategy.Drop)

    val dlqTopic = json.get("dlqTopic").map(_.toString)

    val lateDataTable = json.get("lateDataTable").map(_.toString)

    LateDataConfig(
      strategy = strategy,
      dlqTopic = dlqTopic,
      lateDataTable = lateDataTable
    )
  }

  /**
   * Parse LateDataStrategy from string.
   * Supported values: "drop", "loganddrop", "sendtodlq", "separatestream"
   */
  private def parseLateDataStrategy(value: String): LateDataStrategy = {
    value.toLowerCase.trim.replace("_", "").replace("-", "") match {
      case "drop" =>
        LateDataStrategy.Drop

      case "loganddrop" | "logdrop" =>
        LateDataStrategy.LogAndDrop

      case "sendtodlq" | "dlq" =>
        LateDataStrategy.SendToDLQ

      case "separatestream" | "separate" =>
        LateDataStrategy.SeparateStream

      case other =>
        throw new IllegalArgumentException(
          s"Invalid lateDataStrategy: '$other'. Valid values: drop, loganddrop, sendtodlq, separatestream"
        )
    }
  }

  /**
   * Convert StreamingConfig to JSON map for serialization.
   *
   * @param config StreamingConfig instance
   * @return JSON representation as Map[String, Any]
   */
  def toJson(config: StreamingConfig): Map[String, Any] = {
    var json = Map[String, Any](
      "checkpointLocation" -> config.checkpointLocation,
      "queryName" -> config.queryName,
      "outputMode" -> config.outputMode.value,
      "triggerMode" -> triggerModeToString(config.triggerMode),
      "enableIdempotence" -> config.enableIdempotence
    )

    // Add optional fields
    config.watermark.foreach { wm =>
      json = json + ("watermark" -> Map(
        "eventTimeColumn" -> wm.eventTimeColumn,
        "delayThreshold" -> wm.delayThreshold
      ))
    }

    config.maxRecordsPerTrigger.foreach { limit =>
      json = json + ("maxRecordsPerTrigger" -> limit)
    }

    config.maxFilesPerTrigger.foreach { limit =>
      json = json + ("maxFilesPerTrigger" -> limit)
    }

    config.stateTimeout.foreach { timeout =>
      json = json + ("stateTimeout" -> timeout)
    }

    config.lateDataConfig.foreach { ldc =>
      json = json + ("lateDataConfig" -> Map(
        "strategy" -> lateDataStrategyToString(ldc.strategy),
        "dlqTopic" -> ldc.dlqTopic.getOrElse(""),
        "lateDataTable" -> ldc.lateDataTable.getOrElse("")
      ))
    }

    json
  }

  /**
   * Convert TriggerMode to string representation.
   */
  private def triggerModeToString(mode: TriggerMode): String = mode match {
    case TriggerMode.Continuous => "continuous"
    case TriggerMode.ProcessingTime(interval) => s"processingtime=$interval"
    case TriggerMode.Once => "once"
    case TriggerMode.AvailableNow => "availablenow"
  }

  /**
   * Convert LateDataStrategy to string representation.
   */
  private def lateDataStrategyToString(strategy: LateDataStrategy): String = strategy match {
    case LateDataStrategy.Drop => "drop"
    case LateDataStrategy.LogAndDrop => "loganddrop"
    case LateDataStrategy.SendToDLQ => "sendtodlq"
    case LateDataStrategy.SeparateStream => "separatestream"
  }
}
