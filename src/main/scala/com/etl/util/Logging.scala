package com.etl.util

import org.slf4j.{Logger, LoggerFactory, MDC}

import scala.collection.JavaConverters._

/**
 * Logging trait with MDC (Mapped Diagnostic Context) support.
 * Mix-in this trait to get structured logging capabilities with contextual fields.
 *
 * Features:
 * - Structured logging with SLF4J
 * - MDC context for trace IDs, pipeline IDs, etc.
 * - Automatic MDC cleanup after execution blocks
 * - JSON log output (configured in logback.xml)
 *
 * Example usage:
 * {{{
 * class MyClass extends Logging {
 *   def process(): Unit = {
 *     withMDC(Map("pipelineId" -> "my-pipeline", "traceId" -> uuid)) {
 *       logInfo("Processing started", Map("recordCount" -> "1000"))
 *       // ... do work ...
 *       logInfo("Processing complete")
 *     }
 *   }
 * }
 * }}}
 */
trait Logging {

  @transient
  protected lazy val logger: Logger = LoggerFactory.getLogger(getClass)

  /**
   * Log info message with optional context.
   *
   * @param message Log message
   * @param context Additional contextual key-value pairs for MDC
   */
  protected def logInfo(message: String, context: Map[String, String] = Map.empty): Unit = {
    withMDC(context) {
      logger.info(message)
    }
  }

  /**
   * Log warning message with optional context.
   *
   * @param message Log message
   * @param context Additional contextual key-value pairs
   */
  protected def logWarn(message: String, context: Map[String, String] = Map.empty): Unit = {
    withMDC(context) {
      logger.warn(message)
    }
  }

  /**
   * Log error message with optional exception and context.
   *
   * @param message Log message
   * @param error Optional exception/throwable
   * @param context Additional contextual key-value pairs
   */
  protected def logError(
    message: String,
    error: Option[Throwable] = None,
    context: Map[String, String] = Map.empty
  ): Unit = {
    withMDC(context) {
      error match {
        case Some(throwable) => logger.error(message, throwable)
        case None            => logger.error(message)
      }
    }
  }

  /**
   * Log debug message with optional context.
   *
   * @param message Log message
   * @param context Additional contextual key-value pairs
   */
  protected def logDebug(message: String, context: Map[String, String] = Map.empty): Unit = {
    withMDC(context) {
      logger.debug(message)
    }
  }

  /**
   * Execute block with MDC context.
   * MDC values are set before execution and cleared after (even on exception).
   *
   * @param context Map of MDC key-value pairs
   * @param block Code block to execute with MDC context
   * @tparam A Return type of block
   * @return Result of block execution
   */
  protected def withMDC[A](context: Map[String, String])(block: => A): A = {
    // Save current MDC state
    val previousMDC = getCurrentMDC

    try {
      // Set new MDC values
      context.foreach { case (key, value) =>
        MDC.put(key, value)
      }

      // Execute block
      block
    } finally {
      // Restore previous MDC state
      context.keys.foreach(MDC.remove)
      previousMDC.foreach { case (key, value) =>
        MDC.put(key, value)
      }
    }
  }

  /**
   * Get current MDC context as Map.
   *
   * @return Map of current MDC key-value pairs
   */
  protected def getCurrentMDC: Map[String, String] = {
    val mdcMap = MDC.getCopyOfContextMap
    if (mdcMap == null) Map.empty
    else mdcMap.asScala.toMap
  }

  /**
   * Clear all MDC context.
   */
  protected def clearMDC(): Unit = {
    MDC.clear()
  }

  /**
   * Log metrics as structured data.
   *
   * @param metricName Name of metric
   * @param value Metric value
   * @param additionalContext Additional context
   */
  protected def logMetric(
    metricName: String,
    value: Any,
    additionalContext: Map[String, String] = Map.empty
  ): Unit = {
    val context = additionalContext + ("metric" -> metricName, "value" -> value.toString)
    withMDC(context) {
      logger.info(s"Metric: $metricName = $value")
    }
  }
}
