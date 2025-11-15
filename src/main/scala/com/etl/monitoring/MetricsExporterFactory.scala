package com.etl.monitoring

import com.etl.model.ExecutionMetrics
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
 * Factory for creating and managing metrics exporters.
 *
 * Supports:
 * - Prometheus (push and pull modes)
 * - CloudWatch
 * - Multiple exporters simultaneously
 *
 * Example usage:
 * {{{
 *   val config = MetricsExporterConfig(
 *     prometheus = Some(PrometheusConfig(enabled = true, mode = "pull")),
 *     cloudWatch = Some(CloudWatchConfig(enabled = true, region = Some("us-east-1")))
 *   )
 *
 *   val manager = MetricsExporterFactory.createManager(config)
 *
 *   manager.export(executionMetrics)
 *
 *   manager.close()
 * }}}
 */
object MetricsExporterFactory {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Create metrics exporter manager from configuration.
   */
  def createManager(config: MetricsExporterConfig): MetricsExporterManager = {
    val exporters = scala.collection.mutable.ArrayBuffer[AutoCloseable with MetricsExporter]()

    // Create Prometheus exporter if enabled
    config.prometheus.filter(_.enabled).foreach { prometheusConfig =>
      PrometheusMetricsExporter.fromConfig(prometheusConfig) match {
        case Success(exporter) =>
          exporters += new PrometheusExporterAdapter(exporter)
          logger.info(s"Prometheus exporter enabled (mode=${prometheusConfig.mode})")
        case Failure(e) =>
          logger.error(s"Failed to create Prometheus exporter: ${e.getMessage}", e)
      }
    }

    // Create CloudWatch exporter if enabled
    config.cloudWatch.filter(_.enabled).foreach { cloudWatchConfig =>
      CloudWatchMetricsExporter.fromConfig(cloudWatchConfig) match {
        case Success(exporter) =>
          exporters += new CloudWatchExporterAdapter(exporter)
          logger.info(s"CloudWatch exporter enabled (region=${cloudWatchConfig.region.getOrElse("default")})")
        case Failure(e) =>
          logger.error(s"Failed to create CloudWatch exporter: ${e.getMessage}", e)
      }
    }

    if (exporters.isEmpty) {
      logger.warn("No metrics exporters enabled")
    }

    new MetricsExporterManager(exporters.toSeq)
  }

  /**
   * Create Prometheus-only exporter (convenience method).
   */
  def createPrometheusOnly(
    mode: String = "pull",
    pushgatewayUrl: Option[String] = None,
    port: Int = 9090
  ): MetricsExporterManager = {
    val config = MetricsExporterConfig(
      prometheus = Some(PrometheusConfig(
        enabled = true,
        mode = mode,
        pushgatewayUrl = pushgatewayUrl,
        port = Some(port)
      ))
    )
    createManager(config)
  }

  /**
   * Create CloudWatch-only exporter (convenience method).
   */
  def createCloudWatchOnly(
    namespace: String = "ETL/Pipelines",
    region: String = "us-east-1"
  ): MetricsExporterManager = {
    val config = MetricsExporterConfig(
      cloudWatch = Some(CloudWatchConfig(
        enabled = true,
        namespace = Some(namespace),
        region = Some(region)
      ))
    )
    createManager(config)
  }
}

/**
 * Unified interface for metrics exporters.
 */
trait MetricsExporter {
  def export(metrics: ExecutionMetrics): Unit
}

/**
 * Adapter for Prometheus exporter.
 */
class PrometheusExporterAdapter(exporter: PrometheusMetricsExporter)
  extends MetricsExporter with AutoCloseable {

  override def export(metrics: ExecutionMetrics): Unit = {
    exporter.export(metrics)
  }

  override def close(): Unit = {
    exporter.close()
  }
}

/**
 * Adapter for CloudWatch exporter.
 */
class CloudWatchExporterAdapter(exporter: CloudWatchMetricsExporter)
  extends MetricsExporter with AutoCloseable {

  override def export(metrics: ExecutionMetrics): Unit = {
    exporter.export(metrics)
  }

  override def close(): Unit = {
    exporter.close()
  }
}

/**
 * Manager for multiple metrics exporters.
 *
 * Handles exporting metrics to multiple destinations concurrently.
 */
class MetricsExporterManager(exporters: Seq[AutoCloseable with MetricsExporter]) {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Export metrics to all configured exporters.
   */
  def export(metrics: ExecutionMetrics): Unit = {
    if (exporters.isEmpty) {
      logger.debug("No exporters configured, skipping metrics export")
      return
    }

    // Export to all exporters (failures in one don't affect others)
    exporters.foreach { exporter =>
      try {
        exporter.export(metrics)
      } catch {
        case e: Exception =>
          logger.error(s"Failed to export metrics: ${e.getMessage}", e)
      }
    }
  }

  /**
   * Export metrics asynchronously.
   */
  def exportAsync(metrics: ExecutionMetrics): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.Future

    if (exporters.isEmpty) {
      logger.debug("No exporters configured, skipping async metrics export")
      return
    }

    // Export to all exporters concurrently
    val futures = exporters.map { exporter =>
      Future {
        try {
          exporter.export(metrics)
        } catch {
          case e: Exception =>
            logger.error(s"Failed to export metrics asynchronously: ${e.getMessage}", e)
        }
      }
    }

    // Don't wait for completion (fire and forget)
    // In production, you might want to track these futures
  }

  /**
   * Get count of active exporters.
   */
  def exporterCount: Int = exporters.size

  /**
   * Check if any exporters are configured.
   */
  def isEnabled: Boolean = exporters.nonEmpty

  /**
   * Close all exporters.
   */
  def close(): Unit = {
    exporters.foreach { exporter =>
      try {
        exporter.close()
      } catch {
        case e: Exception =>
          logger.error(s"Error closing exporter: ${e.getMessage}", e)
      }
    }
    logger.info(s"Closed ${exporters.size} metrics exporters")
  }
}

/**
 * Overall metrics exporter configuration.
 */
case class MetricsExporterConfig(
  prometheus: Option[PrometheusConfig] = None,
  cloudWatch: Option[CloudWatchConfig] = None
)
