package com.etl.monitoring

import com.etl.model.ExecutionMetrics
import io.prometheus.client._
import io.prometheus.client.exporter.{HTTPServer, PushGateway}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
 * Prometheus metrics exporter for ETL pipelines.
 *
 * Supports two modes:
 * 1. Push mode: Pushes metrics to Prometheus Pushgateway
 * 2. Pull mode: Exposes metrics HTTP endpoint for Prometheus to scrape
 *
 * Example usage:
 * {{{
 *   // Push mode
 *   val exporter = PrometheusMetricsExporter.createPushExporter(
 *     pushgatewayUrl = "localhost:9091",
 *     jobName = "etl-pipeline"
 *   )
 *
 *   // Pull mode
 *   val exporter = PrometheusMetricsExporter.createPullExporter(
 *     port = 9090
 *   )
 *
 *   // Export metrics
 *   exporter.export(executionMetrics)
 *
 *   // Cleanup
 *   exporter.close()
 * }}}
 */
trait PrometheusMetricsExporter {
  /**
   * Export execution metrics to Prometheus.
   */
  def export(metrics: ExecutionMetrics): Unit

  /**
   * Close and cleanup resources.
   */
  def close(): Unit
}

/**
 * Push-based Prometheus exporter using Pushgateway.
 */
class PrometheusPushExporter(
  pushgatewayUrl: String,
  jobName: String,
  instanceName: Option[String] = None
) extends PrometheusMetricsExporter {

  private val logger = LoggerFactory.getLogger(getClass)
  private val registry = new CollectorRegistry()
  private val pushGateway = new PushGateway(pushgatewayUrl)

  // Define metrics
  private val recordsProcessedCounter = Counter.build()
    .name("etl_records_processed_total")
    .help("Total number of records processed")
    .labelNames("pipeline_id", "stage")
    .register(registry)

  private val processingDurationGauge = Gauge.build()
    .name("etl_processing_duration_seconds")
    .help("Processing duration in seconds")
    .labelNames("pipeline_id")
    .register(registry)

  private val errorCounter = Counter.build()
    .name("etl_errors_total")
    .help("Total number of errors")
    .labelNames("pipeline_id", "error_type")
    .register(registry)

  private val pipelineSuccessGauge = Gauge.build()
    .name("etl_pipeline_success")
    .help("Pipeline execution success (1) or failure (0)")
    .labelNames("pipeline_id")
    .register(registry)

  private val dataQualityGauge = Gauge.build()
    .name("etl_data_quality_score")
    .help("Data quality score (0-100)")
    .labelNames("pipeline_id")
    .register(registry)

  override def export(metrics: ExecutionMetrics): Unit = {
    try {
      val pipelineId = metrics.pipelineId

      // Update counters and gauges
      recordsProcessedCounter
        .labels(pipelineId, "extract")
        .inc(metrics.extractedRecords.toDouble)

      recordsProcessedCounter
        .labels(pipelineId, "transform")
        .inc(metrics.transformedRecords.toDouble)

      recordsProcessedCounter
        .labels(pipelineId, "load")
        .inc(metrics.loadedRecords.toDouble)

      val durationSeconds = metrics.totalDurationMs / 1000.0
      processingDurationGauge
        .labels(pipelineId)
        .set(durationSeconds)

      errorCounter
        .labels(pipelineId, "total")
        .inc(metrics.errorCount.toDouble)

      pipelineSuccessGauge
        .labels(pipelineId)
        .set(if (metrics.success) 1.0 else 0.0)

      // Calculate simple data quality score
      val qualityScore = calculateQualityScore(metrics)
      dataQualityGauge
        .labels(pipelineId)
        .set(qualityScore)

      // Push to Pushgateway
      val groupingKey = instanceName match {
        case Some(instance) => Map("instance" -> instance)
        case None => Map.empty[String, String]
      }

      if (groupingKey.isEmpty) {
        pushGateway.push(registry, jobName)
      } else {
        import scala.jdk.CollectionConverters._
        pushGateway.push(registry, jobName, groupingKey.asJava)
      }

      logger.info(s"Pushed metrics to Prometheus Pushgateway: $pushgatewayUrl (job=$jobName)")

    } catch {
      case e: Exception =>
        logger.error(s"Failed to push metrics to Prometheus: ${e.getMessage}", e)
    }
  }

  override def close(): Unit = {
    try {
      // Optionally delete metrics from pushgateway on shutdown
      // pushGateway.delete(jobName)
      logger.info("Prometheus push exporter closed")
    } catch {
      case e: Exception =>
        logger.error(s"Error closing Prometheus exporter: ${e.getMessage}", e)
    }
  }

  /**
   * Calculate overall data quality score from metrics.
   */
  private def calculateQualityScore(metrics: ExecutionMetrics): Double = {
    if (metrics.extractedRecords == 0) return 100.0

    // Simple quality score: (loaded / extracted) * 100
    // In production, this would consider data quality validation results
    val successRate = metrics.loadedRecords.toDouble / metrics.extractedRecords.toDouble
    math.min(100.0, successRate * 100.0)
  }
}

/**
 * Pull-based Prometheus exporter using HTTP server.
 */
class PrometheusPullExporter(port: Int) extends PrometheusMetricsExporter {

  private val logger = LoggerFactory.getLogger(getClass)
  private val registry = CollectorRegistry.defaultRegistry

  // Start HTTP server for metrics scraping
  private val server: HTTPServer = new HTTPServer(port)

  // Define metrics (using default registry)
  private val recordsProcessedCounter = Counter.build()
    .name("etl_records_processed_total")
    .help("Total number of records processed")
    .labelNames("pipeline_id", "stage")
    .register(registry)

  private val processingDurationHistogram = Histogram.build()
    .name("etl_processing_duration_seconds")
    .help("Processing duration in seconds")
    .labelNames("pipeline_id")
    .buckets(1, 5, 10, 30, 60, 120, 300, 600) // Buckets: 1s, 5s, 10s, 30s, 1m, 2m, 5m, 10m
    .register(registry)

  private val errorCounter = Counter.build()
    .name("etl_errors_total")
    .help("Total number of errors")
    .labelNames("pipeline_id", "error_type")
    .register(registry)

  private val pipelineRunsCounter = Counter.build()
    .name("etl_pipeline_runs_total")
    .help("Total number of pipeline runs")
    .labelNames("pipeline_id", "status")
    .register(registry)

  private val throughputGauge = Gauge.build()
    .name("etl_throughput_records_per_second")
    .help("Processing throughput in records per second")
    .labelNames("pipeline_id")
    .register(registry)

  logger.info(s"Prometheus metrics HTTP server started on port $port")
  logger.info(s"Metrics available at: http://localhost:$port/metrics")

  override def export(metrics: ExecutionMetrics): Unit = {
    try {
      val pipelineId = metrics.pipelineId

      // Update counters
      recordsProcessedCounter
        .labels(pipelineId, "extract")
        .inc(metrics.extractedRecords.toDouble)

      recordsProcessedCounter
        .labels(pipelineId, "transform")
        .inc(metrics.transformedRecords.toDouble)

      recordsProcessedCounter
        .labels(pipelineId, "load")
        .inc(metrics.loadedRecords.toDouble)

      // Update histogram
      val durationSeconds = metrics.totalDurationMs / 1000.0
      processingDurationHistogram
        .labels(pipelineId)
        .observe(durationSeconds)

      // Update error counter
      errorCounter
        .labels(pipelineId, "total")
        .inc(metrics.errorCount.toDouble)

      // Update pipeline runs counter
      val status = if (metrics.success) "success" else "failure"
      pipelineRunsCounter
        .labels(pipelineId, status)
        .inc()

      // Calculate and update throughput
      if (metrics.totalDurationMs > 0) {
        val throughput = (metrics.loadedRecords * 1000.0) / metrics.totalDurationMs
        throughputGauge
          .labels(pipelineId)
          .set(throughput)
      }

      logger.debug(s"Updated Prometheus metrics for pipeline: $pipelineId")

    } catch {
      case e: Exception =>
        logger.error(s"Failed to update Prometheus metrics: ${e.getMessage}", e)
    }
  }

  override def close(): Unit = {
    try {
      server.stop()
      logger.info("Prometheus HTTP server stopped")
    } catch {
      case e: Exception =>
        logger.error(s"Error stopping Prometheus server: ${e.getMessage}", e)
    }
  }
}

/**
 * Companion object with factory methods.
 */
object PrometheusMetricsExporter {

  /**
   * Create push-based exporter.
   *
   * @param pushgatewayUrl Pushgateway URL (e.g., "localhost:9091")
   * @param jobName Job name for grouping metrics
   * @param instanceName Optional instance identifier
   */
  def createPushExporter(
    pushgatewayUrl: String,
    jobName: String,
    instanceName: Option[String] = None
  ): PrometheusMetricsExporter = {
    new PrometheusPushExporter(pushgatewayUrl, jobName, instanceName)
  }

  /**
   * Create pull-based exporter.
   *
   * @param port HTTP server port (default: 9090)
   */
  def createPullExporter(port: Int = 9090): PrometheusMetricsExporter = {
    new PrometheusPullExporter(port)
  }

  /**
   * Create exporter from configuration.
   */
  def fromConfig(config: PrometheusConfig): Try[PrometheusMetricsExporter] = Try {
    config.mode match {
      case "push" =>
        val pushgatewayUrl = config.pushgatewayUrl.getOrElse(
          throw new IllegalArgumentException("pushgatewayUrl required for push mode")
        )
        val jobName = config.jobName.getOrElse("etl-pipeline")
        createPushExporter(pushgatewayUrl, jobName, config.instanceName)

      case "pull" =>
        val port = config.port.getOrElse(9090)
        createPullExporter(port)

      case mode =>
        throw new IllegalArgumentException(s"Unknown Prometheus mode: $mode (expected 'push' or 'pull')")
    }
  }
}

/**
 * Configuration for Prometheus exporter.
 */
case class PrometheusConfig(
  enabled: Boolean = false,
  mode: String = "pull", // "push" or "pull"
  pushgatewayUrl: Option[String] = None,
  jobName: Option[String] = None,
  instanceName: Option[String] = None,
  port: Option[Int] = Some(9090)
)
