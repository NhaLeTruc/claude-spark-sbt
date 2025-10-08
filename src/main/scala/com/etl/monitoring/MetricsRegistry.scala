package com.etl.monitoring

import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap

/**
 * Central registry for all Prometheus metrics.
 *
 * Provides a singleton registry where all metrics are registered and can be
 * collected for export to Prometheus.
 *
 * Usage:
 * {{{
 * // Register metrics (typically done once at application startup)
 * val recordsProcessed = MetricsRegistry.counter(
 *   "etl_records_processed_total",
 *   "Total number of records processed",
 *   Seq("pipeline", "stage")
 * )
 *
 * // Use metrics
 * recordsProcessed.inc(labels = Map("pipeline" -> "my-pipeline", "stage" -> "extract"))
 *
 * // Export all metrics in Prometheus format
 * val metricsText = MetricsRegistry.exportMetrics()
 * }}}
 */
object MetricsRegistry {
  private val logger = LoggerFactory.getLogger(getClass)

  // Thread-safe metric storage
  private val metrics = new TrieMap[String, Metric]()

  /**
   * Register or get existing Counter metric.
   *
   * If a counter with the same name already exists, returns the existing counter.
   * Otherwise, creates and registers a new counter.
   *
   * @param name Metric name (must be unique across all metric types)
   * @param help Description of what this metric measures
   * @param labels Label names for this metric
   * @return Counter metric
   */
  def counter(
    name: String,
    help: String,
    labels: Seq[String] = Seq.empty
  ): Counter = {
    metrics.get(name) match {
      case Some(existing: Counter) =>
        logger.debug(s"Returning existing counter: $name")
        existing

      case Some(existing) =>
        throw new IllegalStateException(
          s"Metric $name already registered as ${existing.getClass.getSimpleName}, cannot register as Counter"
        )

      case None =>
        val counter = Counter(name, help, labels)
        metrics.put(name, counter)
        logger.info(s"Registered counter: $name (labels: ${labels.mkString(", ")})")
        counter
    }
  }

  /**
   * Register or get existing Gauge metric.
   *
   * @param name Metric name
   * @param help Description
   * @param labels Label names
   * @return Gauge metric
   */
  def gauge(
    name: String,
    help: String,
    labels: Seq[String] = Seq.empty
  ): Gauge = {
    metrics.get(name) match {
      case Some(existing: Gauge) =>
        logger.debug(s"Returning existing gauge: $name")
        existing

      case Some(existing) =>
        throw new IllegalStateException(
          s"Metric $name already registered as ${existing.getClass.getSimpleName}, cannot register as Gauge"
        )

      case None =>
        val gauge = Gauge(name, help, labels)
        metrics.put(name, gauge)
        logger.info(s"Registered gauge: $name (labels: ${labels.mkString(", ")})")
        gauge
    }
  }

  /**
   * Register or get existing Histogram metric.
   *
   * @param name Metric name
   * @param help Description
   * @param buckets Bucket boundaries
   * @param labels Label names
   * @return Histogram metric
   */
  def histogram(
    name: String,
    help: String,
    buckets: Seq[Double] = Histogram.defaultBuckets,
    labels: Seq[String] = Seq.empty
  ): Histogram = {
    metrics.get(name) match {
      case Some(existing: Histogram) =>
        logger.debug(s"Returning existing histogram: $name")
        existing

      case Some(existing) =>
        throw new IllegalStateException(
          s"Metric $name already registered as ${existing.getClass.getSimpleName}, cannot register as Histogram"
        )

      case None =>
        val histogram = Histogram(name, help, buckets, labels)
        metrics.put(name, histogram)
        logger.info(s"Registered histogram: $name (buckets: ${buckets.mkString(", ")})")
        histogram
    }
  }

  /**
   * Get metric by name.
   *
   * @param name Metric name
   * @return Some(metric) if found, None otherwise
   */
  def getMetric(name: String): Option[Metric] = {
    metrics.get(name)
  }

  /**
   * Export all metrics in Prometheus text format.
   *
   * @return Metrics in Prometheus exposition format
   */
  def exportMetrics(): String = {
    val sb = new StringBuilder

    metrics.values.toSeq.sortBy(_.name).foreach { metric =>
      sb.append(metric.prometheusText())
      sb.append("\n")
    }

    sb.toString()
  }

  /**
   * Get count of registered metrics.
   *
   * @return Number of registered metrics
   */
  def metricCount: Int = metrics.size

  /**
   * Clear all metrics (for testing).
   */
  def clear(): Unit = {
    metrics.clear()
    logger.info("Cleared all metrics from registry")
  }

  /**
   * Unregister a metric.
   *
   * @param name Metric name to unregister
   * @return true if metric was removed, false if not found
   */
  def unregister(name: String): Boolean = {
    metrics.remove(name) match {
      case Some(_) =>
        logger.info(s"Unregistered metric: $name")
        true
      case None =>
        logger.warn(s"Cannot unregister metric $name: not found")
        false
    }
  }

  /**
   * List all registered metric names.
   *
   * @return Seq of metric names
   */
  def metricNames: Seq[String] = {
    metrics.keys.toSeq.sorted
  }
}

/**
 * Standard ETL pipeline metrics.
 *
 * Pre-registered metrics that are commonly used across the ETL framework.
 * Provides a centralized place to define all framework metrics.
 */
object ETLMetrics {
  private val logger = LoggerFactory.getLogger(getClass)

  // Initialize all metrics on first access
  logger.info("Initializing ETL metrics")

  // ===== Pipeline Metrics =====

  val pipelineExecutionsTotal: Counter = MetricsRegistry.counter(
    "etl_pipeline_executions_total",
    "Total number of pipeline executions",
    labels = Seq("pipeline_id", "status")
  )

  val pipelineDurationSeconds: Histogram = MetricsRegistry.histogram(
    "etl_pipeline_duration_seconds",
    "Pipeline execution duration in seconds",
    buckets = Histogram.durationBuckets,
    labels = Seq("pipeline_id", "status")
  )

  val activePipelines: Gauge = MetricsRegistry.gauge(
    "etl_active_pipelines",
    "Number of currently running pipelines",
    labels = Seq("mode")
  )

  // ===== Record Metrics =====

  val recordsExtractedTotal: Counter = MetricsRegistry.counter(
    "etl_records_extracted_total",
    "Total number of records extracted from sources",
    labels = Seq("pipeline_id", "source_type")
  )

  val recordsTransformedTotal: Counter = MetricsRegistry.counter(
    "etl_records_transformed_total",
    "Total number of records transformed",
    labels = Seq("pipeline_id", "transform_type")
  )

  val recordsLoadedTotal: Counter = MetricsRegistry.counter(
    "etl_records_loaded_total",
    "Total number of records loaded to sinks",
    labels = Seq("pipeline_id", "sink_type", "write_mode")
  )

  val recordsFailedTotal: Counter = MetricsRegistry.counter(
    "etl_records_failed_total",
    "Total number of records that failed processing",
    labels = Seq("pipeline_id", "stage", "reason")
  )

  val recordBatchSize: Histogram = MetricsRegistry.histogram(
    "etl_record_batch_size",
    "Distribution of batch sizes (number of records)",
    buckets = Histogram.countBuckets,
    labels = Seq("pipeline_id", "stage")
  )

  // ===== Error Handling Metrics =====

  val retriesTotal: Counter = MetricsRegistry.counter(
    "etl_retries_total",
    "Total number of retry attempts",
    labels = Seq("pipeline_id", "stage", "strategy")
  )

  val circuitBreakerState: Gauge = MetricsRegistry.gauge(
    "etl_circuit_breaker_state",
    "Circuit breaker state (0=closed, 1=half_open, 2=open)",
    labels = Seq("pipeline_id", "component")
  )

  val circuitBreakerTransitionsTotal: Counter = MetricsRegistry.counter(
    "etl_circuit_breaker_transitions_total",
    "Total number of circuit breaker state transitions",
    labels = Seq("pipeline_id", "component", "from_state", "to_state")
  )

  val dlqRecordsTotal: Counter = MetricsRegistry.counter(
    "etl_dlq_records_total",
    "Total number of records sent to dead letter queue",
    labels = Seq("pipeline_id", "stage", "dlq_type")
  )

  // ===== Data Quality Metrics =====

  val dataQualityChecksTotal: Counter = MetricsRegistry.counter(
    "etl_data_quality_checks_total",
    "Total number of data quality checks performed",
    labels = Seq("pipeline_id", "rule_type", "severity", "status")
  )

  val dataQualityViolationsTotal: Counter = MetricsRegistry.counter(
    "etl_data_quality_violations_total",
    "Total number of data quality violations detected",
    labels = Seq("pipeline_id", "rule_name", "severity")
  )

  val schemaValidationFailuresTotal: Counter = MetricsRegistry.counter(
    "etl_schema_validation_failures_total",
    "Total number of schema validation failures",
    labels = Seq("pipeline_id", "stage", "schema_name")
  )

  // ===== Streaming Metrics =====

  val streamingBatchesProcessedTotal: Counter = MetricsRegistry.counter(
    "etl_streaming_batches_processed_total",
    "Total number of streaming batches processed",
    labels = Seq("pipeline_id", "query_name", "status")
  )

  val streamingBatchDurationSeconds: Histogram = MetricsRegistry.histogram(
    "etl_streaming_batch_duration_seconds",
    "Streaming batch processing duration",
    buckets = Histogram.defaultBuckets,
    labels = Seq("pipeline_id", "query_name")
  )

  val streamingLagSeconds: Gauge = MetricsRegistry.gauge(
    "etl_streaming_lag_seconds",
    "Streaming lag in seconds (time since last processed batch)",
    labels = Seq("pipeline_id", "query_name")
  )

  val streamingActiveQueries: Gauge = MetricsRegistry.gauge(
    "etl_streaming_active_queries",
    "Number of active streaming queries"
  )

  val streamingDuplicateBatchesTotal: Counter = MetricsRegistry.counter(
    "etl_streaming_duplicate_batches_total",
    "Total number of duplicate batches skipped (exactly-once semantics)",
    labels = Seq("pipeline_id", "query_name")
  )

  val batchTrackerRecords: Gauge = MetricsRegistry.gauge(
    "etl_batch_tracker_records",
    "Number of records in batch tracker table",
    labels = Seq("pipeline_id")
  )

  // ===== Performance Metrics =====

  val extractDurationSeconds: Histogram = MetricsRegistry.histogram(
    "etl_extract_duration_seconds",
    "Time spent in extract stage",
    buckets = Histogram.defaultBuckets,
    labels = Seq("pipeline_id", "source_type")
  )

  val transformDurationSeconds: Histogram = MetricsRegistry.histogram(
    "etl_transform_duration_seconds",
    "Time spent in transform stage",
    buckets = Histogram.defaultBuckets,
    labels = Seq("pipeline_id", "transform_type")
  )

  val loadDurationSeconds: Histogram = MetricsRegistry.histogram(
    "etl_load_duration_seconds",
    "Time spent in load stage",
    buckets = Histogram.defaultBuckets,
    labels = Seq("pipeline_id", "sink_type")
  )

  // ===== Resource Metrics =====

  val sparkExecutors: Gauge = MetricsRegistry.gauge(
    "etl_spark_executors",
    "Number of active Spark executors"
  )

  val sparkActiveJobs: Gauge = MetricsRegistry.gauge(
    "etl_spark_active_jobs",
    "Number of active Spark jobs"
  )

  val sparkActiveStages: Gauge = MetricsRegistry.gauge(
    "etl_spark_active_stages",
    "Number of active Spark stages"
  )

  logger.info(s"Initialized ${MetricsRegistry.metricCount} ETL metrics")
}
