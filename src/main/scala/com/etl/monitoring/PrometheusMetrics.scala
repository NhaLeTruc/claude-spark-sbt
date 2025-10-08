package com.etl.monitoring

import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.concurrent.TrieMap

/**
 * Prometheus-compatible metrics implementation.
 *
 * Provides Counter, Gauge, and Histogram metric types that can be exported
 * in Prometheus text format via the /metrics endpoint.
 *
 * Metrics are thread-safe and suitable for concurrent updates from multiple
 * Spark executors and driver threads.
 *
 * Usage:
 * {{{
 * val counter = Counter("records_processed_total", "Total records processed", Seq("pipeline", "stage"))
 * counter.inc(labels = Map("pipeline" -> "my-pipeline", "stage" -> "extract"))
 *
 * val gauge = Gauge("active_pipelines", "Number of active pipelines")
 * gauge.set(5.0)
 *
 * val histogram = Histogram("pipeline_duration_seconds", "Pipeline execution duration", buckets = Seq(1, 5, 10, 30, 60))
 * histogram.observe(12.5, labels = Map("pipeline" -> "my-pipeline"))
 * }}}
 */
sealed trait Metric {
  def name: String
  def help: String
  def labels: Seq[String]
  def prometheusText(): String
}

/**
 * Counter metric - monotonically increasing value.
 *
 * Use for: total records processed, total errors, total requests, etc.
 *
 * @param name Metric name (e.g., "records_processed_total")
 * @param help Description of what this metric measures
 * @param labels Label names for this metric (e.g., Seq("pipeline", "stage"))
 */
case class Counter(
  name: String,
  help: String,
  labels: Seq[String] = Seq.empty
) extends Metric {
  private val logger = LoggerFactory.getLogger(getClass)
  private val values = new TrieMap[Map[String, String], Double]()

  /**
   * Increment counter by 1.
   *
   * @param labels Label values (must match label names)
   */
  def inc(labels: Map[String, String] = Map.empty): Unit = {
    inc(1.0, labels)
  }

  /**
   * Increment counter by specific amount.
   *
   * @param amount Amount to increment (must be >= 0)
   * @param labels Label values
   */
  def inc(amount: Double, labels: Map[String, String]): Unit = {
    require(amount >= 0, "Counter increment must be non-negative")

    val labelKey = normalizeLabelMap(labels)
    values.updateWith(labelKey) {
      case Some(current) => Some(current + amount)
      case None => Some(amount)
    }

    logger.trace(s"Counter $name incremented by $amount (labels: $labelKey)")
  }

  /**
   * Get current counter value.
   *
   * @param labels Label values
   * @return Current value, or 0 if not set
   */
  def get(labels: Map[String, String] = Map.empty): Double = {
    values.getOrElse(normalizeLabelMap(labels), 0.0)
  }

  /**
   * Reset counter to 0 (for testing).
   */
  def reset(): Unit = {
    values.clear()
  }

  override def prometheusText(): String = {
    val sb = new StringBuilder

    sb.append(s"# HELP $name $help\n")
    sb.append(s"# TYPE $name counter\n")

    if (values.isEmpty) {
      // Export 0 for metrics with no samples
      if (labels.isEmpty) {
        sb.append(s"$name 0\n")
      }
    } else {
      values.foreach { case (labelMap, value) =>
        val labelStr = formatLabels(labelMap)
        sb.append(s"$name$labelStr $value\n")
      }
    }

    sb.toString()
  }

  private def normalizeLabelMap(labelMap: Map[String, String]): Map[String, String] = {
    // Ensure all expected labels are present
    labels.map { labelName =>
      labelName -> labelMap.getOrElse(labelName, "")
    }.toMap
  }

  private def formatLabels(labelMap: Map[String, String]): String = {
    if (labelMap.isEmpty || labelMap.values.forall(_.isEmpty)) {
      ""
    } else {
      val labelPairs = labelMap.toSeq.sortBy(_._1).map { case (k, v) =>
        s"""$k="$v""""
      }.mkString(",")
      s"{$labelPairs}"
    }
  }
}

/**
 * Gauge metric - value that can go up and down.
 *
 * Use for: active connections, queue size, temperature, memory usage, etc.
 *
 * @param name Metric name (e.g., "active_pipelines")
 * @param help Description of what this metric measures
 * @param labels Label names for this metric
 */
case class Gauge(
  name: String,
  help: String,
  labels: Seq[String] = Seq.empty
) extends Metric {
  private val logger = LoggerFactory.getLogger(getClass)
  private val values = new TrieMap[Map[String, String], Double]()

  /**
   * Set gauge to specific value.
   *
   * @param value Value to set
   * @param labels Label values
   */
  def set(value: Double, labels: Map[String, String] = Map.empty): Unit = {
    val labelKey = normalizeLabelMap(labels)
    values.put(labelKey, value)
    logger.trace(s"Gauge $name set to $value (labels: $labelKey)")
  }

  /**
   * Increment gauge by 1.
   *
   * @param labels Label values
   */
  def inc(labels: Map[String, String] = Map.empty): Unit = {
    inc(1.0, labels)
  }

  /**
   * Increment gauge by specific amount.
   *
   * @param amount Amount to increment
   * @param labels Label values
   */
  def inc(amount: Double, labels: Map[String, String]): Unit = {
    val labelKey = normalizeLabelMap(labels)
    values.updateWith(labelKey) {
      case Some(current) => Some(current + amount)
      case None => Some(amount)
    }
    logger.trace(s"Gauge $name incremented by $amount (labels: $labelKey)")
  }

  /**
   * Decrement gauge by 1.
   *
   * @param labels Label values
   */
  def dec(labels: Map[String, String] = Map.empty): Unit = {
    inc(-1.0, labels)
  }

  /**
   * Decrement gauge by specific amount.
   *
   * @param amount Amount to decrement
   * @param labels Label values
   */
  def dec(amount: Double, labels: Map[String, String]): Unit = {
    inc(-amount, labels)
  }

  /**
   * Get current gauge value.
   *
   * @param labels Label values
   * @return Current value, or 0 if not set
   */
  def get(labels: Map[String, String] = Map.empty): Double = {
    values.getOrElse(normalizeLabelMap(labels), 0.0)
  }

  /**
   * Reset gauge to 0 (for testing).
   */
  def reset(): Unit = {
    values.clear()
  }

  override def prometheusText(): String = {
    val sb = new StringBuilder

    sb.append(s"# HELP $name $help\n")
    sb.append(s"# TYPE $name gauge\n")

    if (values.isEmpty) {
      if (labels.isEmpty) {
        sb.append(s"$name 0\n")
      }
    } else {
      values.foreach { case (labelMap, value) =>
        val labelStr = formatLabels(labelMap)
        sb.append(s"$name$labelStr $value\n")
      }
    }

    sb.toString()
  }

  private def normalizeLabelMap(labelMap: Map[String, String]): Map[String, String] = {
    labels.map { labelName =>
      labelName -> labelMap.getOrElse(labelName, "")
    }.toMap
  }

  private def formatLabels(labelMap: Map[String, String]): String = {
    if (labelMap.isEmpty || labelMap.values.forall(_.isEmpty)) {
      ""
    } else {
      val labelPairs = labelMap.toSeq.sortBy(_._1).map { case (k, v) =>
        s"""$k="$v""""
      }.mkString(",")
      s"{$labelPairs}"
    }
  }
}

/**
 * Histogram metric - distribution of values with buckets.
 *
 * Use for: request duration, response size, batch size, latency, etc.
 *
 * Automatically tracks:
 * - Count of observations
 * - Sum of all values
 * - Buckets (cumulative counts <= bucket boundary)
 *
 * @param name Metric name (e.g., "pipeline_duration_seconds")
 * @param help Description of what this metric measures
 * @param buckets Bucket boundaries (default: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10])
 * @param labels Label names for this metric
 */
case class Histogram(
  name: String,
  help: String,
  buckets: Seq[Double] = Histogram.defaultBuckets,
  labels: Seq[String] = Seq.empty
) extends Metric {
  private val logger = LoggerFactory.getLogger(getClass)

  // Each label combination gets its own histogram data
  private val observations = new TrieMap[Map[String, String], HistogramData]()

  private case class HistogramData(
    var count: Long = 0L,
    var sum: Double = 0.0,
    bucketCounts: mutable.Map[Double, Long] = mutable.Map(buckets.map(_ -> 0L): _*)
  )

  /**
   * Observe a value.
   *
   * @param value Value to observe
   * @param labels Label values
   */
  def observe(value: Double, labels: Map[String, String] = Map.empty): Unit = {
    val labelKey = normalizeLabelMap(labels)

    observations.updateWith(labelKey) {
      case Some(data) =>
        data.count += 1
        data.sum += value

        // Update bucket counts
        buckets.foreach { bucket =>
          if (value <= bucket) {
            data.bucketCounts(bucket) += 1
          }
        }

        Some(data)

      case None =>
        val data = HistogramData()
        data.count = 1
        data.sum = value

        buckets.foreach { bucket =>
          if (value <= bucket) {
            data.bucketCounts(bucket) = 1
          }
        }

        Some(data)
    }

    logger.trace(s"Histogram $name observed $value (labels: $labelKey)")
  }

  /**
   * Get count of observations.
   *
   * @param labels Label values
   * @return Count of observations
   */
  def count(labels: Map[String, String] = Map.empty): Long = {
    observations.get(normalizeLabelMap(labels)).map(_.count).getOrElse(0L)
  }

  /**
   * Get sum of all observed values.
   *
   * @param labels Label values
   * @return Sum of values
   */
  def sum(labels: Map[String, String] = Map.empty): Double = {
    observations.get(normalizeLabelMap(labels)).map(_.sum).getOrElse(0.0)
  }

  /**
   * Reset histogram (for testing).
   */
  def reset(): Unit = {
    observations.clear()
  }

  override def prometheusText(): String = {
    val sb = new StringBuilder

    sb.append(s"# HELP $name $help\n")
    sb.append(s"# TYPE $name histogram\n")

    if (observations.isEmpty) {
      if (labels.isEmpty) {
        // Export empty histogram
        buckets.foreach { bucket =>
          sb.append(s"${name}_bucket{le=\"$bucket\"} 0\n")
        }
        sb.append(s"${name}_bucket{le=\"+Inf\"} 0\n")
        sb.append(s"${name}_sum 0\n")
        sb.append(s"${name}_count 0\n")
      }
    } else {
      observations.foreach { case (labelMap, data) =>
        val labelStr = formatLabels(labelMap, includeComma = true)

        // Bucket counts (cumulative)
        buckets.foreach { bucket =>
          val count = data.bucketCounts(bucket)
          sb.append(s"${name}_bucket${formatBucketLabels(labelMap, bucket)} $count\n")
        }

        // +Inf bucket (total count)
        sb.append(s"${name}_bucket${formatBucketLabels(labelMap, "+Inf")} ${data.count}\n")

        // Sum and count
        sb.append(s"${name}_sum$labelStr ${data.sum}\n")
        sb.append(s"${name}_count$labelStr ${data.count}\n")
      }
    }

    sb.toString()
  }

  private def normalizeLabelMap(labelMap: Map[String, String]): Map[String, String] = {
    labels.map { labelName =>
      labelName -> labelMap.getOrElse(labelName, "")
    }.toMap
  }

  private def formatLabels(labelMap: Map[String, String], includeComma: Boolean = false): String = {
    if (labelMap.isEmpty || labelMap.values.forall(_.isEmpty)) {
      ""
    } else {
      val labelPairs = labelMap.toSeq.sortBy(_._1).map { case (k, v) =>
        s"""$k="$v""""
      }.mkString(",")
      s"{$labelPairs}"
    }
  }

  private def formatBucketLabels(labelMap: Map[String, String], le: Any): String = {
    val leLabel = s"""le="$le""""

    if (labelMap.isEmpty || labelMap.values.forall(_.isEmpty)) {
      s"{$leLabel}"
    } else {
      val labelPairs = labelMap.toSeq.sortBy(_._1).map { case (k, v) =>
        s"""$k="$v""""
      }.mkString(",")
      s"{$labelPairs,$leLabel}"
    }
  }
}

object Histogram {
  /**
   * Default histogram buckets (in seconds):
   * 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2.5s, 5s, 10s
   */
  val defaultBuckets: Seq[Double] = Seq(
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0
  )

  /**
   * Buckets for large durations (in seconds):
   * 1s, 5s, 10s, 30s, 60s, 120s, 300s (5min), 600s (10min)
   */
  val durationBuckets: Seq[Double] = Seq(
    1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0
  )

  /**
   * Buckets for record counts:
   * 10, 100, 1K, 10K, 100K, 1M, 10M
   */
  val countBuckets: Seq[Double] = Seq(
    10, 100, 1000, 10000, 100000, 1000000, 10000000
  )

  /**
   * Buckets for sizes (in bytes):
   * 1KB, 10KB, 100KB, 1MB, 10MB, 100MB, 1GB
   */
  val sizeBuckets: Seq[Double] = Seq(
    1024, 10240, 102400, 1048576, 10485760, 104857600, 1073741824
  )
}
