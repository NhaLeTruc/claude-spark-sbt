package com.etl.monitoring

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.services.cloudwatch.model._
import com.amazonaws.services.cloudwatch.{AmazonCloudWatch, AmazonCloudWatchClientBuilder}
import com.etl.model.ExecutionMetrics
import org.slf4j.LoggerFactory

import java.util.Date
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/**
 * CloudWatch metrics exporter for ETL pipelines.
 *
 * Publishes pipeline metrics to AWS CloudWatch for monitoring and alerting.
 *
 * Example usage:
 * {{{
 *   val exporter = CloudWatchMetricsExporter(
 *     namespace = "ETL/Pipelines",
 *     region = "us-east-1"
 *   )
 *
 *   exporter.export(executionMetrics)
 *
 *   exporter.close()
 * }}}
 *
 * Metrics exported:
 * - RecordsProcessed (per stage: extract, transform, load)
 * - ProcessingDuration (milliseconds)
 * - ErrorCount
 * - PipelineSuccess (0 or 1)
 * - Throughput (records/second)
 * - DataQualityScore (0-100)
 */
class CloudWatchMetricsExporter(
  namespace: String,
  region: String,
  credentialsProvider: Option[(String, String)] = None
) {

  private val logger = LoggerFactory.getLogger(getClass)

  // Create CloudWatch client
  private val cloudWatch: AmazonCloudWatch = {
    val builder = AmazonCloudWatchClientBuilder.standard()
      .withRegion(region)

    credentialsProvider match {
      case Some((accessKey, secretKey)) =>
        val credentials = new BasicAWSCredentials(accessKey, secretKey)
        builder.withCredentials(new AWSStaticCredentialsProvider(credentials))
      case None =>
        builder.withCredentials(new DefaultAWSCredentialsProviderChain())
    }

    builder.build()
  }

  logger.info(s"CloudWatch metrics exporter initialized (namespace=$namespace, region=$region)")

  /**
   * Export execution metrics to CloudWatch.
   */
  def export(metrics: ExecutionMetrics): Unit = {
    try {
      val timestamp = new Date()
      val pipelineId = metrics.pipelineId

      // Create dimensions for all metrics
      val pipelineDimension = new Dimension()
        .withName("PipelineId")
        .withValue(pipelineId)

      val metricData = scala.collection.mutable.ArrayBuffer[MetricDatum]()

      // Records processed metrics (by stage)
      metricData += createMetric(
        name = "RecordsProcessed",
        value = metrics.extractedRecords.toDouble,
        unit = StandardUnit.Count,
        timestamp = timestamp,
        dimensions = Seq(
          pipelineDimension,
          new Dimension().withName("Stage").withValue("Extract")
        )
      )

      metricData += createMetric(
        name = "RecordsProcessed",
        value = metrics.transformedRecords.toDouble,
        unit = StandardUnit.Count,
        timestamp = timestamp,
        dimensions = Seq(
          pipelineDimension,
          new Dimension().withName("Stage").withValue("Transform")
        )
      )

      metricData += createMetric(
        name = "RecordsProcessed",
        value = metrics.loadedRecords.toDouble,
        unit = StandardUnit.Count,
        timestamp = timestamp,
        dimensions = Seq(
          pipelineDimension,
          new Dimension().withName("Stage").withValue("Load")
        )
      )

      // Processing duration
      metricData += createMetric(
        name = "ProcessingDuration",
        value = metrics.totalDurationMs.toDouble,
        unit = StandardUnit.Milliseconds,
        timestamp = timestamp,
        dimensions = Seq(pipelineDimension)
      )

      // Error count
      metricData += createMetric(
        name = "ErrorCount",
        value = metrics.errorCount.toDouble,
        unit = StandardUnit.Count,
        timestamp = timestamp,
        dimensions = Seq(pipelineDimension)
      )

      // Pipeline success (binary: 0 or 1)
      metricData += createMetric(
        name = "PipelineSuccess",
        value = if (metrics.success) 1.0 else 0.0,
        unit = StandardUnit.None,
        timestamp = timestamp,
        dimensions = Seq(pipelineDimension)
      )

      // Throughput (records per second)
      if (metrics.totalDurationMs > 0) {
        val throughput = (metrics.loadedRecords * 1000.0) / metrics.totalDurationMs
        metricData += createMetric(
          name = "Throughput",
          value = throughput,
          unit = StandardUnit.CountSecond,
          timestamp = timestamp,
          dimensions = Seq(pipelineDimension)
        )
      }

      // Data quality score
      val qualityScore = calculateQualityScore(metrics)
      metricData += createMetric(
        name = "DataQualityScore",
        value = qualityScore,
        unit = StandardUnit.Percent,
        timestamp = timestamp,
        dimensions = Seq(pipelineDimension)
      )

      // Stage durations
      if (metrics.extractDurationMs > 0) {
        metricData += createMetric(
          name = "StageDuration",
          value = metrics.extractDurationMs.toDouble,
          unit = StandardUnit.Milliseconds,
          timestamp = timestamp,
          dimensions = Seq(
            pipelineDimension,
            new Dimension().withName("Stage").withValue("Extract")
          )
        )
      }

      if (metrics.transformDurationMs > 0) {
        metricData += createMetric(
          name = "StageDuration",
          value = metrics.transformDurationMs.toDouble,
          unit = StandardUnit.Milliseconds,
          timestamp = timestamp,
          dimensions = Seq(
            pipelineDimension,
            new Dimension().withName("Stage").withValue("Transform")
          )
        )
      }

      if (metrics.loadDurationMs > 0) {
        metricData += createMetric(
          name = "StageDuration",
          value = metrics.loadDurationMs.toDouble,
          unit = StandardUnit.Milliseconds,
          timestamp = timestamp,
          dimensions = Seq(
            pipelineDimension,
            new Dimension().withName("Stage").withValue("Load")
          )
        )
      }

      // Publish metrics in batches (CloudWatch limit: 20 metrics per request)
      metricData.grouped(20).foreach { batch =>
        val request = new PutMetricDataRequest()
          .withNamespace(namespace)
          .withMetricData(batch.asJava)

        cloudWatch.putMetricData(request)
      }

      logger.info(s"Published ${metricData.size} metrics to CloudWatch for pipeline: $pipelineId")

    } catch {
      case e: Exception =>
        logger.error(s"Failed to publish metrics to CloudWatch: ${e.getMessage}", e)
    }
  }

  /**
   * Create a CloudWatch metric datum.
   */
  private def createMetric(
    name: String,
    value: Double,
    unit: StandardUnit,
    timestamp: Date,
    dimensions: Seq[Dimension]
  ): MetricDatum = {
    new MetricDatum()
      .withMetricName(name)
      .withValue(value)
      .withUnit(unit)
      .withTimestamp(timestamp)
      .withDimensions(dimensions.asJava)
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

  /**
   * Publish custom metric.
   *
   * Allows publishing arbitrary metrics beyond standard execution metrics.
   */
  def publishCustomMetric(
    name: String,
    value: Double,
    unit: StandardUnit = StandardUnit.None,
    dimensions: Map[String, String] = Map.empty
  ): Unit = {
    try {
      val dimensionList = dimensions.map { case (key, value) =>
        new Dimension().withName(key).withValue(value)
      }.toSeq

      val metricDatum = createMetric(
        name = name,
        value = value,
        unit = unit,
        timestamp = new Date(),
        dimensions = dimensionList
      )

      val request = new PutMetricDataRequest()
        .withNamespace(namespace)
        .withMetricData(metricDatum)

      cloudWatch.putMetricData(request)

      logger.debug(s"Published custom metric: $name = $value")

    } catch {
      case e: Exception =>
        logger.error(s"Failed to publish custom metric: ${e.getMessage}", e)
    }
  }

  /**
   * Close and cleanup resources.
   */
  def close(): Unit = {
    try {
      cloudWatch.shutdown()
      logger.info("CloudWatch metrics exporter closed")
    } catch {
      case e: Exception =>
        logger.error(s"Error closing CloudWatch exporter: ${e.getMessage}", e)
    }
  }
}

/**
 * Companion object with factory methods.
 */
object CloudWatchMetricsExporter {

  /**
   * Create CloudWatch exporter with AWS credentials.
   */
  def apply(
    namespace: String,
    region: String,
    accessKey: String,
    secretKey: String
  ): CloudWatchMetricsExporter = {
    new CloudWatchMetricsExporter(namespace, region, Some((accessKey, secretKey)))
  }

  /**
   * Create CloudWatch exporter using default credentials chain.
   */
  def apply(
    namespace: String,
    region: String
  ): CloudWatchMetricsExporter = {
    new CloudWatchMetricsExporter(namespace, region, None)
  }

  /**
   * Create exporter from configuration.
   */
  def fromConfig(config: CloudWatchConfig): Try[CloudWatchMetricsExporter] = Try {
    val namespace = config.namespace.getOrElse("ETL/Pipelines")
    val region = config.region.getOrElse("us-east-1")

    (config.accessKey, config.secretKey) match {
      case (Some(accessKey), Some(secretKey)) =>
        CloudWatchMetricsExporter(namespace, region, accessKey, secretKey)
      case _ =>
        CloudWatchMetricsExporter(namespace, region)
    }
  }
}

/**
 * Configuration for CloudWatch exporter.
 */
case class CloudWatchConfig(
  enabled: Boolean = false,
  namespace: Option[String] = Some("ETL/Pipelines"),
  region: Option[String] = Some("us-east-1"),
  accessKey: Option[String] = None,
  secretKey: Option[String] = None
)
