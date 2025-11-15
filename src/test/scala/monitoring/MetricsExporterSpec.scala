package monitoring

import com.etl.model.ExecutionMetrics
import com.etl.monitoring._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests for metrics exporters (Prometheus and CloudWatch).
 *
 * These tests demonstrate configuration and usage of metrics exporters.
 *
 * Note: Some tests are marked as 'ignore' because they require:
 * - Prometheus Pushgateway running (localhost:9091)
 * - AWS credentials and CloudWatch access
 *
 * Run with: sbt "testOnly monitoring.MetricsExporterSpec"
 */
class MetricsExporterSpec extends AnyFlatSpec with Matchers {

  /**
   * Create sample execution metrics for testing.
   */
  def createSampleMetrics(pipelineId: String = "test-pipeline"): ExecutionMetrics = {
    ExecutionMetrics(
      pipelineId = pipelineId,
      extractedRecords = 10000,
      transformedRecords = 9500,
      loadedRecords = 9500,
      errorCount = 0,
      extractDurationMs = 1000,
      transformDurationMs = 2000,
      loadDurationMs = 1500,
      totalDurationMs = 4500,
      success = true
    )
  }

  /**
   * Test: PrometheusConfig creation.
   */
  "PrometheusConfig" should "have sensible defaults" in {
    val config = PrometheusConfig()

    config.enabled shouldBe false
    config.mode shouldBe "pull"
    config.port shouldBe Some(9090)
  }

  /**
   * Test: CloudWatchConfig creation.
   */
  "CloudWatchConfig" should "have sensible defaults" in {
    val config = CloudWatchConfig()

    config.enabled shouldBe false
    config.namespace shouldBe Some("ETL/Pipelines")
    config.region shouldBe Some("us-east-1")
  }

  /**
   * Test: Create Prometheus pull exporter.
   */
  "PrometheusMetricsExporter" should "create pull exporter" ignore {
    // Ignored by default - requires running server
    // To test: Remove 'ignore' and run manually

    val exporter = PrometheusMetricsExporter.createPullExporter(port = 9999)

    try {
      val metrics = createSampleMetrics()
      exporter.export(metrics)

      println("✓ Prometheus pull exporter created")
      println("  Metrics available at: http://localhost:9999/metrics")
      println("  Open in browser to view metrics")

      // Give some time to check metrics in browser
      Thread.sleep(5000)

    } finally {
      exporter.close()
    }
  }

  /**
   * Test: Create Prometheus push exporter.
   */
  "PrometheusMetricsExporter" should "create push exporter" ignore {
    // Ignored by default - requires Prometheus Pushgateway
    // Start Pushgateway: docker run -d -p 9091:9091 prom/pushgateway

    val exporter = PrometheusMetricsExporter.createPushExporter(
      pushgatewayUrl = "localhost:9091",
      jobName = "etl-test"
    )

    try {
      val metrics = createSampleMetrics()
      exporter.export(metrics)

      println("✓ Prometheus push exporter created")
      println("  Pushed metrics to Pushgateway")
      println("  View at: http://localhost:9091/metrics")

    } finally {
      exporter.close()
    }
  }

  /**
   * Test: Create CloudWatch exporter.
   */
  "CloudWatchMetricsExporter" should "create exporter with config" ignore {
    // Ignored by default - requires AWS credentials

    val config = CloudWatchConfig(
      enabled = true,
      namespace = Some("ETL/Test"),
      region = Some("us-east-1")
    )

    CloudWatchMetricsExporter.fromConfig(config) match {
      case scala.util.Success(exporter) =>
        try {
          val metrics = createSampleMetrics()
          exporter.export(metrics)

          println("✓ CloudWatch exporter created and metrics published")
          println("  Check CloudWatch console for metrics")

        } finally {
          exporter.close()
        }

      case scala.util.Failure(e) =>
        fail(s"Failed to create CloudWatch exporter: ${e.getMessage}")
    }
  }

  /**
   * Test: MetricsExporterFactory with multiple exporters.
   */
  "MetricsExporterFactory" should "create manager with multiple exporters" ignore {
    // Ignored by default - requires Prometheus/CloudWatch setup

    val config = MetricsExporterConfig(
      prometheus = Some(PrometheusConfig(
        enabled = true,
        mode = "pull",
        port = Some(9998)
      )),
      cloudWatch = Some(CloudWatchConfig(
        enabled = true,
        namespace = Some("ETL/Test"),
        region = Some("us-east-1")
      ))
    )

    val manager = MetricsExporterFactory.createManager(config)

    try {
      manager.isEnabled shouldBe true
      manager.exporterCount should be > 0

      val metrics = createSampleMetrics()
      manager.export(metrics)

      println(s"✓ Metrics exported to ${manager.exporterCount} destinations")

    } finally {
      manager.close()
    }
  }

  /**
   * Test: Factory convenience methods.
   */
  "MetricsExporterFactory" should "provide convenience methods" ignore {
    // Test Prometheus-only
    val prometheusManager = MetricsExporterFactory.createPrometheusOnly(
      mode = "pull",
      port = 9997
    )

    try {
      prometheusManager.isEnabled shouldBe true
      prometheusManager.exporterCount shouldBe 1

      val metrics = createSampleMetrics()
      prometheusManager.export(metrics)

      println("✓ Prometheus-only manager created")

    } finally {
      prometheusManager.close()
    }

    // Test CloudWatch-only
    val cloudWatchManager = MetricsExporterFactory.createCloudWatchOnly(
      namespace = "ETL/Test",
      region = "us-east-1"
    )

    try {
      cloudWatchManager.isEnabled shouldBe true

      println("✓ CloudWatch-only manager created")

    } finally {
      cloudWatchManager.close()
    }
  }

  /**
   * Test: Async export.
   */
  "MetricsExporterManager" should "support async export" ignore {
    val manager = MetricsExporterFactory.createPrometheusOnly(port = 9996)

    try {
      val metrics = createSampleMetrics()

      // Export asynchronously
      manager.exportAsync(metrics)

      // Give async export time to complete
      Thread.sleep(1000)

      println("✓ Async metrics export completed")

    } finally {
      manager.close()
    }
  }

  /**
   * Test: Manager with no exporters.
   */
  "MetricsExporterManager" should "handle no exporters gracefully" in {
    val config = MetricsExporterConfig() // No exporters enabled

    val manager = MetricsExporterFactory.createManager(config)

    manager.isEnabled shouldBe false
    manager.exporterCount shouldBe 0

    // Should not throw exception
    val metrics = createSampleMetrics()
    manager.export(metrics)

    manager.close()

    println("✓ Manager handles no exporters gracefully")
  }

  /**
   * Test: Export error handling.
   */
  "MetricsExporterManager" should "handle export errors gracefully" in {
    // Create a failing exporter
    val failingExporter = new MetricsExporter with AutoCloseable {
      override def export(metrics: ExecutionMetrics): Unit = {
        throw new RuntimeException("Simulated export failure")
      }
      override def close(): Unit = {}
    }

    val manager = new MetricsExporterManager(Seq(failingExporter))

    // Should not throw exception - errors are logged
    val metrics = createSampleMetrics()
    manager.export(metrics)

    manager.close()

    println("✓ Manager handles export errors gracefully")
  }

  /**
   * Test: Validate Prometheus config.
   */
  "PrometheusConfig" should "validate correctly" in {
    // Valid pull config
    val pullConfig = PrometheusConfig(
      enabled = true,
      mode = "pull",
      port = Some(9090)
    )
    PrometheusMetricsExporter.fromConfig(pullConfig) should matchPattern {
      case scala.util.Success(_) =>
    }

    // Valid push config
    val pushConfig = PrometheusConfig(
      enabled = true,
      mode = "push",
      pushgatewayUrl = Some("localhost:9091"),
      jobName = Some("test")
    )
    PrometheusMetricsExporter.fromConfig(pushConfig) should matchPattern {
      case scala.util.Success(_) =>
    }

    // Invalid: push mode without pushgatewayUrl
    val invalidConfig = PrometheusConfig(
      enabled = true,
      mode = "push"
    )
    PrometheusMetricsExporter.fromConfig(invalidConfig) should matchPattern {
      case scala.util.Failure(_) =>
    }

    println("✓ Prometheus config validation working")
  }

  /**
   * Test: Multiple metrics export.
   */
  "MetricsExporterManager" should "export multiple metrics" ignore {
    val manager = MetricsExporterFactory.createPrometheusOnly(port = 9995)

    try {
      // Export metrics from multiple pipeline runs
      (1 to 5).foreach { i =>
        val metrics = createSampleMetrics(s"pipeline-$i")
        manager.export(metrics)
      }

      println("✓ Multiple metrics exported successfully")
      println("  Check metrics endpoint for accumulated data")

    } finally {
      manager.close()
    }
  }
}

/**
 * Example: End-to-end metrics export demonstration.
 *
 * This is not a test - it's a runnable example showing how to use
 * metrics exporters in a real pipeline.
 */
object MetricsExporterExample {

  def runExample(): Unit = {
    println("\n=== Metrics Exporter Example ===\n")

    // Configure exporters
    val config = MetricsExporterConfig(
      prometheus = Some(PrometheusConfig(
        enabled = true,
        mode = "pull",
        port = Some(9090)
      ))
      // CloudWatch disabled for this example
      // cloudWatch = Some(CloudWatchConfig(enabled = true))
    )

    val manager = MetricsExporterFactory.createManager(config)

    try {
      println(s"Created metrics exporter manager with ${manager.exporterCount} exporter(s)")

      // Simulate multiple pipeline runs
      (1 to 3).foreach { run =>
        println(s"\nPipeline run $run...")

        val metrics = ExecutionMetrics(
          pipelineId = s"example-pipeline-$run",
          extractedRecords = 10000 * run,
          transformedRecords = 9500 * run,
          loadedRecords = 9500 * run,
          errorCount = 0,
          extractDurationMs = 1000,
          transformDurationMs = 2000,
          loadDurationMs = 1500,
          totalDurationMs = 4500,
          success = true
        )

        // Export metrics
        manager.export(metrics)
        println(s"  Exported metrics for pipeline run $run")

        Thread.sleep(1000) // Simulate time between runs
      }

      println("\n✓ All metrics exported successfully")
      println("\nPrometheus metrics available at: http://localhost:9090/metrics")
      println("Press Ctrl+C to stop...")

      // Keep server running
      Thread.sleep(30000)

    } finally {
      manager.close()
      println("\nMetrics exporters closed")
    }
  }

  def main(args: Array[String]): Unit = {
    try {
      runExample()
    } catch {
      case e: Exception =>
        println(s"Example failed: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}
