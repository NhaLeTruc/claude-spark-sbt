package com.etl.unit.core

import com.etl.config._
import com.etl.core.ExecutionContext
import com.etl.model.ExecutionMetrics
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class ExecutionContextSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("ExecutionContextTest")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  "ExecutionContext" should "initialize with valid configuration" in {
    // Given a pipeline config
    val config = PipelineConfig(
      pipelineId = "test-pipeline",
      extract = ExtractConfig(
        sourceType = SourceType.Kafka,
        topic = Some("test-topic"),
        schemaName = "user-event",
        connectionParams = Map("kafka.bootstrap.servers" -> "localhost:9092")
      ),
      transforms = Seq.empty,
      load = LoadConfig(
        sinkType = SinkType.S3,
        path = Some("s3a://bucket/output/"),
        connectionParams = Map("format" -> "parquet")
      ),
      retry = RetryConfig(maxAttempts = 3, delaySeconds = 5),
      performance = PerformanceConfig(batchSize = 10000, parallelism = 4),
      logging = LoggingConfig(level = "INFO", structuredLogging = true)
    )

    // When creating execution context
    val context = ExecutionContext.create(spark, config)

    // Then should have valid fields
    context.spark shouldBe spark
    context.config shouldBe config
    context.metrics.pipelineId shouldBe "test-pipeline"
    context.metrics.executionId should not be empty
    context.traceId should not be empty
  }

  it should "generate unique trace IDs for each execution" in {
    val config = PipelineConfig(
      pipelineId = "test-pipeline",
      extract = ExtractConfig(
        sourceType = SourceType.Kafka,
        topic = Some("test-topic"),
        schemaName = "user-event",
        connectionParams = Map("kafka.bootstrap.servers" -> "localhost:9092")
      ),
      transforms = Seq.empty,
      load = LoadConfig(
        sinkType = SinkType.S3,
        path = Some("s3a://bucket/output/"),
        connectionParams = Map("format" -> "parquet")
      ),
      retry = RetryConfig(maxAttempts = 3, delaySeconds = 5),
      performance = PerformanceConfig(batchSize = 10000, parallelism = 4),
      logging = LoggingConfig(level = "INFO", structuredLogging = true)
    )

    // When creating multiple execution contexts
    val context1 = ExecutionContext.create(spark, config)
    val context2 = ExecutionContext.create(spark, config)

    // Then should have different trace IDs
    context1.traceId should not equal context2.traceId
    context1.metrics.executionId should not equal context2.metrics.executionId
  }

  it should "provide MDC context for logging" in {
    val config = PipelineConfig(
      pipelineId = "logging-test-pipeline",
      extract = ExtractConfig(
        sourceType = SourceType.Kafka,
        topic = Some("test-topic"),
        schemaName = "user-event",
        connectionParams = Map("kafka.bootstrap.servers" -> "localhost:9092")
      ),
      transforms = Seq.empty,
      load = LoadConfig(
        sinkType = SinkType.S3,
        path = Some("s3a://bucket/output/"),
        connectionParams = Map("format" -> "parquet")
      ),
      retry = RetryConfig(maxAttempts = 3, delaySeconds = 5),
      performance = PerformanceConfig(batchSize = 10000, parallelism = 4),
      logging = LoggingConfig(level = "INFO", structuredLogging = true)
    )

    // When creating execution context
    val context = ExecutionContext.create(spark, config)

    // Then should provide MDC context map
    val mdcContext = context.getMDCContext

    mdcContext should contain key "pipelineId"
    mdcContext should contain key "traceId"
    mdcContext should contain key "executionId"

    mdcContext("pipelineId") shouldBe "logging-test-pipeline"
    mdcContext("traceId") shouldBe context.traceId
    mdcContext("executionId") shouldBe context.metrics.executionId
  }

  it should "allow metrics updates during execution" in {
    val config = PipelineConfig(
      pipelineId = "metrics-test-pipeline",
      extract = ExtractConfig(
        sourceType = SourceType.Kafka,
        topic = Some("test-topic"),
        schemaName = "user-event",
        connectionParams = Map("kafka.bootstrap.servers" -> "localhost:9092")
      ),
      transforms = Seq.empty,
      load = LoadConfig(
        sinkType = SinkType.S3,
        path = Some("s3a://bucket/output/"),
        connectionParams = Map("format" -> "parquet")
      ),
      retry = RetryConfig(maxAttempts = 3, delaySeconds = 5),
      performance = PerformanceConfig(batchSize = 10000, parallelism = 4),
      logging = LoggingConfig(level = "INFO", structuredLogging = true)
    )

    // When creating execution context
    val context = ExecutionContext.create(spark, config)

    // And updating metrics
    val initialMetrics = context.metrics
    val updatedMetrics = initialMetrics.copy(
      recordsExtracted = 1000,
      recordsTransformed = 950,
      recordsLoaded = 950
    )

    context.updateMetrics(updatedMetrics)

    // Then metrics should be updated
    context.metrics.recordsExtracted shouldBe 1000
    context.metrics.recordsTransformed shouldBe 950
    context.metrics.recordsLoaded shouldBe 950
  }

  it should "preserve trace ID across metric updates" in {
    val config = PipelineConfig(
      pipelineId = "trace-test-pipeline",
      extract = ExtractConfig(
        sourceType = SourceType.Kafka,
        topic = Some("test-topic"),
        schemaName = "user-event",
        connectionParams = Map("kafka.bootstrap.servers" -> "localhost:9092")
      ),
      transforms = Seq.empty,
      load = LoadConfig(
        sinkType = SinkType.S3,
        path = Some("s3a://bucket/output/"),
        connectionParams = Map("format" -> "parquet")
      ),
      retry = RetryConfig(maxAttempts = 3, delaySeconds = 5),
      performance = PerformanceConfig(batchSize = 10000, parallelism = 4),
      logging = LoggingConfig(level = "INFO", structuredLogging = true)
    )

    // When creating execution context
    val context = ExecutionContext.create(spark, config)
    val originalTraceId = context.traceId

    // And updating metrics multiple times
    val updatedMetrics1 = context.metrics.copy(recordsExtracted = 100)
    context.updateMetrics(updatedMetrics1)

    val updatedMetrics2 = context.metrics.copy(recordsTransformed = 100)
    context.updateMetrics(updatedMetrics2)

    // Then trace ID should remain constant
    context.traceId shouldBe originalTraceId
  }

  it should "support custom trace ID" in {
    val customTraceId = "custom-trace-" + UUID.randomUUID().toString

    val config = PipelineConfig(
      pipelineId = "custom-trace-pipeline",
      extract = ExtractConfig(
        sourceType = SourceType.Kafka,
        topic = Some("test-topic"),
        schemaName = "user-event",
        connectionParams = Map("kafka.bootstrap.servers" -> "localhost:9092")
      ),
      transforms = Seq.empty,
      load = LoadConfig(
        sinkType = SinkType.S3,
        path = Some("s3a://bucket/output/"),
        connectionParams = Map("format" -> "parquet")
      ),
      retry = RetryConfig(maxAttempts = 3, delaySeconds = 5),
      performance = PerformanceConfig(batchSize = 10000, parallelism = 4),
      logging = LoggingConfig(level = "INFO", structuredLogging = true)
    )

    val metrics = ExecutionMetrics.initial("custom-trace-pipeline", UUID.randomUUID().toString)

    // When creating execution context with custom trace ID
    val context = ExecutionContext(
      spark = spark,
      config = config,
      metrics = metrics,
      traceId = customTraceId
    )

    // Then should use custom trace ID
    context.traceId shouldBe customTraceId
  }
}
