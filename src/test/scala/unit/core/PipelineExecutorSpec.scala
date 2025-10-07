package com.etl.unit.core

import com.etl.config._
import com.etl.core.{ExecutionContext, Pipeline, PipelineExecutor}
import com.etl.model.{ExecutionMetrics, PipelineFailure, PipelineResult, PipelineSuccess}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

class PipelineExecutorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("PipelineExecutorTest")
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

  // Mock pipeline that always succeeds
  class SuccessfulPipeline extends Pipeline {
    override def run(context: ExecutionContext): PipelineResult = {
      val metrics = context.metrics.copy(
        recordsExtracted = 100,
        recordsTransformed = 100,
        recordsLoaded = 100
      )
      PipelineSuccess(metrics.complete())
    }
  }

  // Mock pipeline that fails on first N attempts, then succeeds
  class RetryablePipeline(failAttempts: Int) extends Pipeline {
    private var attemptCount = 0

    override def run(context: ExecutionContext): PipelineResult = {
      attemptCount += 1
      if (attemptCount <= failAttempts) {
        throw new RuntimeException(s"Simulated failure on attempt $attemptCount")
      } else {
        val metrics = context.metrics.copy(
          recordsExtracted = 50,
          recordsTransformed = 50,
          recordsLoaded = 50,
          retryCount = attemptCount - 1
        )
        PipelineSuccess(metrics.complete())
      }
    }
  }

  // Mock pipeline that always fails
  class FailingPipeline extends Pipeline {
    override def run(context: ExecutionContext): PipelineResult = {
      throw new RuntimeException("Permanent pipeline failure")
    }
  }

  "PipelineExecutor" should "execute successful pipeline and return success result" in {
    // Given a successful pipeline
    val pipeline = new SuccessfulPipeline()
    val config = createTestConfig()

    // When executing
    val executor = new PipelineExecutor()
    val result = executor.execute(pipeline, config)(spark)

    // Then should return success
    result.isSuccess shouldBe true
    result.metrics.recordsExtracted shouldBe 100
    result.metrics.recordsTransformed shouldBe 100
    result.metrics.recordsLoaded shouldBe 100
  }

  it should "retry failed pipeline according to retry config" in {
    // Given a pipeline that fails once then succeeds
    val pipeline = new RetryablePipeline(failAttempts = 1)
    val config = createTestConfig(retryConfig = RetryConfig(maxAttempts = 3, delaySeconds = 1))

    // When executing
    val executor = new PipelineExecutor()
    val startTime = System.currentTimeMillis()
    val result = executor.execute(pipeline, config)(spark)
    val endTime = System.currentTimeMillis()

    // Then should succeed after retry
    result.isSuccess shouldBe true
    result.metrics.retryCount shouldBe 1

    // Should have delayed at least 1 second for retry
    val elapsedSeconds = (endTime - startTime) / 1000.0
    elapsedSeconds should be >= 1.0
  }

  it should "exhaust retries and return failure for permanently failing pipeline" in {
    // Given a pipeline that always fails
    val pipeline = new FailingPipeline()
    val config = createTestConfig(retryConfig = RetryConfig(maxAttempts = 2, delaySeconds = 1))

    // When executing
    val executor = new PipelineExecutor()
    val result = executor.execute(pipeline, config)(spark)

    // Then should return failure after exhausting retries
    result.isSuccess shouldBe false
    result shouldBe a[PipelineFailure]

    val failure = result.asInstanceOf[PipelineFailure]
    failure.error.getMessage should include("Permanent pipeline failure")
  }

  it should "track execution metrics throughout pipeline execution" in {
    // Given a successful pipeline
    val pipeline = new SuccessfulPipeline()
    val config = createTestConfig()

    // When executing
    val executor = new PipelineExecutor()
    val result = executor.execute(pipeline, config)(spark)

    // Then should have valid execution metrics
    result.metrics.pipelineId shouldBe "test-pipeline"
    result.metrics.executionId should not be empty
    result.metrics.startTime should not be null
    result.metrics.endTime should not be null
    result.metrics.duration should be > 0L
  }

  it should "populate MDC context during execution" in {
    // Given a successful pipeline
    val pipeline = new SuccessfulPipeline()
    val config = createTestConfig()

    // When executing
    val executor = new PipelineExecutor()
    val result = executor.execute(pipeline, config)(spark)

    // Then execution should complete with context
    result.isSuccess shouldBe true
    result.metrics.pipelineId shouldBe "test-pipeline"
  }

  it should "handle multiple retry attempts with delays" in {
    // Given a pipeline that fails twice then succeeds
    val pipeline = new RetryablePipeline(failAttempts = 2)
    val config = createTestConfig(retryConfig = RetryConfig(maxAttempts = 3, delaySeconds = 1))

    // When executing
    val executor = new PipelineExecutor()
    val startTime = System.currentTimeMillis()
    val result = executor.execute(pipeline, config)(spark)
    val endTime = System.currentTimeMillis()

    // Then should succeed after 2 retries
    result.isSuccess shouldBe true
    result.metrics.retryCount shouldBe 2

    // Should have delayed at least 2 seconds (1s per retry)
    val elapsedSeconds = (endTime - startTime) / 1000.0
    elapsedSeconds should be >= 2.0
  }

  it should "not retry on first successful execution" in {
    // Given a successful pipeline
    val pipeline = new SuccessfulPipeline()
    val config = createTestConfig(retryConfig = RetryConfig(maxAttempts = 3, delaySeconds = 5))

    // When executing
    val executor = new PipelineExecutor()
    val startTime = System.currentTimeMillis()
    val result = executor.execute(pipeline, config)(spark)
    val endTime = System.currentTimeMillis()

    // Then should succeed immediately without retry delay
    result.isSuccess shouldBe true
    result.metrics.retryCount shouldBe 0

    // Should complete in less than retry delay
    val elapsedSeconds = (endTime - startTime) / 1000.0
    elapsedSeconds should be < 2.0
  }

  it should "include error details in failure result" in {
    // Given a failing pipeline
    val pipeline = new FailingPipeline()
    val config = createTestConfig(retryConfig = RetryConfig(maxAttempts = 1, delaySeconds = 1))

    // When executing
    val executor = new PipelineExecutor()
    val result = executor.execute(pipeline, config)(spark)

    // Then should include error details
    result shouldBe a[PipelineFailure]
    val failure = result.asInstanceOf[PipelineFailure]
    failure.error shouldBe a[RuntimeException]
    failure.error.getMessage shouldBe "Permanent pipeline failure"
  }

  private def createTestConfig(retryConfig: RetryConfig = RetryConfig(maxAttempts = 3, delaySeconds = 5)): PipelineConfig = {
    PipelineConfig(
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
      retry = retryConfig,
      performance = PerformanceConfig(batchSize = 10000, parallelism = 4),
      logging = LoggingConfig(level = "INFO", structuredLogging = true)
    )
  }
}
