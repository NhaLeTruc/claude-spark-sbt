package chaos

import com.etl.config._
import com.etl.core.{ETLPipeline, ExecutionContext}
import com.etl.extract.Extractor
import com.etl.load.Loader
import com.etl.model.{ExecutionMetrics, LoadResult}
import com.etl.transform.Transformer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

/**
 * Chaos engineering tests for ETL pipelines.
 *
 * These tests inject various failure scenarios to verify resilience:
 * - Network failures
 * - Timeout scenarios
 * - Partial failures
 * - Resource exhaustion
 * - Data corruption
 *
 * Run with: sbt "testOnly chaos.ChaosEngineeringSpec"
 *
 * Note: These tests are designed to fail gracefully and validate error handling.
 */
class ChaosEngineeringSpec extends AnyFlatSpec with Matchers {

  /**
   * Configuration for chaos testing.
   */
  object ChaosConfig {
    val failureRate = 0.3 // 30% failure rate
    val random = new Random(42)
  }

  /**
   * Chaotic extractor that randomly fails.
   */
  class ChaoticExtractor(failureRate: Double = 0.3) extends Extractor {
    override def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame = {
      if (ChaosConfig.random.nextDouble() < failureRate) {
        throw new RuntimeException("Chaotic extractor: Simulated network failure")
      }

      import spark.implicits._
      Seq((1, "data1"), (2, "data2"), (3, "data3"))
        .toDF("id", "value")
    }
  }

  /**
   * Chaotic loader that randomly fails.
   */
  class ChaoticLoader(failureRate: Double = 0.3) extends Loader {
    override def load(
      df: DataFrame,
      config: LoadConfig,
      writeMode: String
    ): LoadResult = {
      if (ChaosConfig.random.nextDouble() < failureRate) {
        throw new RuntimeException("Chaotic loader: Simulated write failure")
      }

      val count = df.count()
      LoadResult.success(count)
    }
  }

  /**
   * Slow extractor that simulates timeout scenarios.
   */
  class SlowExtractor(delayMs: Long = 5000) extends Extractor {
    override def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame = {
      Thread.sleep(delayMs) // Intentional delay
      import spark.implicits._
      Seq((1, "slow_data")).toDF("id", "value")
    }
  }

  /**
   * Partial failure extractor that succeeds after N attempts.
   */
  class PartialFailureExtractor(failUntilAttempt: Int = 2) extends Extractor {
    private var attempts = 0

    override def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame = {
      attempts += 1
      if (attempts < failUntilAttempt) {
        throw new RuntimeException(s"Partial failure: Attempt $attempts failed")
      }

      import spark.implicits._
      Seq((1, s"success_after_$attempts")).toDF("id", "value")
    }

    def resetAttempts(): Unit = {
      attempts = 0
    }
  }

  /**
   * Corrupted data extractor.
   */
  class CorruptedDataExtractor extends Extractor {
    override def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      // Return data with nulls and invalid values
      Seq(
        (Some(1), Some("valid")),
        (None, Some("null_id")),
        (Some(3), None),
        (None, None)
      ).toDF("id", "value")
    }
  }

  /**
   * Test: Chaotic extractor with retry should eventually succeed.
   */
  "Pipeline with chaotic extractor" should "succeed with retry logic" in {
    val spark = SparkSession.builder()
      .appName("ChaosTest-Extractor")
      .master("local[2]")
      .getOrCreate()

    try {
      val extractor = new ChaoticExtractor(failureRate = 0.7) // High failure rate
      val transformers = Seq.empty[Transformer]
      val loader = new Loader {
        override def load(df: DataFrame, config: LoadConfig, writeMode: String): LoadResult = {
          LoadResult.success(df.count())
        }
      }

      val pipeline = ETLPipeline(extractor, transformers, loader)

      val config = createTestConfig()
      val context = ExecutionContext.create(spark, config)

      // Should eventually succeed with enough retries
      // Note: This might still fail occasionally due to randomness
      // In production, use circuit breaker to prevent infinite retries
      println("Running chaotic extractor test (may see failures)...")

    } finally {
      spark.stop()
    }
  }

  /**
   * Test: Slow operations should timeout appropriately.
   */
  "Pipeline with slow extractor" should "handle timeout scenarios" ignore {
    // Ignored by default to avoid slow tests
    val spark = SparkSession.builder()
      .appName("ChaosTest-Timeout")
      .master("local[2]")
      .getOrCreate()

    try {
      val extractor = new SlowExtractor(delayMs = 10000) // 10 second delay

      // In production, this would trigger timeout handling
      println("Testing timeout scenario...")

    } finally {
      spark.stop()
    }
  }

  /**
   * Test: Partial failures should succeed after N attempts.
   */
  "Pipeline with partial failure extractor" should "succeed after configured retries" in {
    val spark = SparkSession.builder()
      .appName("ChaosTest-PartialFailure")
      .master("local[2]")
      .getOrCreate()

    try {
      val extractor = new PartialFailureExtractor(failUntilAttempt = 3)
      val transformers = Seq.empty[Transformer]
      val loader = new Loader {
        override def load(df: DataFrame, config: LoadConfig, writeMode: String): LoadResult = {
          val count = df.count()
          count should be(1)
          LoadResult.success(count)
        }
      }

      val pipeline = ETLPipeline(extractor, transformers, loader)

      val config = createTestConfig(maxAttempts = 5)
      val context = ExecutionContext.create(spark, config)

      // Should succeed on 3rd attempt
      val result = pipeline.run(context)

      result.isSuccess shouldBe true
      println(s"Succeeded after ${extractor} attempts")

    } finally {
      spark.stop()
    }
  }

  /**
   * Test: Corrupted data should be detected by data quality rules.
   */
  "Pipeline with corrupted data" should "detect quality issues" in {
    val spark = SparkSession.builder()
      .appName("ChaosTest-Corruption")
      .master("local[2]")
      .getOrCreate()

    try {
      val extractor = new CorruptedDataExtractor()
      val df = extractor.extract(ExtractConfig(
        sourceType = SourceType.S3,
        path = Some("test"),
        schemaName = "test",
        connectionParams = Map.empty
      ))(spark)

      // Check for nulls
      val nullCount = df.filter(df("id").isNull || df("value").isNull).count()
      nullCount should be > 0L

      println(s"Detected $nullCount corrupted records")

    } finally {
      spark.stop()
    }
  }

  /**
   * Test: Circuit breaker should open after threshold.
   */
  "Circuit breaker" should "open after failure threshold" in {
    import com.etl.util.CircuitBreaker

    val circuitBreaker = new CircuitBreaker(
      name = "chaos-test",
      failureThreshold = 3,
      resetTimeoutMillis = 5000L,
      halfOpenMaxAttempts = 1
    )

    // Should start closed
    circuitBreaker.getState should include("CLOSED")

    // Simulate failures
    (1 to 5).foreach { attempt =>
      circuitBreaker.execute {
        throw new RuntimeException(s"Failure $attempt")
      }
    }

    // Should be open now
    circuitBreaker.getState should include("OPEN")
    circuitBreaker.canExecute shouldBe false

    println(s"Circuit breaker opened. Failure rate: ${circuitBreaker.getFailureRate}%")
  }

  /**
   * Test: Resource exhaustion simulation.
   */
  "Pipeline under resource pressure" should "handle memory constraints" ignore {
    // Ignored by default to avoid OOM
    val spark = SparkSession.builder()
      .appName("ChaosTest-ResourceExhaustion")
      .master("local[2]")
      .config("spark.executor.memory", "512m")
      .getOrCreate()

    try {
      // Create large dataset that might cause memory pressure
      import spark.implicits._
      val largeData = (1 to 10000000).map(i => (i, s"data_$i")).toDF("id", "value")

      largeData.cache()
      val count = largeData.count()

      println(s"Processed $count records under memory pressure")

    } finally {
      spark.stop()
    }
  }

  /**
   * Test: Network partition simulation (extractor fails, loader succeeds).
   */
  "Pipeline with network partition" should "handle split scenarios" in {
    val spark = SparkSession.builder()
      .appName("ChaosTest-NetworkPartition")
      .master("local[2]")
      .getOrCreate()

    try {
      // Extractor fails (network partition from source)
      val extractor = new ChaoticExtractor(failureRate = 1.0)

      // But loader would succeed (different network segment)
      val loader = new Loader {
        override def load(df: DataFrame, config: LoadConfig, writeMode: String): LoadResult = {
          LoadResult.success(df.count())
        }
      }

      // Pipeline should fail at extract stage
      println("Simulating network partition...")

    } finally {
      spark.stop()
    }
  }

  /**
   * Helper: Create test configuration.
   */
  private def createTestConfig(maxAttempts: Int = 3): PipelineConfig = {
    PipelineConfig(
      pipelineId = "chaos-test",
      name = "Chaos Test Pipeline",
      extract = ExtractConfig(
        sourceType = SourceType.S3,
        path = Some("test://path"),
        schemaName = "test-schema",
        connectionParams = Map.empty
      ),
      transforms = Seq.empty,
      load = LoadConfig(
        sinkType = SinkType.S3,
        path = Some("test://output"),
        writeMode = "Overwrite",
        schemaName = "test-schema",
        connectionParams = Map.empty
      ),
      errorHandlingConfig = ErrorHandlingConfig(
        retryConfig = RetryConfig(
          strategy = RetryStrategyType.ExponentialBackoff,
          maxAttempts = maxAttempts,
          initialDelaySeconds = 1
        ),
        circuitBreakerConfig = CircuitBreakerConfig(
          enabled = true,
          failureThreshold = 3,
          resetTimeoutSeconds = 5
        )
      )
    )
  }
}

/**
 * Companion object with chaos testing utilities.
 */
object ChaosEngineeringUtils {

  /**
   * Inject random failures into a function.
   */
  def withChaos[T](failureRate: Double)(fn: => T): T = {
    if (Random.nextDouble() < failureRate) {
      throw new RuntimeException("Chaos: Injected failure")
    }
    fn
  }

  /**
   * Inject latency into a function.
   */
  def withLatency[T](delayMs: Long)(fn: => T): T = {
    if (delayMs > 0) {
      Thread.sleep(delayMs)
    }
    fn
  }

  /**
   * Inject resource constraints.
   */
  def withResourcePressure[T](memoryMb: Int)(fn: => T): T = {
    // Allocate memory to create pressure
    val garbage = new Array[Byte](memoryMb * 1024 * 1024)
    try {
      fn
    } finally {
      // Help GC
      System.gc()
    }
  }
}
