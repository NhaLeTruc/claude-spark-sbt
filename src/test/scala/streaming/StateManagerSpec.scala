package streaming

import com.etl.streaming._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._
import java.sql.Timestamp

/**
 * Tests for streaming state management utilities.
 *
 * These tests demonstrate how to use StateManager for:
 * - Watermarking
 * - Windowing (tumbling, sliding, session)
 * - Late data handling
 * - Checkpoint management
 *
 * Run with: sbt "testOnly streaming.StateManagerSpec"
 */
class StateManagerSpec extends AnyFlatSpec with Matchers {

  /**
   * Test data for streaming scenarios.
   */
  case class Event(
    userId: String,
    eventType: String,
    timestamp: Timestamp,
    value: Double
  )

  /**
   * Test: Watermark configuration.
   */
  "StateManager" should "add watermark to streaming DataFrame" in {
    val spark = SparkSession.builder()
      .appName("StateManager-Watermark-Test")
      .master("local[2]")
      .config("spark.sql.streaming.checkpointLocation", "/tmp/test-checkpoint")
      .getOrCreate()

    try {
      import spark.implicits._

      val stateManager = StateManager()

      // Create test data
      val events = Seq(
        Event("user1", "click", new Timestamp(1000), 10.0),
        Event("user2", "view", new Timestamp(2000), 20.0),
        Event("user1", "click", new Timestamp(3000), 15.0)
      ).toDF()

      // Apply watermark
      val watermarkedDF = stateManager.withWatermark(
        events,
        timestampColumn = "timestamp",
        delayThreshold = 10.minutes
      )

      // Verify watermark column exists
      watermarkedDF.columns should contain("timestamp")

      println("✓ Watermark configured successfully")

    } finally {
      spark.stop()
    }
  }

  /**
   * Test: Tumbling window.
   */
  "StateManager" should "create tumbling windows correctly" in {
    val spark = SparkSession.builder()
      .appName("StateManager-Tumbling-Test")
      .master("local[2]")
      .getOrCreate()

    try {
      import spark.implicits._

      val stateManager = StateManager()

      val events = Seq(
        Event("user1", "click", new Timestamp(1000), 10.0),
        Event("user2", "view", new Timestamp(2000), 20.0),
        Event("user1", "click", new Timestamp(61000), 15.0)
      ).toDF()

      // Create tumbling window (1 minute)
      val windowedDF = stateManager.withTumblingWindow(
        events,
        timestampColumn = "timestamp",
        windowDuration = 1.minute
      )

      // Aggregate within windows
      val aggregated = windowedDF
        .agg(
          sum("value").as("total_value"),
          count("*").as("event_count")
        )

      val result = aggregated.collect()
      result.length should be(2) // Two different windows

      println(s"✓ Tumbling window created ${result.length} windows")

    } finally {
      spark.stop()
    }
  }

  /**
   * Test: Sliding window.
   */
  "StateManager" should "create sliding windows correctly" in {
    val spark = SparkSession.builder()
      .appName("StateManager-Sliding-Test")
      .master("local[2]")
      .getOrCreate()

    try {
      import spark.implicits._

      val stateManager = StateManager()

      val events = Seq(
        Event("user1", "click", new Timestamp(1000), 10.0),
        Event("user2", "view", new Timestamp(30000), 20.0),
        Event("user1", "click", new Timestamp(60000), 15.0)
      ).toDF()

      // Create sliding window (2 minutes, slide every 1 minute)
      val windowedDF = stateManager.withSlidingWindow(
        events,
        timestampColumn = "timestamp",
        windowDuration = 2.minutes,
        slideDuration = 1.minute
      )

      // Aggregate within windows
      val aggregated = windowedDF
        .agg(count("*").as("event_count"))

      val result = aggregated.collect()
      result.length should be > 0

      println(s"✓ Sliding window created ${result.length} overlapping windows")

    } finally {
      spark.stop()
    }
  }

  /**
   * Test: Checkpoint validation.
   */
  "CheckpointManager" should "validate checkpoint locations" in {
    // Valid locations
    CheckpointManager.validateCheckpointLocation("s3://bucket/path") shouldBe Right(())
    CheckpointManager.validateCheckpointLocation("hdfs://namenode/path") shouldBe Right(())
    CheckpointManager.validateCheckpointLocation("/tmp/checkpoint") shouldBe Right(())
    CheckpointManager.validateCheckpointLocation("file:///tmp/checkpoint") shouldBe Right(())

    // Invalid locations
    CheckpointManager.validateCheckpointLocation("") should matchPattern {
      case Left(_) =>
    }
    CheckpointManager.validateCheckpointLocation(null) should matchPattern {
      case Left(_) =>
    }
    CheckpointManager.validateCheckpointLocation("invalid://path") should matchPattern {
      case Left(_) =>
    }

    println("✓ Checkpoint location validation working correctly")
  }

  /**
   * Test: Checkpoint path generation.
   */
  "CheckpointManager" should "generate valid checkpoint paths" in {
    val path = CheckpointManager.generateCheckpointPath(
      baseLocation = "s3://my-bucket/checkpoints",
      pipelineId = "my-pipeline-123",
      componentType = "state-store"
    )

    path should include("s3://my-bucket/checkpoints")
    path should include("my-pipeline-123")
    path should include("state-store")

    println(s"✓ Generated checkpoint path: $path")
  }

  /**
   * Test: Watermark delay calculation.
   */
  "WatermarkHelper" should "calculate appropriate watermark delays" in {
    // 10% expected lateness, 1 minute avg processing delay
    val delay1 = WatermarkHelper.calculateWatermarkDelay(
      expectedLatenessPct = 0.1,
      avgProcessingDelay = 1.minute
    )
    delay1.toSeconds should be > 60L // Should be > 1 minute
    delay1.toSeconds should be < 150L // But not too large

    // 50% expected lateness (high lateness)
    val delay2 = WatermarkHelper.calculateWatermarkDelay(
      expectedLatenessPct = 0.5,
      avgProcessingDelay = 1.minute
    )
    delay2 should be > delay1 // Higher lateness = larger delay

    println(s"✓ Watermark delay calculation: 10% lateness = $delay1, 50% lateness = $delay2")
  }

  /**
   * Test: Watermark validation.
   */
  "WatermarkHelper" should "validate watermark configuration" in {
    // Valid watermark
    WatermarkHelper.validateWatermark(
      watermarkDelay = 1.minute,
      windowDuration = Some(5.minutes)
    ) shouldBe Right(())

    // Invalid: zero delay
    WatermarkHelper.validateWatermark(
      watermarkDelay = 0.seconds
    ) should matchPattern {
      case Left(_) =>
    }

    // Invalid: watermark >= window
    WatermarkHelper.validateWatermark(
      watermarkDelay = 10.minutes,
      windowDuration = Some(5.minutes)
    ) should matchPattern {
      case Left(_) =>
    }

    println("✓ Watermark validation working correctly")
  }

  /**
   * Test: Recommended watermark delays.
   */
  "WatermarkHelper" should "provide reasonable recommended delays" in {
    val iotDelay = WatermarkHelper.getRecommendedDelay("iot")
    val financialDelay = WatermarkHelper.getRecommendedDelay("financial")
    val mobileDelay = WatermarkHelper.getRecommendedDelay("mobile")

    // Financial should be stricter (lower delay) than mobile
    financialDelay should be < mobileDelay

    // All should be positive
    iotDelay.toSeconds should be > 0L
    financialDelay.toSeconds should be > 0L
    mobileDelay.toSeconds should be > 0L

    println(s"✓ Recommended delays: IoT=$iotDelay, Financial=$financialDelay, Mobile=$mobileDelay")
  }

  /**
   * Test: State store memory estimation.
   */
  "StateStoreHelper" should "estimate memory requirements" in {
    val memoryMB = StateStoreHelper.estimateMemoryRequirement(
      avgStateSize = 1024, // 1 KB per key
      expectedKeys = 1000000, // 1M keys
      overheadFactor = 1.5
    )

    // Should be approximately 1024 * 1000000 * 1.5 / (1024*1024) = ~1465 MB
    memoryMB should be > 1000L
    memoryMB should be < 2000L

    println(s"✓ Estimated state store memory: ${memoryMB}MB")
  }

  /**
   * Test: State store config validation.
   */
  "StateStoreHelper" should "validate state store configuration" in {
    // Valid config
    val validConfig = StateStoreHelper.StateStoreConfig(
      stateTimeout = 1.hour,
      maxRecordsPerKey = Some(1000)
    )
    StateStoreHelper.validateStateStoreConfig(validConfig) shouldBe Right(())

    // Invalid: zero timeout
    val invalidConfig1 = StateStoreHelper.StateStoreConfig(
      stateTimeout = 0.seconds
    )
    StateStoreHelper.validateStateStoreConfig(invalidConfig1) should matchPattern {
      case Left(_) =>
    }

    // Invalid: zero max records
    val invalidConfig2 = StateStoreHelper.StateStoreConfig(
      stateTimeout = 1.hour,
      maxRecordsPerKey = Some(0)
    )
    StateStoreHelper.validateStateStoreConfig(invalidConfig2) should matchPattern {
      case Left(_) =>
    }

    println("✓ State store config validation working correctly")
  }

  /**
   * Test: Recommended timeouts.
   */
  "StateStoreHelper" should "provide reasonable recommended timeouts" in {
    val sessionTimeout = StateStoreHelper.getRecommendedTimeout("user_session")
    val cartTimeout = StateStoreHelper.getRecommendedTimeout("shopping_cart")
    val metricsTimeout = StateStoreHelper.getRecommendedTimeout("metrics_aggregation")

    // Shopping cart should have longer timeout than user session
    cartTimeout should be > sessionTimeout

    // Metrics should have short timeout
    metricsTimeout.toMinutes should be < sessionTimeout.toMinutes

    println(s"✓ Recommended timeouts: Session=$sessionTimeout, Cart=$cartTimeout, Metrics=$metricsTimeout")
  }

  /**
   * Test: Session state data structure.
   */
  "StateManager" should "provide session state structure" in {
    val state = StateManager.SessionState(
      sessionStart = 1000L,
      sessionEnd = 2000L,
      eventCount = 10,
      lastUpdate = 2000L
    )

    state.sessionStart shouldBe 1000L
    state.sessionEnd shouldBe 2000L
    state.eventCount shouldBe 10
    state.lastUpdate shouldBe 2000L

    // Calculate session duration
    val durationMs = state.sessionEnd - state.sessionStart
    durationMs shouldBe 1000L

    println(s"✓ Session state: duration=${durationMs}ms, events=${state.eventCount}")
  }

  /**
   * Test: Aggregation state data structure.
   */
  "StateManager" should "provide aggregation state structure" in {
    val state = StateManager.AggregationState(
      count = 100,
      sum = 1500.0,
      min = 5.0,
      max = 50.0,
      lastUpdate = System.currentTimeMillis()
    )

    state.count shouldBe 100
    state.sum shouldBe 1500.0
    state.min shouldBe 5.0
    state.max shouldBe 50.0

    // Calculate average
    val avg = state.sum / state.count
    avg shouldBe 15.0

    println(s"✓ Aggregation state: count=${state.count}, avg=$avg, min=${state.min}, max=${state.max}")
  }
}

/**
 * Example: End-to-end streaming pipeline with state management.
 *
 * This example is NOT a test - it's a demonstration of how to use
 * state management in a real streaming pipeline.
 *
 * To run manually:
 *   1. Start Kafka: make docker-up
 *   2. Run this example (requires manual execution)
 */
object StateManagementExample {

  case class UserEvent(
    userId: String,
    eventType: String,
    timestamp: Timestamp,
    value: Double
  )

  def runExample(): Unit = {
    val spark = SparkSession.builder()
      .appName("State-Management-Example")
      .master("local[4]")
      .config("spark.sql.streaming.checkpointLocation", "/tmp/state-example-checkpoint")
      .getOrCreate()

    import spark.implicits._

    val stateManager = StateManager()

    // Example 1: Windowed aggregation with watermark
    println("\n=== Example 1: Windowed Aggregation ===")

    val streamDF = spark.readStream
      .format("rate") // Generate test data
      .option("rowsPerSecond", "10")
      .load()
      .select(
        (col("value") % 10).cast("string").as("userId"),
        lit("click").as("eventType"),
        col("timestamp"),
        (col("value") % 100).cast("double").as("value")
      )

    // Add watermark for late data handling
    val watermarkedDF = stateManager.withWatermark(
      streamDF,
      timestampColumn = "timestamp",
      delayThreshold = 10.seconds
    )

    // Create 1-minute tumbling windows
    val windowedAggregation = stateManager
      .withTumblingWindow(
        watermarkedDF,
        timestampColumn = "timestamp",
        windowDuration = 1.minute
      )
      .agg(
        count("*").as("event_count"),
        sum("value").as("total_value"),
        avg("value").as("avg_value")
      )

    // Write to console (for demonstration)
    val query1 = windowedAggregation.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .start()

    println("Streaming query started. Press Ctrl+C to stop.")
    println("Checkpoint location: /tmp/state-example-checkpoint")

    // Run for a short time (in production, this would run indefinitely)
    query1.awaitTermination(30000) // 30 seconds
    query1.stop()

    spark.stop()
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
