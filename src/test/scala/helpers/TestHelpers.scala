package helpers

import com.etl.config._
import com.etl.model.{ExecutionMetrics, LoadResult, WriteMode}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import java.util.UUID

/**
 * Test helper utilities for unit testing.
 * Provides factory methods for creating test data, mock configs, and common assertions.
 */
object TestHelpers {

  // ============================================================================
  // DataFrame Helpers
  // ============================================================================

  /**
   * Create a simple test DataFrame with user data.
   *
   * @param spark SparkSession
   * @param numRows Number of rows to generate
   * @return DataFrame with columns: user_id, username, email
   */
  def createUserDataFrame(spark: SparkSession, numRows: Int = 5): DataFrame = {
    val schema = StructType(Seq(
      StructField("user_id", StringType, nullable = false),
      StructField("username", StringType, nullable = false),
      StructField("email", StringType, nullable = false)
    ))

    val data = (1 to numRows).map { i =>
      Row(s"user-$i", s"user$i", s"user$i@example.com")
    }

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  /**
   * Create a test DataFrame with event data.
   *
   * @param spark SparkSession
   * @param numRows Number of rows to generate
   * @return DataFrame with columns: event_id, user_id, event_type, timestamp, amount
   */
  def createEventDataFrame(spark: SparkSession, numRows: Int = 10): DataFrame = {
    val schema = StructType(Seq(
      StructField("event_id", StringType, nullable = false),
      StructField("user_id", StringType, nullable = false),
      StructField("event_type", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = false),
      StructField("amount", DoubleType, nullable = true)
    ))

    val eventTypes = Seq("PAGE_VIEW", "BUTTON_CLICK", "FORM_SUBMIT", "API_CALL")
    val data = (1 to numRows).map { i =>
      val userId = s"user-${(i % 5) + 1}"
      val eventType = eventTypes(i % eventTypes.length)
      val timestamp = 1704067200000L + (i * 60000)
      val amount = if (i % 3 == 0) Some(99.99 * i) else None
      Row(s"event-$i", userId, eventType, timestamp, amount.orNull)
    }

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  /**
   * Create a test DataFrame with transaction data.
   *
   * @param spark SparkSession
   * @param numRows Number of rows to generate
   * @return DataFrame with columns: transaction_id, user_id, amount, transaction_type, timestamp
   */
  def createTransactionDataFrame(spark: SparkSession, numRows: Int = 10): DataFrame = {
    val schema = StructType(Seq(
      StructField("transaction_id", StringType, nullable = false),
      StructField("user_id", StringType, nullable = false),
      StructField("amount", DoubleType, nullable = false),
      StructField("transaction_type", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = false)
    ))

    val txnTypes = Seq("PURCHASE", "REFUND", "TRANSFER")
    val data = (1 to numRows).map { i =>
      val userId = s"user-${(i % 5) + 1}"
      val txnType = txnTypes(i % txnTypes.length)
      val amount = 50.0 + (i * 10.0)
      val timestamp = 1704067200000L + (i * 3600000)
      Row(s"txn-$i", userId, amount, txnType, timestamp)
    }

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  /**
   * Create an empty DataFrame with specified schema.
   *
   * @param spark SparkSession
   * @param schema StructType schema
   * @return Empty DataFrame
   */
  def createEmptyDataFrame(spark: SparkSession, schema: StructType): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  // ============================================================================
  // Configuration Helpers
  // ============================================================================

  /**
   * Create a minimal valid PipelineConfig for testing.
   *
   * @param pipelineId Optional pipeline ID
   * @return PipelineConfig
   */
  def createTestPipelineConfig(pipelineId: String = "test-pipeline"): PipelineConfig = {
    PipelineConfig(
      pipelineId = pipelineId,
      description = Some("Test pipeline configuration"),
      extract = createTestExtractConfig(),
      transforms = Seq.empty,
      load = createTestLoadConfig(),
      performance = PerformanceConfig(
        batchSize = 1000,
        parallelism = 2,
        memoryPerExecutor = "1g"
      ),
      retry = RetryConfig(
        maxAttempts = 3,
        delaySeconds = 5,
        backoffMultiplier = 2.0
      ),
      logging = LoggingConfig(
        level = "INFO",
        format = "json"
      )
    )
  }

  /**
   * Create a test ExtractConfig.
   *
   * @param sourceType Source type (default: Kafka)
   * @return ExtractConfig
   */
  def createTestExtractConfig(sourceType: SourceType = SourceType.Kafka): ExtractConfig = {
    ExtractConfig(
      sourceType = sourceType,
      connectionParams = Map(
        "kafka.bootstrap.servers" -> "localhost:9092",
        "subscribe" -> "test-topic"
      ),
      table = None,
      query = None,
      format = Some("avro"),
      schema = Some("test-schema")
    )
  }

  /**
   * Create a test TransformConfig for aggregation.
   *
   * @return TransformConfig
   */
  def createTestAggregationConfig(): TransformConfig = {
    TransformConfig(
      transformType = TransformType.Aggregation,
      parameters = Map(
        "groupBy" -> """["user_id"]""",
        "aggregations" -> """{"amount": "sum,avg", "event_id": "count"}"""
      )
    )
  }

  /**
   * Create a test TransformConfig for window function.
   *
   * @return TransformConfig
   */
  def createTestWindowConfig(): TransformConfig = {
    TransformConfig(
      transformType = TransformType.Window,
      parameters = Map(
        "partitionBy" -> """["user_id"]""",
        "orderBy" -> """["timestamp"]""",
        "windowFunction" -> "row_number",
        "outputColumn" -> "row_num"
      )
    )
  }

  /**
   * Create a test LoadConfig.
   *
   * @param sinkType Sink type (default: Kafka)
   * @param writeMode Write mode (default: Append)
   * @return LoadConfig
   */
  def createTestLoadConfig(
    sinkType: SinkType = SinkType.Kafka,
    writeMode: WriteMode = WriteMode.Append
  ): LoadConfig = {
    LoadConfig(
      sinkType = sinkType,
      writeMode = writeMode,
      connectionParams = Map(
        "kafka.bootstrap.servers" -> "localhost:9092",
        "topic" -> "test-output-topic"
      ),
      table = None,
      format = Some("avro"),
      partitionBy = Seq.empty,
      compressionCodec = None,
      upsertKeys = None,
      schema = Some("test-schema")
    )
  }

  // ============================================================================
  // Model Helpers
  // ============================================================================

  /**
   * Create a test ExecutionMetrics.
   *
   * @param pipelineId Pipeline ID
   * @param executionId Execution ID
   * @return ExecutionMetrics
   */
  def createTestMetrics(
    pipelineId: String = "test-pipeline",
    executionId: String = UUID.randomUUID().toString
  ): ExecutionMetrics = {
    ExecutionMetrics(
      pipelineId = pipelineId,
      executionId = executionId,
      startTime = System.currentTimeMillis(),
      endTime = None,
      recordsExtracted = 100L,
      recordsTransformed = 100L,
      recordsLoaded = 100L,
      recordsFailed = 0L,
      retryCount = 0,
      errors = Seq.empty
    )
  }

  /**
   * Create a successful LoadResult.
   *
   * @param recordCount Number of records loaded
   * @return LoadResult
   */
  def createSuccessLoadResult(recordCount: Long = 100L): LoadResult = {
    LoadResult(
      recordsLoaded = recordCount,
      recordsFailed = 0L,
      errors = Seq.empty
    )
  }

  /**
   * Create a failed LoadResult.
   *
   * @param recordCount Number of records loaded
   * @param failedCount Number of records failed
   * @param error Error message
   * @return LoadResult
   */
  def createFailedLoadResult(
    recordCount: Long = 50L,
    failedCount: Long = 50L,
    error: String = "Test error"
  ): LoadResult = {
    LoadResult(
      recordsLoaded = recordCount,
      recordsFailed = failedCount,
      errors = Seq(error)
    )
  }

  // ============================================================================
  // Assertion Helpers
  // ============================================================================

  /**
   * Assert that two DataFrames have the same schema.
   *
   * @param df1 First DataFrame
   * @param df2 Second DataFrame
   * @return true if schemas match
   */
  def assertSameSchema(df1: DataFrame, df2: DataFrame): Boolean = {
    df1.schema == df2.schema
  }

  /**
   * Assert that DataFrame has expected columns.
   *
   * @param df DataFrame to check
   * @param expectedColumns Expected column names
   * @return true if all columns present
   */
  def assertHasColumns(df: DataFrame, expectedColumns: Seq[String]): Boolean = {
    val actualColumns = df.schema.fieldNames.toSet
    expectedColumns.forall(actualColumns.contains)
  }

  /**
   * Assert that DataFrame has expected row count.
   *
   * @param df DataFrame to check
   * @param expectedCount Expected row count
   * @return true if count matches
   */
  def assertRowCount(df: DataFrame, expectedCount: Long): Boolean = {
    df.count() == expectedCount
  }

  /**
   * Get DataFrame as list of maps for easy assertions.
   *
   * @param df DataFrame
   * @return List of maps (column name -> value)
   */
  def dataFrameToMaps(df: DataFrame): List[Map[String, Any]] = {
    val columns = df.schema.fieldNames
    df.collect().map { row =>
      columns.zipWithIndex.map { case (colName, idx) =>
        colName -> row.get(idx)
      }.toMap
    }.toList
  }

  /**
   * Count rows matching a condition.
   *
   * @param df DataFrame
   * @param condition SQL condition string
   * @return Number of matching rows
   */
  def countWhere(df: DataFrame, condition: String): Long = {
    df.filter(condition).count()
  }

  // ============================================================================
  // Data Generation Helpers
  // ============================================================================

  /**
   * Generate random user IDs.
   *
   * @param count Number of IDs to generate
   * @return Sequence of user IDs
   */
  def generateUserIds(count: Int): Seq[String] = {
    (1 to count).map(i => s"user-${UUID.randomUUID().toString.take(8)}")
  }

  /**
   * Generate timestamp sequence.
   *
   * @param start Start timestamp (epoch millis)
   * @param count Number of timestamps
   * @param intervalMillis Interval between timestamps
   * @return Sequence of timestamps
   */
  def generateTimestamps(
    start: Long = 1704067200000L,
    count: Int = 10,
    intervalMillis: Long = 60000L
  ): Seq[Long] = {
    (0 until count).map(i => start + (i * intervalMillis))
  }
}
