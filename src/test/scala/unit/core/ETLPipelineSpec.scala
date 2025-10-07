package com.etl.unit.core

import com.etl.config._
import com.etl.core.{ETLPipeline, ExecutionContext}
import com.etl.extract.Extractor
import com.etl.load.Loader
import com.etl.model.{LoadResult, PipelineSuccess, WriteMode}
import com.etl.transform.Transformer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ETLPipelineSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("ETLPipelineTest")
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

  // Mock extractor that returns test data
  class MockExtractor extends Extractor {
    override def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      Seq(
        (1, "user1", "login"),
        (2, "user1", "click"),
        (3, "user2", "login"),
        (4, "user2", "click"),
        (5, "user2", "logout")
      ).toDF("event_id", "user_id", "event_type")
    }
  }

  // Mock transformer that filters data
  class FilterTransformer extends Transformer {
    override def transform(df: DataFrame, config: TransformConfig): DataFrame = {
      // Filter to only login events
      df.filter("event_type = 'login'")
    }
  }

  // Mock transformer that adds a column
  class AddColumnTransformer extends Transformer {
    override def transform(df: DataFrame, config: TransformConfig): DataFrame = {
      import org.apache.spark.sql.functions._
      df.withColumn("processed", lit(true))
    }
  }

  // Mock loader that counts records
  class MockLoader extends Loader {
    var recordsLoaded: Long = 0

    override def load(df: DataFrame, config: LoadConfig, mode: WriteMode): LoadResult = {
      recordsLoaded = df.count()
      LoadResult.success(recordsLoaded)
    }
  }

  // Mock loader that fails
  class FailingLoader extends Loader {
    override def load(df: DataFrame, config: LoadConfig, mode: WriteMode): LoadResult = {
      throw new RuntimeException("Mock loader failure")
    }
  }

  "ETLPipeline" should "execute extract → transform → load stages" in {
    // Given a pipeline with extract, transform, load
    val extractor = new MockExtractor()
    val transformer = new FilterTransformer()
    val loader = new MockLoader()

    val pipeline = ETLPipeline(
      extractor = extractor,
      transformers = Seq(transformer),
      loader = loader
    )

    val config = createTestConfig()
    val context = ExecutionContext.create(spark, config)

    // When running pipeline
    val result = pipeline.run(context)

    // Then should execute all stages successfully
    result.isSuccess shouldBe true
    result shouldBe a[PipelineSuccess]

    // Should have loaded filtered data (2 login events)
    loader.recordsLoaded shouldBe 2

    // Should have valid metrics
    val metrics = result.metrics
    metrics.recordsExtracted shouldBe 5
    metrics.recordsTransformed shouldBe 2
    metrics.recordsLoaded shouldBe 2
    metrics.recordsFailed shouldBe 0
  }

  it should "chain multiple transformers in sequence" in {
    // Given a pipeline with multiple transformers
    val extractor = new MockExtractor()
    val transformer1 = new FilterTransformer() // Reduces to 2 rows
    val transformer2 = new AddColumnTransformer() // Adds column
    val loader = new MockLoader()

    val pipeline = ETLPipeline(
      extractor = extractor,
      transformers = Seq(transformer1, transformer2),
      loader = loader
    )

    val config = createTestConfig()
    val context = ExecutionContext.create(spark, config)

    // When running pipeline
    val result = pipeline.run(context)

    // Then should apply transformers in order
    result.isSuccess shouldBe true

    // Should have both transformations applied
    loader.recordsLoaded shouldBe 2

    val metrics = result.metrics
    metrics.recordsExtracted shouldBe 5
    metrics.recordsTransformed shouldBe 2 // After all transforms
    metrics.recordsLoaded shouldBe 2
  }

  it should "handle pipeline with no transformers" in {
    // Given a pipeline without transformers
    val extractor = new MockExtractor()
    val loader = new MockLoader()

    val pipeline = ETLPipeline(
      extractor = extractor,
      transformers = Seq.empty,
      loader = loader
    )

    val config = createTestConfig()
    val context = ExecutionContext.create(spark, config)

    // When running pipeline
    val result = pipeline.run(context)

    // Then should pass data directly from extract to load
    result.isSuccess shouldBe true

    loader.recordsLoaded shouldBe 5 // All extracted records

    val metrics = result.metrics
    metrics.recordsExtracted shouldBe 5
    metrics.recordsTransformed shouldBe 5 // No transformation, but counted
    metrics.recordsLoaded shouldBe 5
  }

  it should "handle loader failure and return PipelineFailure" in {
    // Given a pipeline with failing loader
    val extractor = new MockExtractor()
    val loader = new FailingLoader()

    val pipeline = ETLPipeline(
      extractor = extractor,
      transformers = Seq.empty,
      loader = loader
    )

    val config = createTestConfig()
    val context = ExecutionContext.create(spark, config)

    // When running pipeline
    // Then should throw exception (to be caught by PipelineExecutor)
    an[RuntimeException] should be thrownBy {
      pipeline.run(context)
    }
  }

  it should "track metrics through all stages" in {
    // Given a pipeline with all stages
    val extractor = new MockExtractor()
    val transformer = new FilterTransformer()
    val loader = new MockLoader()

    val pipeline = ETLPipeline(
      extractor = extractor,
      transformers = Seq(transformer),
      loader = loader
    )

    val config = createTestConfig()
    val context = ExecutionContext.create(spark, config)

    // When running pipeline
    val result = pipeline.run(context)

    // Then should have detailed metrics
    val metrics = result.metrics

    metrics.pipelineId shouldBe "test-pipeline"
    metrics.executionId should not be empty
    metrics.recordsExtracted shouldBe 5
    metrics.recordsTransformed shouldBe 2
    metrics.recordsLoaded shouldBe 2
    metrics.recordsFailed shouldBe 0
    metrics.errors shouldBe empty
    metrics.duration should be > 0L
  }

  it should "update context metrics during execution" in {
    // Given a pipeline
    val extractor = new MockExtractor()
    val transformer = new FilterTransformer()
    val loader = new MockLoader()

    val pipeline = ETLPipeline(
      extractor = extractor,
      transformers = Seq(transformer),
      loader = loader
    )

    val config = createTestConfig()
    val context = ExecutionContext.create(spark, config)

    // Initial metrics should be zero
    context.metrics.recordsExtracted shouldBe 0

    // When running pipeline
    val result = pipeline.run(context)

    // Then context should be updated
    context.metrics.recordsExtracted shouldBe 5
    context.metrics.recordsTransformed shouldBe 2
    context.metrics.recordsLoaded shouldBe 2
  }

  it should "execute with write mode from config" in {
    // Given a pipeline with specific write mode
    val extractor = new MockExtractor()
    val loader = new MockLoader()

    val pipeline = ETLPipeline(
      extractor = extractor,
      transformers = Seq.empty,
      loader = loader
    )

    val config = createTestConfig(writeMode = WriteMode.Overwrite)
    val context = ExecutionContext.create(spark, config)

    // When running pipeline
    val result = pipeline.run(context)

    // Then should execute with specified mode
    result.isSuccess shouldBe true
  }

  private def createTestConfig(writeMode: WriteMode = WriteMode.Append): PipelineConfig = {
    PipelineConfig(
      pipelineId = "test-pipeline",
      extract = ExtractConfig(
        sourceType = SourceType.Kafka,
        topic = Some("test-topic"),
        schemaName = "user-event",
        connectionParams = Map("kafka.bootstrap.servers" -> "localhost:9092")
      ),
      transforms = Seq(
        TransformConfig(
          transformType = TransformType.Aggregation,
          parameters = Map.empty
        )
      ),
      load = LoadConfig(
        sinkType = SinkType.S3,
        path = Some("s3a://bucket/output/"),
        writeMode = writeMode,
        connectionParams = Map("format" -> "parquet")
      ),
      retry = RetryConfig(maxAttempts = 3, delaySeconds = 5),
      performance = PerformanceConfig(batchSize = 10000, parallelism = 4),
      logging = LoggingConfig(level = "INFO", structuredLogging = true)
    )
  }
}
