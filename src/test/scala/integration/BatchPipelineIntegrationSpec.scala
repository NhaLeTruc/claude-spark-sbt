package integration

import com.etl.config._
import com.etl.core.ETLPipeline
import com.etl.extract.S3Extractor
import com.etl.load.S3Loader
import com.etl.model.{PipelineMode, PipelineSuccess, WriteMode}
import com.etl.monitoring.{ETLMetrics, MetricsRegistry}
import com.etl.transform.{AggregationTransformer, FilterTransformer}
import org.apache.spark.sql.functions._

/**
 * End-to-end integration tests for batch pipelines.
 *
 * Tests:
 * - Complete ETL pipeline execution (extract → transform → load)
 * - Data correctness through all stages
 * - Metrics collection
 * - Error handling
 */
class BatchPipelineIntegrationSpec extends IntegrationTestBase {

  describe("Batch Pipeline End-to-End") {

    it("should successfully execute S3 → Filter → Aggregate → S3 pipeline") {
      import spark.implicits._

      // Setup: Create source data
      val sourceData = (1 to 1000).map { i =>
        (i, s"category_${i % 10}", i * 10.0, if (i % 2 == 0) "active" else "inactive")
      }.toDF("id", "category", "amount", "status")

      val sourcePath = tempFilePath("source")
      sourceData.write.parquet(sourcePath)

      // Setup: Create pipeline configuration
      val extractConfig = ExtractConfig(
        sourceType = SourceType.S3,
        path = Some(sourcePath),
        format = Some("parquet"),
        connectionParams = Map.empty
      )

      val filterConfig = TransformConfig(
        transformType = TransformType.Filter,
        parameters = Map(
          "condition" -> "status = 'active'"
        )
      )

      val aggregateConfig = TransformConfig(
        transformType = TransformType.Aggregation,
        parameters = Map(
          "groupByColumns" -> "category",
          "aggregations" -> "total_amount:sum(amount),record_count:count(*),avg_amount:avg(amount)"
        )
      )

      val loadPath = tempFilePath("output")
      val loadConfig = LoadConfig(
        sinkType = SinkType.S3,
        path = Some(loadPath),
        format = Some("parquet"),
        writeMode = WriteMode.Overwrite,
        connectionParams = Map.empty
      )

      val pipelineConfig = PipelineConfig(
        pipelineId = "test-batch-pipeline",
        name = "Test Batch Pipeline",
        mode = PipelineMode.Batch,
        extract = extractConfig,
        transforms = Seq(filterConfig, aggregateConfig),
        load = loadConfig
      )

      // Execute: Run pipeline
      val pipeline = ETLPipeline(
        extractor = new S3Extractor(),
        transformers = Seq(new FilterTransformer(), new AggregationTransformer()),
        loader = new S3Loader()
      )

      val context = createTestContext("test-batch-pipeline", pipelineConfig)

      // Clear metrics before test
      MetricsRegistry.clear()

      val result = pipeline.run(context)

      // Verify: Pipeline succeeded
      result shouldBe a[PipelineSuccess]
      result.metrics.recordsExtracted should be(1000)
      result.metrics.recordsTransformed should be(10) // 10 categories
      result.metrics.recordsLoaded should be(10)
      result.metrics.recordsFailed should be(0)

      // Verify: Output data correctness
      val outputData = spark.read.parquet(loadPath)
      outputData.count() should be(10) // 10 categories

      // Verify: Aggregation correctness for category_0
      val category0 = outputData.filter($"category" === "category_0").head()
      category0.getAs[Long]("record_count") should be(50) // 500 active records / 10 categories
      category0.getAs[Double]("total_amount") should be(25000.0) // Sum of amounts

      // Verify: Prometheus metrics were collected
      val extractedMetric = ETLMetrics.recordsExtractedTotal.get(
        Map("pipeline_id" -> "test-batch-pipeline", "source_type" -> "S3")
      )
      extractedMetric should be(1000.0)

      val loadedMetric = ETLMetrics.recordsLoadedTotal.get(
        Map("pipeline_id" -> "test-batch-pipeline", "sink_type" -> "S3", "write_mode" -> "Overwrite")
      )
      loadedMetric should be(10.0)
    }

    it("should handle pipeline with no transformers (passthrough)") {
      import spark.implicits._

      // Setup: Create source data
      val sourceData = (1 to 100).map(i => (i, s"value_$i")).toDF("id", "value")
      val sourcePath = tempFilePath("passthrough-source")
      sourceData.write.json(sourcePath)

      val extractConfig = ExtractConfig(
        sourceType = SourceType.S3,
        path = Some(sourcePath),
        format = Some("json"),
        connectionParams = Map.empty
      )

      val loadPath = tempFilePath("passthrough-output")
      val loadConfig = LoadConfig(
        sinkType = SinkType.S3,
        path = Some(loadPath),
        format = Some("json"),
        writeMode = WriteMode.Overwrite,
        connectionParams = Map.empty
      )

      val pipelineConfig = PipelineConfig(
        pipelineId = "test-passthrough-pipeline",
        name = "Test Passthrough Pipeline",
        mode = PipelineMode.Batch,
        extract = extractConfig,
        transforms = Seq.empty, // No transformers
        load = loadConfig
      )

      // Execute: Run pipeline
      val pipeline = ETLPipeline(
        extractor = new S3Extractor(),
        transformers = Seq.empty,
        loader = new S3Loader()
      )

      val context = createTestContext("test-passthrough-pipeline", pipelineConfig)
      val result = pipeline.run(context)

      // Verify: Pipeline succeeded
      result shouldBe a[PipelineSuccess]
      result.metrics.recordsExtracted should be(100)
      result.metrics.recordsTransformed should be(100) // Same as extracted
      result.metrics.recordsLoaded should be(100)

      // Verify: Output data matches input
      val outputData = spark.read.json(loadPath)
      outputData.count() should be(100)

      val inputSorted = sourceData.orderBy("id").collect()
      val outputSorted = outputData.orderBy("id").collect()
      inputSorted should equal(outputSorted)
    }

    it("should handle large datasets efficiently") {
      import spark.implicits._

      // Setup: Create large dataset (10,000 records)
      val largeData = (1 to 10000).map { i =>
        (i, s"category_${i % 100}", i * 1.5, java.sql.Timestamp.valueOf(s"2024-01-01 ${i % 24}:00:00"))
      }.toDF("id", "category", "amount", "timestamp")

      val sourcePath = tempFilePath("large-source")
      largeData.write.parquet(sourcePath)

      val extractConfig = ExtractConfig(
        sourceType = SourceType.S3,
        path = Some(sourcePath),
        format = Some("parquet"),
        connectionParams = Map.empty
      )

      val aggregateConfig = TransformConfig(
        transformType = TransformType.Aggregation,
        parameters = Map(
          "groupByColumns" -> "category",
          "aggregations" -> "total:sum(amount),count:count(*)"
        )
      )

      val loadPath = tempFilePath("large-output")
      val loadConfig = LoadConfig(
        sinkType = SinkType.S3,
        path = Some(loadPath),
        format = Some("parquet"),
        writeMode = WriteMode.Overwrite,
        connectionParams = Map.empty
      )

      val pipelineConfig = PipelineConfig(
        pipelineId = "test-large-pipeline",
        name = "Test Large Pipeline",
        mode = PipelineMode.Batch,
        extract = extractConfig,
        transforms = Seq(aggregateConfig),
        load = loadConfig
      )

      // Execute: Run pipeline with timing
      val pipeline = ETLPipeline(
        extractor = new S3Extractor(),
        transformers = Seq(new AggregationTransformer()),
        loader = new S3Loader()
      )

      val context = createTestContext("test-large-pipeline", pipelineConfig)

      val startTime = System.nanoTime()
      val result = pipeline.run(context)
      val durationSeconds = (System.nanoTime() - startTime) / 1e9

      // Verify: Pipeline succeeded
      result shouldBe a[PipelineSuccess]
      result.metrics.recordsExtracted should be(10000)
      result.metrics.recordsLoaded should be(100) // 100 categories

      // Verify: Performance is acceptable (should complete in under 30 seconds for local mode)
      durationSeconds should be < 30.0

      // Verify: Output correctness
      val outputData = spark.read.parquet(loadPath)
      outputData.count() should be(100)

      // Each category should have 100 records
      outputData.select("count").distinct().collect() should have length 1
      outputData.select("count").head().getLong(0) should be(100)
    }

    it("should properly handle append mode") {
      import spark.implicits._

      // Setup: Create initial data
      val initialData = (1 to 50).map(i => (i, s"value_$i")).toDF("id", "value")
      val outputPath = tempFilePath("append-output")
      initialData.write.parquet(outputPath)

      // Setup: Create new data to append
      val newData = (51 to 100).map(i => (i, s"value_$i")).toDF("id", "value")
      val sourcePath = tempFilePath("append-source")
      newData.write.parquet(sourcePath)

      val extractConfig = ExtractConfig(
        sourceType = SourceType.S3,
        path = Some(sourcePath),
        format = Some("parquet"),
        connectionParams = Map.empty
      )

      val loadConfig = LoadConfig(
        sinkType = SinkType.S3,
        path = Some(outputPath),
        format = Some("parquet"),
        writeMode = WriteMode.Append,
        connectionParams = Map.empty
      )

      val pipelineConfig = PipelineConfig(
        pipelineId = "test-append-pipeline",
        name = "Test Append Pipeline",
        mode = PipelineMode.Batch,
        extract = extractConfig,
        transforms = Seq.empty,
        load = loadConfig
      )

      // Execute: Run pipeline
      val pipeline = ETLPipeline(
        extractor = new S3Extractor(),
        transformers = Seq.empty,
        loader = new S3Loader()
      )

      val context = createTestContext("test-append-pipeline", pipelineConfig)
      val result = pipeline.run(context)

      // Verify: Pipeline succeeded
      result shouldBe a[PipelineSuccess]

      // Verify: Output contains both initial and new data
      val finalData = spark.read.parquet(outputPath)
      finalData.count() should be(100) // 50 initial + 50 new

      val ids = finalData.select("id").collect().map(_.getInt(0)).sorted
      ids should equal((1 to 100).toArray)
    }

    it("should collect accurate metrics at each stage") {
      import spark.implicits._

      // Setup: Create test data with known characteristics
      val sourceData = (1 to 200).map(i => (i, i % 2 == 0)).toDF("id", "is_even")
      val sourcePath = tempFilePath("metrics-source")
      sourceData.write.parquet(sourcePath)

      val extractConfig = ExtractConfig(
        sourceType = SourceType.S3,
        path = Some(sourcePath),
        format = Some("parquet"),
        connectionParams = Map.empty
      )

      // Filter to keep only even numbers (should reduce to 100 records)
      val filterConfig = TransformConfig(
        transformType = TransformType.Filter,
        parameters = Map("condition" -> "is_even = true")
      )

      val loadPath = tempFilePath("metrics-output")
      val loadConfig = LoadConfig(
        sinkType = SinkType.S3,
        path = Some(loadPath),
        format = Some("parquet"),
        writeMode = WriteMode.Overwrite,
        connectionParams = Map.empty
      )

      val pipelineConfig = PipelineConfig(
        pipelineId = "test-metrics-pipeline",
        name = "Test Metrics Pipeline",
        mode = PipelineMode.Batch,
        extract = extractConfig,
        transforms = Seq(filterConfig),
        load = loadConfig
      )

      // Execute: Run pipeline
      val pipeline = ETLPipeline(
        extractor = new S3Extractor(),
        transformers = Seq(new FilterTransformer()),
        loader = new S3Loader()
      )

      val context = createTestContext("test-metrics-pipeline", pipelineConfig)

      // Clear metrics
      MetricsRegistry.clear()

      val result = pipeline.run(context)

      // Verify: ExecutionMetrics are accurate
      result.metrics.recordsExtracted should be(200)
      result.metrics.recordsTransformed should be(100) // After filter
      result.metrics.recordsLoaded should be(100)
      result.metrics.recordsFailed should be(0)
      result.metrics.duration.isDefined should be(true)
      result.metrics.successRate should be(1.0) // 100% success

      // Verify: Prometheus metrics match
      val extractCount = ETLMetrics.recordsExtractedTotal.get(
        Map("pipeline_id" -> "test-metrics-pipeline", "source_type" -> "S3")
      )
      extractCount should be(200.0)

      val transformCount = ETLMetrics.recordsTransformedTotal.get(
        Map("pipeline_id" -> "test-metrics-pipeline", "transform_type" -> "all")
      )
      transformCount should be(100.0)

      val loadCount = ETLMetrics.recordsLoadedTotal.get(
        Map("pipeline_id" -> "test-metrics-pipeline", "sink_type" -> "S3", "write_mode" -> "Overwrite")
      )
      loadCount should be(100.0)

      // Verify: Duration metrics exist
      val pipelineDurationCount = ETLMetrics.pipelineDurationSeconds.count(
        Map("pipeline_id" -> "test-metrics-pipeline", "status" -> "success")
      )
      pipelineDurationCount should be(1L)
    }
  }
}
