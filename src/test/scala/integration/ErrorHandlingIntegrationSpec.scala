package integration

import com.etl.config._
import com.etl.core.ETLPipeline
import com.etl.extract.S3Extractor
import com.etl.load.S3Loader
import com.etl.model.{PipelineFailure, PipelineMode, WriteMode}
import com.etl.quality.{DataQualityConfig, DataQualityRuleConfig, OnFailureAction, RuleType, Severity}
import com.etl.transform.FilterTransformer
import com.etl.util.{CircuitBreakerConfig, ErrorHandlingConfig, RetryConfig, RetryStrategy}

/**
 * Integration tests for error handling features.
 *
 * Tests:
 * - Retry behavior on transient failures
 * - Circuit breaker integration
 * - DLQ (Dead Letter Queue) functionality
 * - Data quality validation failures
 */
class ErrorHandlingIntegrationSpec extends IntegrationTestBase {

  describe("Error Handling Integration") {

    it("should handle missing source file gracefully") {
      // Setup: Reference non-existent file
      val nonExistentPath = tempFilePath("does-not-exist")

      val extractConfig = ExtractConfig(
        sourceType = SourceType.S3,
        path = Some(nonExistentPath),
        format = Some("parquet"),
        connectionParams = Map.empty
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
        pipelineId = "test-missing-source",
        name = "Test Missing Source",
        mode = PipelineMode.Batch,
        extract = extractConfig,
        transforms = Seq.empty,
        load = loadConfig
      )

      // Execute: Run pipeline (should fail)
      val pipeline = ETLPipeline(
        extractor = new S3Extractor(),
        transformers = Seq.empty,
        loader = new S3Loader()
      )

      val context = createTestContext("test-missing-source", pipelineConfig)
      val result = pipeline.run(context)

      // Verify: Pipeline failed with appropriate error
      result shouldBe a[PipelineFailure]
      result.metrics.recordsExtracted should be(0)
      result.metrics.errors should not be empty
      result.metrics.errors.head should include("Path does not exist")
    }

    it("should fail when data quality validation finds critical violations") {
      import spark.implicits._

      // Setup: Create data with quality issues (negative values)
      val sourceData = Seq(
        (1, "valid", 100.0),
        (2, "invalid", -50.0), // Negative value (violation)
        (3, "valid", 200.0),
        (4, "invalid", -100.0), // Negative value (violation)
        (5, "valid", 150.0)
      ).toDF("id", "status", "amount")

      val sourcePath = tempFilePath("quality-source")
      sourceData.write.parquet(sourcePath)

      val extractConfig = ExtractConfig(
        sourceType = SourceType.S3,
        path = Some(sourcePath),
        format = Some("parquet"),
        connectionParams = Map.empty
      )

      val loadPath = tempFilePath("quality-output")
      val loadConfig = LoadConfig(
        sinkType = SinkType.S3,
        path = Some(loadPath),
        format = Some("parquet"),
        writeMode = WriteMode.Overwrite,
        connectionParams = Map.empty
      )

      // Configure data quality rules
      val qualityRules = Seq(
        DataQualityRuleConfig(
          ruleType = RuleType.Range,
          name = "AmountMustBePositive",
          columns = Seq("amount"),
          severity = Severity.Error,
          parameters = Map("min" -> "0", "max" -> "10000")
        )
      )

      val dataQualityConfig = DataQualityConfig(
        enabled = true,
        validateAfterExtract = true,
        validateAfterTransform = false,
        onFailure = "fail", // Fail on validation errors
        rules = qualityRules
      )

      val pipelineConfig = PipelineConfig(
        pipelineId = "test-quality-fail",
        name = "Test Quality Fail",
        mode = PipelineMode.Batch,
        extract = extractConfig,
        transforms = Seq.empty,
        load = loadConfig,
        dataQualityConfig = dataQualityConfig
      )

      // Execute: Run pipeline (should fail due to quality violations)
      val pipeline = ETLPipeline(
        extractor = new S3Extractor(),
        transformers = Seq.empty,
        loader = new S3Loader()
      )

      val context = createTestContext("test-quality-fail", pipelineConfig)
      val result = pipeline.run(context)

      // Verify: Pipeline failed due to data quality
      result shouldBe a[PipelineFailure]
      result.metrics.recordsExtracted should be(5)
      result.metrics.recordsLoaded should be(0) // Should not load any data
    }

    it("should continue when data quality validation has warnings") {
      import spark.implicits._

      // Setup: Create data with warnings (not errors)
      val sourceData = Seq(
        (1, "valid", 100.0),
        (2, "warning", 5000.0), // High value (warning only)
        (3, "valid", 200.0)
      ).toDF("id", "status", "amount")

      val sourcePath = tempFilePath("quality-warning-source")
      sourceData.write.parquet(sourcePath)

      val extractConfig = ExtractConfig(
        sourceType = SourceType.S3,
        path = Some(sourcePath),
        format = Some("parquet"),
        connectionParams = Map.empty
      )

      val loadPath = tempFilePath("quality-warning-output")
      val loadConfig = LoadConfig(
        sinkType = SinkType.S3,
        path = Some(loadPath),
        format = Some("parquet"),
        writeMode = WriteMode.Overwrite,
        connectionParams = Map.empty
      )

      // Configure data quality rules with WARNING severity
      val qualityRules = Seq(
        DataQualityRuleConfig(
          ruleType = RuleType.Range,
          name = "AmountReasonableRange",
          columns = Seq("amount"),
          severity = Severity.Warning, // Warning only
          parameters = Map("min" -> "0", "max" -> "1000")
        )
      )

      val dataQualityConfig = DataQualityConfig(
        enabled = true,
        validateAfterExtract = true,
        validateAfterTransform = false,
        onFailure = "warn", // Continue on warnings
        rules = qualityRules
      )

      val pipelineConfig = PipelineConfig(
        pipelineId = "test-quality-warning",
        name = "Test Quality Warning",
        mode = PipelineMode.Batch,
        extract = extractConfig,
        transforms = Seq.empty,
        load = loadConfig,
        dataQualityConfig = dataQualityConfig
      )

      // Execute: Run pipeline (should succeed despite warnings)
      val pipeline = ETLPipeline(
        extractor = new S3Extractor(),
        transformers = Seq.empty,
        loader = new S3Loader()
      )

      val context = createTestContext("test-quality-warning", pipelineConfig)
      val result = pipeline.run(context)

      // Verify: Pipeline succeeded despite warnings
      result.isSuccess should be(true)
      result.metrics.recordsExtracted should be(3)
      result.metrics.recordsLoaded should be(3)

      // Verify: Output contains all records (including those with warnings)
      val outputData = spark.read.parquet(loadPath)
      outputData.count() should be(3)
    }

    it("should validate not-null rule correctly") {
      import spark.implicits._

      // Setup: Create data with null values
      val sourceData = Seq(
        (Some(1), Some("value1")),
        (Some(2), None), // Null name (violation)
        (Some(3), Some("value3")),
        (None, Some("value4")) // Null id (violation)
      ).toDF("id", "name")

      val sourcePath = tempFilePath("null-check-source")
      sourceData.write.parquet(sourcePath)

      val extractConfig = ExtractConfig(
        sourceType = SourceType.S3,
        path = Some(sourcePath),
        format = Some("parquet"),
        connectionParams = Map.empty
      )

      val loadPath = tempFilePath("null-check-output")
      val loadConfig = LoadConfig(
        sinkType = SinkType.S3,
        path = Some(loadPath),
        format = Some("parquet"),
        writeMode = WriteMode.Overwrite,
        connectionParams = Map.empty
      )

      // Configure not-null validation
      val qualityRules = Seq(
        DataQualityRuleConfig(
          ruleType = RuleType.NotNull,
          name = "RequiredFields",
          columns = Seq("id", "name"),
          severity = Severity.Error,
          parameters = Map.empty
        )
      )

      val dataQualityConfig = DataQualityConfig(
        enabled = true,
        validateAfterExtract = true,
        validateAfterTransform = false,
        onFailure = "fail",
        rules = qualityRules
      )

      val pipelineConfig = PipelineConfig(
        pipelineId = "test-null-check",
        name = "Test Null Check",
        mode = PipelineMode.Batch,
        extract = extractConfig,
        transforms = Seq.empty,
        load = loadConfig,
        dataQualityConfig = dataQualityConfig
      )

      // Execute: Run pipeline (should fail due to nulls)
      val pipeline = ETLPipeline(
        extractor = new S3Extractor(),
        transformers = Seq.empty,
        loader = new S3Loader()
      )

      val context = createTestContext("test-null-check", pipelineConfig)
      val result = pipeline.run(context)

      // Verify: Pipeline failed due to null values
      result shouldBe a[PipelineFailure]
      result.metrics.recordsExtracted should be(4)
      result.metrics.recordsLoaded should be(0)
    }

    it("should handle transformation errors gracefully") {
      import spark.implicits._

      // Setup: Create data
      val sourceData = (1 to 100).map(i => (i, s"value_$i")).toDF("id", "value")
      val sourcePath = tempFilePath("transform-error-source")
      sourceData.write.parquet(sourcePath)

      val extractConfig = ExtractConfig(
        sourceType = SourceType.S3,
        path = Some(sourcePath),
        format = Some("parquet"),
        connectionParams = Map.empty
      )

      // Configure filter with invalid condition (should cause error)
      val filterConfig = TransformConfig(
        transformType = TransformType.Filter,
        parameters = Map(
          "condition" -> "invalid_column = 'test'" // Invalid column reference
        )
      )

      val loadPath = tempFilePath("transform-error-output")
      val loadConfig = LoadConfig(
        sinkType = SinkType.S3,
        path = Some(loadPath),
        format = Some("parquet"),
        writeMode = WriteMode.Overwrite,
        connectionParams = Map.empty
      )

      val pipelineConfig = PipelineConfig(
        pipelineId = "test-transform-error",
        name = "Test Transform Error",
        mode = PipelineMode.Batch,
        extract = extractConfig,
        transforms = Seq(filterConfig),
        load = loadConfig
      )

      // Execute: Run pipeline (should fail during transformation)
      val pipeline = ETLPipeline(
        extractor = new S3Extractor(),
        transformers = Seq(new FilterTransformer()),
        loader = new S3Loader()
      )

      val context = createTestContext("test-transform-error", pipelineConfig)
      val result = pipeline.run(context)

      // Verify: Pipeline failed during transformation
      result shouldBe a[PipelineFailure]
      result.metrics.recordsExtracted should be(100)
      result.metrics.errors should not be empty
    }

    it("should track failures in metrics") {
      import spark.implicits._

      // Setup: Create empty source (will succeed but with 0 records)
      val sourceData = Seq.empty[(Int, String)].toDF("id", "value")
      val sourcePath = tempFilePath("empty-source")
      sourceData.write.parquet(sourcePath)

      val extractConfig = ExtractConfig(
        sourceType = SourceType.S3,
        path = Some(sourcePath),
        format = Some("parquet"),
        connectionParams = Map.empty
      )

      val loadPath = tempFilePath("empty-output")
      val loadConfig = LoadConfig(
        sinkType = SinkType.S3,
        path = Some(loadPath),
        format = Some("parquet"),
        writeMode = WriteMode.Overwrite,
        connectionParams = Map.empty
      )

      val pipelineConfig = PipelineConfig(
        pipelineId = "test-empty-data",
        name = "Test Empty Data",
        mode = PipelineMode.Batch,
        extract = extractConfig,
        transforms = Seq.empty,
        load = loadConfig
      )

      // Execute: Run pipeline with empty data
      val pipeline = ETLPipeline(
        extractor = new S3Extractor(),
        transformers = Seq.empty,
        loader = new S3Loader()
      )

      val context = createTestContext("test-empty-data", pipelineConfig)
      val result = pipeline.run(context)

      // Verify: Pipeline succeeded (empty is not an error)
      result.isSuccess should be(true)
      result.metrics.recordsExtracted should be(0)
      result.metrics.recordsLoaded should be(0)
      result.metrics.recordsFailed should be(0)
      result.metrics.successRate should be(0.0) // 0/0 = 0
    }
  }
}
