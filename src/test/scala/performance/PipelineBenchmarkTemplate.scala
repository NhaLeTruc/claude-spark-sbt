package performance

import com.etl.config._
import com.etl.core.{ETLPipeline, ExecutionContext}
import com.etl.extract.S3Extractor
import com.etl.load.S3Loader
import com.etl.model.ExecutionMetrics
import com.etl.monitoring.ETLMetrics
import com.etl.transform.AggregationTransformer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Random

/**
 * Performance benchmark template for ETL pipelines.
 *
 * This template provides a structure for performance testing pipelines.
 * Adapt it to your specific use case.
 *
 * To run benchmarks:
 *   sbt "testOnly performance.PipelineBenchmarkTemplate"
 *
 * Note: This is a template, not a production benchmark suite.
 * For production benchmarks, consider using JMH (Java Microbenchmark Harness).
 */
class PipelineBenchmarkTemplate extends AnyFlatSpec with Matchers {

  /**
   * Configuration for benchmark tests.
   */
  object BenchmarkConfig {
    val warmupIterations = 3
    val measurementIterations = 10
    val recordCounts = Seq(1000, 10000, 100000, 1000000)
    val partitionCounts = Seq(2, 4, 8, 16)
  }

  /**
   * Benchmark result for reporting.
   */
  case class BenchmarkResult(
    testName: String,
    recordCount: Long,
    partitions: Int,
    avgDurationMs: Double,
    minDurationMs: Long,
    maxDurationMs: Long,
    stdDev: Double,
    throughputRecordsPerSec: Double
  )

  /**
   * Generate test data for benchmarks.
   */
  def generateTestData(spark: SparkSession, recordCount: Int, partitions: Int): DataFrame = {
    import spark.implicits._

    val random = new Random(42) // Fixed seed for reproducibility

    val data = (1 to recordCount).map { i =>
      (
        s"user_${random.nextInt(1000)}",
        s"event_${random.nextInt(100)}",
        random.nextDouble() * 1000,
        System.currentTimeMillis()
      )
    }

    spark.createDataset(data)
      .toDF("user_id", "event_type", "amount", "timestamp")
      .repartition(partitions)
  }

  /**
   * Run a benchmark with multiple iterations.
   */
  def runBenchmark(
    name: String,
    recordCount: Int,
    partitions: Int
  )(testFn: => Unit): BenchmarkResult = {
    println(s"\n=== Running benchmark: $name ===")
    println(s"Records: $recordCount, Partitions: $partitions")

    // Warmup
    println("Warming up...")
    (1 to BenchmarkConfig.warmupIterations).foreach { _ =>
      testFn
    }

    // Measurement
    println("Measuring...")
    val durations = (1 to BenchmarkConfig.measurementIterations).map { iteration =>
      val startTime = System.nanoTime()
      testFn
      val duration = (System.nanoTime() - startTime) / 1000000 // Convert to milliseconds
      println(s"  Iteration $iteration: ${duration}ms")
      duration
    }

    // Calculate statistics
    val avgDuration = durations.sum.toDouble / durations.size
    val minDuration = durations.min
    val maxDuration = durations.max
    val variance = durations.map(d => math.pow(d - avgDuration, 2)).sum / durations.size
    val stdDev = math.sqrt(variance)
    val throughput = (recordCount * 1000.0) / avgDuration

    val result = BenchmarkResult(
      name,
      recordCount,
      partitions,
      avgDuration,
      minDuration,
      maxDuration,
      stdDev,
      throughput
    )

    printBenchmarkResult(result)
    result
  }

  /**
   * Print benchmark result in readable format.
   */
  def printBenchmarkResult(result: BenchmarkResult): Unit = {
    println("\nResults:")
    println(f"  Avg Duration:  ${result.avgDurationMs}%.2f ms")
    println(f"  Min Duration:  ${result.minDurationMs} ms")
    println(f"  Max Duration:  ${result.maxDurationMs} ms")
    println(f"  Std Dev:       ${result.stdDev}%.2f ms")
    println(f"  Throughput:    ${result.throughputRecordsPerSec}%.2f records/sec")
  }

  /**
   * Example benchmark: Test aggregation performance.
   */
  "Aggregation pipeline" should "demonstrate performance characteristics" in {
    val spark = SparkSession.builder()
      .appName("BenchmarkTest")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    try {
      val results = BenchmarkConfig.recordCounts.flatMap { recordCount =>
        BenchmarkConfig.partitionCounts.map { partitions =>
          runBenchmark(
            name = s"Aggregation-$recordCount-records-$partitions-partitions",
            recordCount = recordCount,
            partitions = partitions
          ) {
            val testData = generateTestData(spark, recordCount, partitions)
            val result = testData
              .groupBy("user_id")
              .agg(
                org.apache.spark.sql.functions.sum("amount").as("total_amount"),
                org.apache.spark.sql.functions.count("*").as("event_count")
              )
              .collect() // Force execution
          }
        }
      }

      // Generate summary report
      printSummaryReport(results)

    } finally {
      spark.stop()
    }
  }

  /**
   * Example benchmark: Test full pipeline performance.
   */
  "Full ETL pipeline" should "benchmark end-to-end performance" ignore {
    // This test is ignored by default to avoid long test runs
    // Enable it when you want to run full pipeline benchmarks

    val spark = SparkSession.builder()
      .appName("FullPipelineBenchmark")
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    try {
      // Configure test pipeline
      val extractConfig = ExtractConfig(
        sourceType = SourceType.S3,
        path = Some("s3a://test-bucket/input/"),
        schemaName = "test-schema",
        connectionParams = Map.empty
      )

      val transformConfig = TransformConfig(
        transformType = TransformType.Aggregation,
        parameters = Map(
          "groupBy" -> "[\"user_id\"]",
          "aggregations" -> """[{"column":"amount","function":"sum","alias":"total"}]"""
        )
      )

      val loadConfig = LoadConfig(
        sinkType = SinkType.S3,
        path = Some("s3a://test-bucket/output/"),
        writeMode = "Overwrite",
        schemaName = "test-schema",
        connectionParams = Map.empty
      )

      val config = PipelineConfig(
        pipelineId = "benchmark-pipeline",
        name = "Benchmark Pipeline",
        extract = extractConfig,
        transforms = Seq(transformConfig),
        load = loadConfig,
        performanceConfig = PerformanceConfig(
          shufflePartitions = Some(8)
        )
      )

      // Create pipeline
      val extractor = new S3Extractor()
      val transformers = Seq(new AggregationTransformer())
      val loader = new S3Loader()
      val pipeline = ETLPipeline(extractor, transformers, loader)

      // Run benchmark
      val result = runBenchmark(
        name = "Full-ETL-Pipeline",
        recordCount = 100000,
        partitions = 8
      ) {
        // Note: This requires actual S3 setup
        // Modify to use test data or mock S3
        val context = ExecutionContext(
          spark = spark,
          config = config,
          vault = new CredentialVault(),
          metrics = ExecutionMetrics.empty
        )
        pipeline.run(context)
      }

      result.avgDurationMs should be < 10000.0 // Example assertion

    } finally {
      spark.stop()
    }
  }

  /**
   * Print summary report of all benchmarks.
   */
  def printSummaryReport(results: Seq[BenchmarkResult]): Unit = {
    println("\n" + "="*80)
    println("BENCHMARK SUMMARY")
    println("="*80)
    println()
    println(f"${"Test"}%-50s ${"Records"}%12s ${"Partitions"}%12s ${"Avg(ms)"}%12s ${"Throughput"}%15s")
    println("-"*80)

    results.foreach { result =>
      println(f"${result.testName}%-50s ${result.recordCount}%12d ${result.partitions}%12d ${result.avgDurationMs}%12.2f ${result.throughputRecordsPerSec}%15.2f")
    }

    println()
    println("Notes:")
    println("  - Throughput is measured in records/second")
    println("  - Results include JVM warmup effects")
    println("  - For production benchmarks, use JMH")
    println()
  }
}

/**
 * Companion object with utility methods for benchmarking.
 */
object PipelineBenchmarkTemplate {

  /**
   * Example: How to create a custom benchmark.
   *
   * Usage:
   * {{{
   *   val benchmark = new CustomBenchmark()
   *   benchmark.runCustomTest()
   * }}}
   */
  class CustomBenchmark {
    def runCustomTest(): Unit = {
      println("Implement your custom benchmark here")
      // 1. Setup test data
      // 2. Run warmup iterations
      // 3. Measure performance
      // 4. Calculate statistics
      // 5. Report results
    }
  }

  /**
   * Export benchmark results to CSV for analysis.
   */
  def exportToCsv(results: Seq[BenchmarkResult], filename: String): Unit = {
    import java.io.PrintWriter

    val writer = new PrintWriter(filename)
    try {
      writer.println("Test,Records,Partitions,AvgDuration(ms),MinDuration(ms),MaxDuration(ms),StdDev,Throughput(rec/sec)")
      results.foreach { r =>
        writer.println(s"${r.testName},${r.recordCount},${r.partitions},${r.avgDurationMs},${r.minDurationMs},${r.maxDurationMs},${r.stdDev},${r.throughputRecordsPerSec}")
      }
    } finally {
      writer.close()
    }
    println(s"Results exported to: $filename")
  }
}
