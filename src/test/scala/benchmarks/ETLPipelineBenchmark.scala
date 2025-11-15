package benchmarks

import com.etl.config._
import com.etl.core.{ETLPipeline, ExecutionContext}
import com.etl.extract.Extractor
import com.etl.load.{LoadResult, Loader}
import com.etl.transform.Transformer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit
import scala.util.Random

/**
 * JMH benchmarks for ETL pipeline operations.
 *
 * Run with: sbt "Jmh/run -i 10 -wi 5 -f 1 -t 1 benchmarks.ETLPipelineBenchmark"
 *
 * Options:
 *   -i  : Number of measurement iterations (default: 10)
 *   -wi : Number of warmup iterations (default: 5)
 *   -f  : Number of forks (default: 1)
 *   -t  : Number of threads (default: 1)
 *   -bm : Benchmark mode (thrpt, avgt, sample, ss, all)
 *
 * Example: sbt "Jmh/run -i 20 -wi 10 -f 2 ETLPipelineBenchmark.benchmarkFullPipeline"
 */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class ETLPipelineBenchmark {

  /**
   * Benchmark state for Spark session lifecycle.
   */
  @State(Scope.Benchmark)
  class SparkState {
    var spark: SparkSession = _

    @Setup(Level.Trial)
    def setupSpark(): Unit = {
      spark = SparkSession.builder()
        .appName("ETL-Benchmark")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    }

    @TearDown(Level.Trial)
    def teardownSpark(): Unit = {
      if (spark != null) {
        spark.stop()
        spark = null
      }
    }
  }

  /**
   * Simple extractor for benchmarking.
   */
  class BenchmarkExtractor(recordCount: Int) extends Extractor {
    override def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame = {
      import spark.implicits._
      val random = new Random(42)

      val data = (1 to recordCount).map { i =>
        (
          s"user_${random.nextInt(100)}",
          s"event_${random.nextInt(20)}",
          random.nextDouble() * 1000,
          System.currentTimeMillis()
        )
      }

      spark.createDataset(data)
        .toDF("user_id", "event_type", "amount", "timestamp")
    }
  }

  /**
   * Simple aggregation transformer for benchmarking.
   */
  class BenchmarkAggregationTransformer extends Transformer {
    override def transform(df: DataFrame, config: TransformConfig): DataFrame = {
      import org.apache.spark.sql.functions._

      df.groupBy("user_id")
        .agg(
          sum("amount").as("total_amount"),
          count("*").as("event_count"),
          avg("amount").as("avg_amount")
        )
    }
  }

  /**
   * Simple loader for benchmarking.
   */
  class BenchmarkLoader extends Loader {
    override def load(
      df: DataFrame,
      config: LoadConfig,
      writeMode: String
    ): LoadResult = {
      val count = df.count() // Force computation
      LoadResult.success(count)
    }
  }

  /**
   * Benchmark: Extract operation with different record counts.
   */
  @Benchmark
  def benchmarkExtract_1K(state: SparkState): DataFrame = {
    implicit val spark: SparkSession = state.spark
    val extractor = new BenchmarkExtractor(1000)
    val config = ExtractConfig(
      sourceType = SourceType.S3,
      path = Some("benchmark://test"),
      schemaName = "test",
      connectionParams = Map.empty
    )
    extractor.extract(config)
  }

  @Benchmark
  def benchmarkExtract_10K(state: SparkState): DataFrame = {
    implicit val spark: SparkSession = state.spark
    val extractor = new BenchmarkExtractor(10000)
    val config = ExtractConfig(
      sourceType = SourceType.S3,
      path = Some("benchmark://test"),
      schemaName = "test",
      connectionParams = Map.empty
    )
    extractor.extract(config)
  }

  @Benchmark
  def benchmarkExtract_100K(state: SparkState): DataFrame = {
    implicit val spark: SparkSession = state.spark
    val extractor = new BenchmarkExtractor(100000)
    val config = ExtractConfig(
      sourceType = SourceType.S3,
      path = Some("benchmark://test"),
      schemaName = "test",
      connectionParams = Map.empty
    )
    extractor.extract(config)
  }

  /**
   * Benchmark: Transform operation (aggregation).
   */
  @Benchmark
  def benchmarkTransform_Aggregation(state: SparkState): DataFrame = {
    implicit val spark: SparkSession = state.spark
    val extractor = new BenchmarkExtractor(10000)
    val extractConfig = ExtractConfig(
      sourceType = SourceType.S3,
      path = Some("benchmark://test"),
      schemaName = "test",
      connectionParams = Map.empty
    )
    val df = extractor.extract(extractConfig)

    val transformer = new BenchmarkAggregationTransformer()
    val transformConfig = TransformConfig(
      transformType = TransformType.Aggregation,
      parameters = Map.empty
    )
    transformer.transform(df, transformConfig)
  }

  /**
   * Benchmark: Load operation.
   */
  @Benchmark
  def benchmarkLoad(state: SparkState): LoadResult = {
    implicit val spark: SparkSession = state.spark
    val extractor = new BenchmarkExtractor(10000)
    val extractConfig = ExtractConfig(
      sourceType = SourceType.S3,
      path = Some("benchmark://test"),
      schemaName = "test",
      connectionParams = Map.empty
    )
    val df = extractor.extract(extractConfig)

    val loader = new BenchmarkLoader()
    val loadConfig = LoadConfig(
      sinkType = SinkType.S3,
      path = Some("benchmark://output"),
      writeMode = "Overwrite",
      schemaName = "test",
      connectionParams = Map.empty
    )
    loader.load(df, loadConfig, "Overwrite")
  }

  /**
   * Benchmark: Full pipeline (extract + transform + load).
   */
  @Benchmark
  def benchmarkFullPipeline(state: SparkState): Unit = {
    implicit val spark: SparkSession = state.spark

    val extractor = new BenchmarkExtractor(10000)
    val transformers = Seq(new BenchmarkAggregationTransformer())
    val loader = new BenchmarkLoader()

    val pipeline = ETLPipeline(extractor, transformers, loader)

    val config = PipelineConfig(
      pipelineId = "benchmark-pipeline",
      name = "Benchmark Pipeline",
      extract = ExtractConfig(
        sourceType = SourceType.S3,
        path = Some("benchmark://test"),
        schemaName = "test",
        connectionParams = Map.empty
      ),
      transforms = Seq(
        TransformConfig(
          transformType = TransformType.Aggregation,
          parameters = Map.empty
        )
      ),
      load = LoadConfig(
        sinkType = SinkType.S3,
        path = Some("benchmark://output"),
        writeMode = "Overwrite",
        schemaName = "test",
        connectionParams = Map.empty
      ),
      performanceConfig = PerformanceConfig(
        shufflePartitions = Some(4)
      )
    )

    val context = ExecutionContext.create(spark, config)
    pipeline.run(context)
  }

  /**
   * Benchmark: Pipeline with caching.
   */
  @Benchmark
  def benchmarkPipelineWithCaching(state: SparkState): Unit = {
    implicit val spark: SparkSession = state.spark

    val extractor = new BenchmarkExtractor(10000)
    val transformers = Seq(new BenchmarkAggregationTransformer())
    val loader = new BenchmarkLoader()

    val pipeline = ETLPipeline(extractor, transformers, loader)

    val config = PipelineConfig(
      pipelineId = "benchmark-pipeline-cached",
      name = "Benchmark Pipeline with Caching",
      extract = ExtractConfig(
        sourceType = SourceType.S3,
        path = Some("benchmark://test"),
        schemaName = "test",
        connectionParams = Map.empty
      ),
      transforms = Seq(
        TransformConfig(
          transformType = TransformType.Aggregation,
          parameters = Map.empty
        )
      ),
      load = LoadConfig(
        sinkType = SinkType.S3,
        path = Some("benchmark://output"),
        writeMode = "Overwrite",
        schemaName = "test",
        connectionParams = Map.empty
      ),
      performanceConfig = PerformanceConfig(
        shufflePartitions = Some(4),
        enableCaching = Some(true)
      )
    )

    val context = ExecutionContext.create(spark, config)
    pipeline.run(context)
  }

  /**
   * Benchmark: Different partition counts.
   */
  @Benchmark
  @OperationsPerInvocation(1)
  def benchmarkPipeline_2Partitions(state: SparkState): Unit = {
    benchmarkWithPartitions(state, 2)
  }

  @Benchmark
  @OperationsPerInvocation(1)
  def benchmarkPipeline_4Partitions(state: SparkState): Unit = {
    benchmarkWithPartitions(state, 4)
  }

  @Benchmark
  @OperationsPerInvocation(1)
  def benchmarkPipeline_8Partitions(state: SparkState): Unit = {
    benchmarkWithPartitions(state, 8)
  }

  /**
   * Helper to benchmark with specific partition count.
   */
  private def benchmarkWithPartitions(state: SparkState, partitions: Int): Unit = {
    implicit val spark: SparkSession = state.spark

    val extractor = new BenchmarkExtractor(10000)
    val transformers = Seq(new BenchmarkAggregationTransformer())
    val loader = new BenchmarkLoader()

    val pipeline = ETLPipeline(extractor, transformers, loader)

    val config = PipelineConfig(
      pipelineId = s"benchmark-pipeline-$partitions",
      name = s"Benchmark Pipeline $partitions partitions",
      extract = ExtractConfig(
        sourceType = SourceType.S3,
        path = Some("benchmark://test"),
        schemaName = "test",
        connectionParams = Map.empty
      ),
      transforms = Seq(
        TransformConfig(
          transformType = TransformType.Aggregation,
          parameters = Map.empty
        )
      ),
      load = LoadConfig(
        sinkType = SinkType.S3,
        path = Some("benchmark://output"),
        writeMode = "Overwrite",
        schemaName = "test",
        connectionParams = Map.empty
      ),
      performanceConfig = PerformanceConfig(
        shufflePartitions = Some(partitions)
      )
    )

    val context = ExecutionContext.create(spark, config)
    pipeline.run(context)
  }
}
