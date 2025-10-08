package integration

import com.etl.config.CredentialVault
import com.etl.core.ExecutionContext
import com.etl.model.ExecutionMetrics
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.{Files, Path, Paths}
import scala.util.{Failure, Success, Try}

/**
 * Base class for integration tests.
 *
 * Provides:
 * - SparkSession setup/teardown
 * - Temporary directory management
 * - Test data helpers
 * - Embedded service management
 */
abstract class IntegrationTestBase extends AnyFunSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  // SparkSession for tests (local mode)
  protected var spark: SparkSession = _

  // Temporary directory for test data
  protected var tempDir: Path = _

  // Credential vault for tests
  protected var vault: CredentialVault = _

  /**
   * Setup SparkSession before all tests.
   */
  override def beforeAll(): Unit = {
    super.beforeAll()

    spark = SparkSession.builder()
      .appName("Integration Test")
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // Create test credential vault
    vault = createTestVault()
  }

  /**
   * Teardown SparkSession after all tests.
   */
  override def afterAll(): Unit = {
    try {
      if (spark != null) {
        spark.stop()
      }
    } finally {
      super.afterAll()
    }
  }

  /**
   * Create temporary directory before each test.
   */
  override def beforeEach(): Unit = {
    super.beforeEach()
    tempDir = Files.createTempDirectory("etl-test-")
  }

  /**
   * Clean up temporary directory after each test.
   */
  override def afterEach(): Unit = {
    try {
      if (tempDir != null) {
        deleteDirectory(tempDir.toFile)
      }
    } finally {
      super.afterEach()
    }
  }

  /**
   * Create a test credential vault with sample credentials.
   */
  protected def createTestVault(): CredentialVault = {
    import com.etl.config.InMemoryVault

    InMemoryVault(
      "test-postgres-password" -> "test-password",
      "test-mysql-password" -> "test-password",
      "test-kafka-username" -> "test-user",
      "test-kafka-password" -> "test-password",
      "test-s3-access" -> "test-access-key",
      "test-s3-secret" -> "test-secret-key"
    )
  }

  /**
   * Create a test execution context.
   */
  protected def createTestContext(
    pipelineId: String,
    config: com.etl.config.PipelineConfig
  ): ExecutionContext = {
    val metrics = ExecutionMetrics.initial(pipelineId, java.util.UUID.randomUUID().toString)
    ExecutionContext(spark, config, vault, metrics)
  }

  /**
   * Create a temporary file path in the test directory.
   */
  protected def tempFilePath(name: String): String = {
    tempDir.resolve(name).toString
  }

  /**
   * Write text content to a file.
   */
  protected def writeFile(path: String, content: String): Unit = {
    import java.nio.file.StandardOpenOption
    Files.write(Paths.get(path), content.getBytes("UTF-8"), StandardOpenOption.CREATE)
  }

  /**
   * Read text content from a file.
   */
  protected def readFile(path: String): String = {
    new String(Files.readAllBytes(Paths.get(path)), "UTF-8")
  }

  /**
   * Recursively delete a directory.
   */
  protected def deleteDirectory(dir: File): Unit = {
    Try {
      if (dir.exists()) {
        if (dir.isDirectory) {
          dir.listFiles().foreach(deleteDirectory)
        }
        dir.delete()
      }
    } match {
      case Success(_) => // Successfully deleted
      case Failure(e) => println(s"Warning: Failed to delete ${dir.getPath}: ${e.getMessage}")
    }
  }

  /**
   * Wait for a condition to become true (with timeout).
   */
  protected def waitFor(
    condition: => Boolean,
    timeoutMs: Long = 10000,
    intervalMs: Long = 100,
    message: String = "Condition not met"
  ): Unit = {
    val startTime = System.currentTimeMillis()
    while (!condition && (System.currentTimeMillis() - startTime) < timeoutMs) {
      Thread.sleep(intervalMs)
    }
    if (!condition) {
      fail(s"Timeout waiting for: $message")
    }
  }

  /**
   * Create sample test data as a DataFrame.
   */
  protected def createSampleData(rows: Int = 100): org.apache.spark.sql.DataFrame = {
    import spark.implicits._

    val data = (1 to rows).map { i =>
      (i, s"user_$i", i * 10.0, java.sql.Timestamp.valueOf(s"2024-01-01 00:00:${i % 60}"))
    }

    data.toDF("id", "name", "value", "timestamp")
  }

  /**
   * Assert DataFrame equality (schema and data).
   */
  protected def assertDataFrameEquals(
    actual: org.apache.spark.sql.DataFrame,
    expected: org.apache.spark.sql.DataFrame,
    message: String = "DataFrames are not equal"
  ): Unit = {
    // Check schema
    actual.schema should equal(expected.schema)

    // Check data (sorted for deterministic comparison)
    val actualData = actual.collect().sortBy(_.toString())
    val expectedData = expected.collect().sortBy(_.toString())

    actualData should equal(expectedData)
  }

  /**
   * Count rows in a DataFrame.
   */
  protected def countRows(df: org.apache.spark.sql.DataFrame): Long = {
    df.count()
  }
}
