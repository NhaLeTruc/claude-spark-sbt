package unit.extract

import com.etl.config.{DeltaConfig, ExtractConfig, InMemoryVault, SourceType}
import com.etl.extract.DeltaLakeExtractor
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.nio.file.{Files, Path}

class DeltaLakeExtractorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  implicit var spark: SparkSession = _
  var tempDir: Path = _
  val vault = InMemoryVault()

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("DeltaLakeExtractorTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      .getOrCreate()

    DeltaConfig.configure(spark)
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  override def beforeEach(): Unit = {
    tempDir = Files.createTempDirectory("delta-extractor-test-")
  }

  override def afterEach(): Unit = {
    if (tempDir != null) {
      deleteDirectory(tempDir.toFile)
    }
  }

  private def deleteDirectory(dir: java.io.File): Unit = {
    if (dir.exists()) {
      if (dir.isDirectory) {
        dir.listFiles().foreach(deleteDirectory)
      }
      dir.delete()
    }
  }

  behavior of "DeltaLakeExtractor - Latest Version"

  it should "read latest version from Delta table" in {
    val extractor = new DeltaLakeExtractor()
    val deltaPath = tempDir.resolve("users-latest").toString

    // Create Delta table
    val df = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    df.write.format("delta").mode("overwrite").save(deltaPath)

    val config = ExtractConfig(
      sourceType = SourceType.DeltaLake,
      connectionParams = Map("path" -> deltaPath),
      query = None,
      topic = None,
      path = Some(deltaPath),
      schemaName = "",
      credentialId = None
    )

    val result = extractor.extractWithVault(config, vault)

    result.count() shouldBe 2
    result.schema.fieldNames should contain allOf ("id", "name")
  }

  it should "read from partitioned Delta table" in {
    val extractor = new DeltaLakeExtractor()
    val deltaPath = tempDir.resolve("users-partitioned").toString

    // Create partitioned Delta table
    val df = Seq(
      (1, "Alice", "US"),
      (2, "Bob", "UK"),
      (3, "Charlie", "US")
    ).toDF("id", "name", "country")
    df.write.format("delta").partitionBy("country").mode("overwrite").save(deltaPath)

    val config = ExtractConfig(
      sourceType = SourceType.DeltaLake,
      connectionParams = Map("path" -> deltaPath),
      query = None,
      topic = None,
      path = Some(deltaPath),
      schemaName = "",
      credentialId = None
    )

    val result = extractor.extractWithVault(config, vault)

    result.count() shouldBe 3
  }

  behavior of "DeltaLakeExtractor - Time Travel"

  it should "read specific version using versionAsOf" in {
    val extractor = new DeltaLakeExtractor()
    val deltaPath = tempDir.resolve("users-versions").toString

    // Create version 0
    val df1 = Seq((1, "Alice")).toDF("id", "name")
    df1.write.format("delta").mode("overwrite").save(deltaPath)

    // Create version 1
    val df2 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    df2.write.format("delta").mode("overwrite").save(deltaPath)

    // Create version 2
    val df3 = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")
    df3.write.format("delta").mode("overwrite").save(deltaPath)

    // Read version 0 (time travel)
    val config = ExtractConfig(
      sourceType = SourceType.DeltaLake,
      connectionParams = Map(
        "path" -> deltaPath,
        "versionAsOf" -> "0"
      ),
      query = None,
      topic = None,
      path = Some(deltaPath),
      schemaName = "",
      credentialId = None
    )

    val result = extractor.extractWithVault(config, vault)

    result.count() shouldBe 1
    result.select("name").as[String].head() shouldBe "Alice"
  }

  it should "read specific timestamp using timestampAsOf" in {
    val extractor = new DeltaLakeExtractor()
    val deltaPath = tempDir.resolve("users-timestamp").toString

    // Create version at specific time
    val df1 = Seq((1, "Alice")).toDF("id", "name")
    df1.write.format("delta").mode("overwrite").save(deltaPath)
    val timestamp1 = getCurrentTimestamp()

    // Wait a bit
    Thread.sleep(1000)

    // Create another version
    val df2 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    df2.write.format("delta").mode("overwrite").save(deltaPath)

    // Read as of first timestamp
    val config = ExtractConfig(
      sourceType = SourceType.DeltaLake,
      connectionParams = Map(
        "path" -> deltaPath,
        "timestampAsOf" -> timestamp1
      ),
      query = None,
      topic = None,
      path = Some(deltaPath),
      schemaName = "",
      credentialId = None
    )

    val result = extractor.extractWithVault(config, vault)

    result.count() shouldBe 1
  }

  it should "fail when both versionAsOf and timestampAsOf are specified" in {
    val extractor = new DeltaLakeExtractor()
    val deltaPath = tempDir.resolve("users-both").toString

    // Create table
    val df = Seq((1, "Alice")).toDF("id", "name")
    df.write.format("delta").mode("overwrite").save(deltaPath)

    val config = ExtractConfig(
      sourceType = SourceType.DeltaLake,
      connectionParams = Map(
        "path" -> deltaPath,
        "versionAsOf" -> "0",
        "timestampAsOf" -> "2024-01-01 00:00:00"
      ),
      query = None,
      topic = None,
      path = Some(deltaPath),
      schemaName = "",
      credentialId = None
    )

    an[IllegalArgumentException] should be thrownBy {
      extractor.extractWithVault(config, vault)
    }
  }

  behavior of "DeltaLakeExtractor - Change Data Feed"

  it should "read change data feed from specific version" in {
    val extractor = new DeltaLakeExtractor()
    val deltaPath = tempDir.resolve("users-cdf").toString

    // Create table with CDC enabled
    val df1 = Seq((1, "Alice", 100)).toDF("id", "name", "amount")
    df1.write
      .format("delta")
      .option("delta.enableChangeDataFeed", "true")
      .mode("overwrite")
      .save(deltaPath)

    // Make some changes (version 1)
    val df2 = Seq((1, "Alice", 150), (2, "Bob", 200)).toDF("id", "name", "amount")
    df2.write
      .format("delta")
      .option("delta.enableChangeDataFeed", "true")
      .mode("overwrite")
      .save(deltaPath)

    // Read change feed from version 1
    val config = ExtractConfig(
      sourceType = SourceType.DeltaLake,
      connectionParams = Map(
        "path" -> deltaPath,
        "readChangeFeed" -> "true",
        "startingVersion" -> "1"
      ),
      query = None,
      topic = None,
      path = Some(deltaPath),
      schemaName = "",
      credentialId = None
    )

    val result = extractor.extractWithVault(config, vault)

    // Change feed should have records
    result.count() should be > 0L

    // Should have CDC metadata columns
    result.schema.fieldNames should contain ("_change_type")
  }

  it should "read change feed with version range" in {
    val extractor = new DeltaLakeExtractor()
    val deltaPath = tempDir.resolve("users-cdf-range").toString

    // Create table with CDC enabled
    val df1 = Seq((1, "Alice")).toDF("id", "name")
    df1.write
      .format("delta")
      .option("delta.enableChangeDataFeed", "true")
      .mode("overwrite")
      .save(deltaPath)  // Version 0

    val df2 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
    df2.write
      .format("delta")
      .option("delta.enableChangeDataFeed", "true")
      .mode("overwrite")
      .save(deltaPath)  // Version 1

    val df3 = Seq((1, "Alice"), (2, "Bob"), (3, "Charlie")).toDF("id", "name")
    df3.write
      .format("delta")
      .option("delta.enableChangeDataFeed", "true")
      .mode("overwrite")
      .save(deltaPath)  // Version 2

    // Read change feed from version 1 to 2
    val config = ExtractConfig(
      sourceType = SourceType.DeltaLake,
      connectionParams = Map(
        "path" -> deltaPath,
        "readChangeFeed" -> "true",
        "startingVersion" -> "1",
        "endingVersion" -> "2"
      ),
      query = None,
      topic = None,
      path = Some(deltaPath),
      schemaName = "",
      credentialId = None
    )

    val result = extractor.extractWithVault(config, vault)

    result.count() should be > 0L
  }

  behavior of "DeltaLakeExtractor - Error Handling"

  it should "handle missing path parameter" in {
    val extractor = new DeltaLakeExtractor()

    val config = ExtractConfig(
      sourceType = SourceType.DeltaLake,
      connectionParams = Map.empty,  // No path!
      query = None,
      topic = None,
      path = None,
      schemaName = "",
      credentialId = None
    )

    an[IllegalArgumentException] should be thrownBy {
      extractor.extractWithVault(config, vault)
    }
  }

  it should "use legacy extract method with empty vault" in {
    val extractor = new DeltaLakeExtractor()
    val deltaPath = tempDir.resolve("users-legacy").toString

    // Create table
    val df = Seq((1, "Alice")).toDF("id", "name")
    df.write.format("delta").mode("overwrite").save(deltaPath)

    val config = ExtractConfig(
      sourceType = SourceType.DeltaLake,
      connectionParams = Map("path" -> deltaPath),
      query = None,
      topic = None,
      path = Some(deltaPath),
      schemaName = "",
      credentialId = None
    )

    val result = extractor.extract(config)

    result.count() shouldBe 1
  }

  behavior of "DeltaLakeExtractor - Schema Evolution"

  it should "handle schema evolution across versions" in {
    val extractor = new DeltaLakeExtractor()
    val deltaPath = tempDir.resolve("users-schema-evolution").toString

    // Create version 0 with 2 columns
    val df1 = Seq((1, "Alice")).toDF("id", "name")
    df1.write.format("delta").mode("overwrite").save(deltaPath)

    // Create version 1 with 3 columns (added amount)
    val df2 = Seq((1, "Alice", 100), (2, "Bob", 200)).toDF("id", "name", "amount")
    df2.write
      .format("delta")
      .option("mergeSchema", "true")
      .mode("overwrite")
      .save(deltaPath)

    // Read version 0 (should have old schema)
    val config0 = ExtractConfig(
      sourceType = SourceType.DeltaLake,
      connectionParams = Map(
        "path" -> deltaPath,
        "versionAsOf" -> "0"
      ),
      query = None,
      topic = None,
      path = Some(deltaPath),
      schemaName = "",
      credentialId = None
    )

    val result0 = extractor.extractWithVault(config0, vault)
    result0.schema.fieldNames should not contain "amount"

    // Read latest (should have new schema)
    val config1 = ExtractConfig(
      sourceType = SourceType.DeltaLake,
      connectionParams = Map("path" -> deltaPath),
      query = None,
      topic = None,
      path = Some(deltaPath),
      schemaName = "",
      credentialId = None
    )

    val result1 = extractor.extractWithVault(config1, vault)
    result1.schema.fieldNames should contain ("amount")
  }

  private def getCurrentTimestamp(): String = {
    import java.time.LocalDateTime
    import java.time.format.DateTimeFormatter

    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
    LocalDateTime.now().format(formatter)
  }
}
