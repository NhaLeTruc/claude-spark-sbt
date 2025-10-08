package unit.load

import com.etl.config.{DeltaConfig, LoadConfig, SinkType}
import com.etl.load.DeltaLakeLoader
import com.etl.model.WriteMode
import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.nio.file.{Files, Path}
import scala.util.Try

class DeltaLakeLoaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  implicit var spark: SparkSession = _
  var tempDir: Path = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("DeltaLakeLoaderTest")
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
    tempDir = Files.createTempDirectory("delta-loader-test-")
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

  behavior of "DeltaLakeLoader - Append Mode"

  it should "append records to a new Delta table" in {
    val loader = new DeltaLakeLoader()
    val deltaPath = tempDir.resolve("users-append").toString

    val df = Seq((1, "Alice", 100), (2, "Bob", 200))
      .toDF("id", "name", "amount")

    val config = LoadConfig(
      sinkType = SinkType.DeltaLake,
      connectionParams = Map("path" -> deltaPath),
      table = None,
      topic = None,
      path = Some(deltaPath),
      writeMode = None,
      upsertKeys = None,
      schemaName = None,
      credentialId = None
    )

    val result = loader.load(df, config, WriteMode.Append)

    result.isSuccess shouldBe true
    result.recordsProcessed shouldBe 2

    val loaded = spark.read.format("delta").load(deltaPath)
    loaded.count() shouldBe 2
  }

  it should "append records to an existing Delta table" in {
    val loader = new DeltaLakeLoader()
    val deltaPath = tempDir.resolve("users-append-existing").toString

    // Initial write
    val df1 = Seq((1, "Alice", 100)).toDF("id", "name", "amount")
    df1.write.format("delta").mode("overwrite").save(deltaPath)

    // Append
    val df2 = Seq((2, "Bob", 200), (3, "Charlie", 300)).toDF("id", "name", "amount")
    val config = LoadConfig(
      sinkType = SinkType.DeltaLake,
      connectionParams = Map("path" -> deltaPath),
      table = None,
      topic = None,
      path = Some(deltaPath),
      writeMode = None,
      upsertKeys = None,
      schemaName = None,
      credentialId = None
    )

    val result = loader.load(df2, config, WriteMode.Append)

    result.isSuccess shouldBe true
    result.recordsProcessed shouldBe 2

    val loaded = spark.read.format("delta").load(deltaPath)
    loaded.count() shouldBe 3
  }

  it should "handle schema evolution during append" in {
    val loader = new DeltaLakeLoader()
    val deltaPath = tempDir.resolve("users-schema-evolution").toString

    // Initial write with 2 columns
    val df1 = Seq((1, "Alice")).toDF("id", "name")
    df1.write.format("delta").mode("overwrite").save(deltaPath)

    // Append with 3 columns (new column: amount)
    val df2 = Seq((2, "Bob", 200)).toDF("id", "name", "amount")
    val config = LoadConfig(
      sinkType = SinkType.DeltaLake,
      connectionParams = Map(
        "path" -> deltaPath,
        "mergeSchema" -> "true"
      ),
      table = None,
      topic = None,
      path = Some(deltaPath),
      writeMode = None,
      upsertKeys = None,
      schemaName = None,
      credentialId = None
    )

    val result = loader.load(df2, config, WriteMode.Append)

    result.isSuccess shouldBe true

    val loaded = spark.read.format("delta").load(deltaPath)
    loaded.count() shouldBe 2
    loaded.schema.fieldNames should contain ("amount")
  }

  behavior of "DeltaLakeLoader - Overwrite Mode"

  it should "overwrite entire Delta table" in {
    val loader = new DeltaLakeLoader()
    val deltaPath = tempDir.resolve("users-overwrite").toString

    // Initial write
    val df1 = Seq((1, "Alice", 100), (2, "Bob", 200)).toDF("id", "name", "amount")
    df1.write.format("delta").mode("overwrite").save(deltaPath)

    // Overwrite
    val df2 = Seq((3, "Charlie", 300)).toDF("id", "name", "amount")
    val config = LoadConfig(
      sinkType = SinkType.DeltaLake,
      connectionParams = Map("path" -> deltaPath),
      table = None,
      topic = None,
      path = Some(deltaPath),
      writeMode = None,
      upsertKeys = None,
      schemaName = None,
      credentialId = None
    )

    val result = loader.load(df2, config, WriteMode.Overwrite)

    result.isSuccess shouldBe true
    result.recordsProcessed shouldBe 1

    val loaded = spark.read.format("delta").load(deltaPath)
    loaded.count() shouldBe 1
    loaded.select("id").as[Int].head() shouldBe 3
  }

  it should "overwrite specific partitions using replaceWhere" in {
    val loader = new DeltaLakeLoader()
    val deltaPath = tempDir.resolve("users-partition-overwrite").toString

    // Initial write with 2 dates
    val df1 = Seq(
      (1, "Alice", "2024-01-01"),
      (2, "Bob", "2024-01-01"),
      (3, "Charlie", "2024-01-02")
    ).toDF("id", "name", "date")
    df1.write.format("delta").partitionBy("date").mode("overwrite").save(deltaPath)

    // Overwrite only 2024-01-01 partition
    val df2 = Seq((4, "David", "2024-01-01")).toDF("id", "name", "date")
    val config = LoadConfig(
      sinkType = SinkType.DeltaLake,
      connectionParams = Map(
        "path" -> deltaPath,
        "replaceWhere" -> "date = '2024-01-01'"
      ),
      table = None,
      topic = None,
      path = Some(deltaPath),
      writeMode = None,
      upsertKeys = None,
      schemaName = None,
      credentialId = None
    )

    val result = loader.load(df2, config, WriteMode.Overwrite)

    result.isSuccess shouldBe true

    val loaded = spark.read.format("delta").load(deltaPath)
    loaded.count() shouldBe 2  // 1 from 2024-01-01 + 1 from 2024-01-02
    loaded.filter("date = '2024-01-01'").count() shouldBe 1
    loaded.filter("date = '2024-01-01'").select("id").as[Int].head() shouldBe 4
  }

  behavior of "DeltaLakeLoader - Upsert Mode"

  it should "insert new records when table doesn't exist" in {
    val loader = new DeltaLakeLoader()
    val deltaPath = tempDir.resolve("users-upsert-new").toString

    val df = Seq((1, "Alice", 100), (2, "Bob", 200)).toDF("id", "name", "amount")
    val config = LoadConfig(
      sinkType = SinkType.DeltaLake,
      connectionParams = Map(
        "path" -> deltaPath,
        "mergeKeys" -> "[\"id\"]"
      ),
      table = None,
      topic = None,
      path = Some(deltaPath),
      writeMode = None,
      upsertKeys = None,
      schemaName = None,
      credentialId = None
    )

    val result = loader.load(df, config, WriteMode.Upsert)

    result.isSuccess shouldBe true
    result.recordsProcessed shouldBe 2

    val loaded = spark.read.format("delta").load(deltaPath)
    loaded.count() shouldBe 2
  }

  it should "perform upsert with single merge key" in {
    val loader = new DeltaLakeLoader()
    val deltaPath = tempDir.resolve("users-upsert").toString

    // Initial data
    val df1 = Seq((1, "Alice", 100), (2, "Bob", 200)).toDF("id", "name", "amount")
    df1.write.format("delta").mode("overwrite").save(deltaPath)

    // Upsert: Update Alice (id=1), Insert Charlie (id=3)
    val df2 = Seq((1, "Alice", 150), (3, "Charlie", 300)).toDF("id", "name", "amount")
    val config = LoadConfig(
      sinkType = SinkType.DeltaLake,
      connectionParams = Map(
        "path" -> deltaPath,
        "mergeKeys" -> "[\"id\"]"
      ),
      table = None,
      topic = None,
      path = Some(deltaPath),
      writeMode = None,
      upsertKeys = None,
      schemaName = None,
      credentialId = None
    )

    val result = loader.load(df2, config, WriteMode.Upsert)

    result.isSuccess shouldBe true
    result.recordsProcessed shouldBe 2

    val loaded = spark.read.format("delta").load(deltaPath)
    loaded.count() shouldBe 3

    // Verify update
    val alice = loaded.filter("id = 1").select("amount").as[Int].head()
    alice shouldBe 150

    // Verify insert
    loaded.filter("id = 3").count() shouldBe 1
  }

  it should "perform upsert with composite merge keys" in {
    val loader = new DeltaLakeLoader()
    val deltaPath = tempDir.resolve("users-composite-upsert").toString

    // Initial data
    val df1 = Seq(
      (1, "2024-01-01", 100),
      (1, "2024-01-02", 200),
      (2, "2024-01-01", 300)
    ).toDF("user_id", "date", "amount")
    df1.write.format("delta").mode("overwrite").save(deltaPath)

    // Upsert: Update (1, 2024-01-01), Insert (3, 2024-01-01)
    val df2 = Seq(
      (1, "2024-01-01", 150),
      (3, "2024-01-01", 400)
    ).toDF("user_id", "date", "amount")

    val config = LoadConfig(
      sinkType = SinkType.DeltaLake,
      connectionParams = Map(
        "path" -> deltaPath,
        "mergeKeys" -> "[\"user_id\", \"date\"]"
      ),
      table = None,
      topic = None,
      path = Some(deltaPath),
      writeMode = None,
      upsertKeys = None,
      schemaName = None,
      credentialId = None
    )

    val result = loader.load(df2, config, WriteMode.Upsert)

    result.isSuccess shouldBe true

    val loaded = spark.read.format("delta").load(deltaPath)
    loaded.count() shouldBe 4

    // Verify update
    val updated = loaded.filter("user_id = 1 AND date = '2024-01-01'").select("amount").as[Int].head()
    updated shouldBe 150

    // Verify insert
    loaded.filter("user_id = 3").count() shouldBe 1
  }

  it should "perform conditional upsert with updateCondition" in {
    val loader = new DeltaLakeLoader()
    val deltaPath = tempDir.resolve("users-conditional-upsert").toString

    // Initial data with timestamps
    val df1 = Seq(
      (1, "Alice", 100, "2024-01-01 10:00:00"),
      (2, "Bob", 200, "2024-01-01 10:00:00")
    ).toDF("id", "name", "amount", "timestamp")
    df1.write.format("delta").mode("overwrite").save(deltaPath)

    // Try to update with older timestamp (should not update)
    // and newer timestamp (should update)
    val df2 = Seq(
      (1, "Alice", 150, "2024-01-01 09:00:00"),  // Older - should not update
      (2, "Bob", 250, "2024-01-01 11:00:00")     // Newer - should update
    ).toDF("id", "name", "amount", "timestamp")

    val config = LoadConfig(
      sinkType = SinkType.DeltaLake,
      connectionParams = Map(
        "path" -> deltaPath,
        "mergeKeys" -> "[\"id\"]",
        "updateCondition" -> "source.timestamp > target.timestamp"
      ),
      table = None,
      topic = None,
      path = Some(deltaPath),
      writeMode = None,
      upsertKeys = None,
      schemaName = None,
      credentialId = None
    )

    val result = loader.load(df2, config, WriteMode.Upsert)

    result.isSuccess shouldBe true

    val loaded = spark.read.format("delta").load(deltaPath)

    // Alice should NOT be updated (older timestamp)
    val alice = loaded.filter("id = 1").select("amount").as[Int].head()
    alice shouldBe 100

    // Bob should be updated (newer timestamp)
    val bob = loaded.filter("id = 2").select("amount").as[Int].head()
    bob shouldBe 250
  }

  behavior of "DeltaLakeLoader - Error Handling"

  it should "fail gracefully when mergeKeys are missing for Upsert" in {
    val loader = new DeltaLakeLoader()
    val deltaPath = tempDir.resolve("users-no-keys").toString

    val df = Seq((1, "Alice", 100)).toDF("id", "name", "amount")
    val config = LoadConfig(
      sinkType = SinkType.DeltaLake,
      connectionParams = Map("path" -> deltaPath),  // No mergeKeys!
      table = None,
      topic = None,
      path = Some(deltaPath),
      writeMode = None,
      upsertKeys = None,
      schemaName = None,
      credentialId = None
    )

    val result = loader.load(df, config, WriteMode.Upsert)

    result.isSuccess shouldBe false
    result.error.get should include("mergeKeys parameter is required")
  }

  it should "return failure result on write exception" in {
    val loader = new DeltaLakeLoader()
    val invalidPath = "/invalid/path/delta"  // Invalid path

    val df = Seq((1, "Alice", 100)).toDF("id", "name", "amount")
    val config = LoadConfig(
      sinkType = SinkType.DeltaLake,
      connectionParams = Map("path" -> invalidPath),
      table = None,
      topic = None,
      path = Some(invalidPath),
      writeMode = None,
      upsertKeys = None,
      schemaName = None,
      credentialId = None
    )

    val result = loader.load(df, config, WriteMode.Append)

    result.isSuccess shouldBe false
    result.recordsProcessed shouldBe 0
    result.error shouldBe defined
  }

  behavior of "DeltaLakeLoader - Optimizations"

  it should "support partitioning" in {
    val loader = new DeltaLakeLoader()
    val deltaPath = tempDir.resolve("users-partitioned").toString

    val df = Seq(
      (1, "Alice", "US"),
      (2, "Bob", "UK"),
      (3, "Charlie", "US")
    ).toDF("id", "name", "country")

    val config = LoadConfig(
      sinkType = SinkType.DeltaLake,
      connectionParams = Map(
        "path" -> deltaPath,
        "partitionBy" -> "[\"country\"]"
      ),
      table = None,
      topic = None,
      path = Some(deltaPath),
      writeMode = None,
      upsertKeys = None,
      schemaName = None,
      credentialId = None
    )

    val result = loader.load(df, config, WriteMode.Append)

    result.isSuccess shouldBe true

    // Verify partitioning
    val deltaTable = DeltaTable.forPath(spark, deltaPath)
    val partitionCols = deltaTable.toDF.schema.fieldNames.contains("country")
    partitionCols shouldBe true
  }
}
