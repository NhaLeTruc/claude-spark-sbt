package integration

import com.etl.config.{DeltaConfig, ExtractConfig, LoadConfig, SourceType, SinkType}
import com.etl.extract.DeltaLakeExtractor
import com.etl.load.DeltaLakeLoader
import com.etl.model.WriteMode
import org.scalatest.funspec.AnyFunSpec

class DeltaLakePipelineIntegrationSpec extends IntegrationTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    DeltaConfig.configure(spark)
  }

  describe("Delta Lake End-to-End Upsert Pipeline") {
    it("should perform complete upsert workflow") {
      // Stage 1: Initial load to Delta
      val initialData = (1 to 100).map { i =>
        (i, s"user_$i", i * 10, "2024-01-01")
      }.toDF("id", "name", "amount", "date")

      val deltaPath = tempFilePath("users-delta")

      val loader = new DeltaLakeLoader()
      val loadConfig = LoadConfig(
        sinkType = SinkType.DeltaLake,
        connectionParams = Map(
          "path" -> deltaPath,
          "partitionBy" -> "[\"date\"]"
        ),
        table = None,
        topic = None,
        path = Some(deltaPath),
        writeMode = None,
        upsertKeys = None,
        schemaName = None,
        credentialId = None
      )

      val initialResult = loader.load(initialData, loadConfig, WriteMode.Overwrite)
      assert(initialResult.isSuccess)
      assert(initialResult.recordsProcessed == 100)

      // Stage 2: Upsert - Update 50 records, Insert 50 new
      val updates = (
        (51 to 100).map(i => (i, s"user_${i}_updated", i * 20, "2024-01-01")) ++
        (101 to 150).map(i => (i, s"user_$i", i * 10, "2024-01-02"))
      ).toDF("id", "name", "amount", "date")

      val upsertConfig = LoadConfig(
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

      val upsertResult = loader.load(updates, upsertConfig, WriteMode.Upsert)
      assert(upsertResult.isSuccess)
      assert(upsertResult.recordsProcessed == 100)

      // Stage 3: Verify results
      val finalData = spark.read.format("delta").load(deltaPath)
      assert(finalData.count() == 150)

      // Verify updates (id 51-100 should have updated names)
      val updated = finalData.filter("id >= 51 AND id <= 100")
      assert(updated.count() == 50)
      assert(updated.filter("name LIKE '%_updated'").count() == 50)

      // Verify inserts (id 101-150)
      val inserted = finalData.filter("id >= 101 AND id <= 150")
      assert(inserted.count() == 50)

      // Verify partitioning
      assert(finalData.filter("date = '2024-01-01'").count() == 100)
      assert(finalData.filter("date = '2024-01-02'").count() == 50)
    }
  }

  describe("Delta Lake Time Travel Integration") {
    it("should read historical versions correctly") {
      val deltaPath = tempFilePath("users-time-travel")

      // Version 0: 50 records
      val v0 = (1 to 50).map(i => (i, s"user_$i", i * 10)).toDF("id", "name", "amount")
      v0.write.format("delta").mode("overwrite").save(deltaPath)

      // Version 1: 100 records
      val v1 = (1 to 100).map(i => (i, s"user_$i", i * 10)).toDF("id", "name", "amount")
      v1.write.format("delta").mode("overwrite").save(deltaPath)

      // Version 2: 150 records
      val v2 = (1 to 150).map(i => (i, s"user_$i", i * 10)).toDF("id", "name", "amount")
      v2.write.format("delta").mode("overwrite").save(deltaPath)

      // Read version 0 (time travel)
      val extractor = new DeltaLakeExtractor()
      val extractConfig0 = ExtractConfig(
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

      val df0 = extractor.extractWithVault(extractConfig0, vault)
      assert(df0.count() == 50)

      // Read version 1
      val extractConfig1 = ExtractConfig(
        sourceType = SourceType.DeltaLake,
        connectionParams = Map(
          "path" -> deltaPath,
          "versionAsOf" -> "1"
        ),
        query = None,
        topic = None,
        path = Some(deltaPath),
        schemaName = "",
        credentialId = None
      )

      val df1 = extractor.extractWithVault(extractConfig1, vault)
      assert(df1.count() == 100)

      // Read latest (version 2)
      val extractConfigLatest = ExtractConfig(
        sourceType = SourceType.DeltaLake,
        connectionParams = Map("path" -> deltaPath),
        query = None,
        topic = None,
        path = Some(deltaPath),
        schemaName = "",
        credentialId = None
      )

      val dfLatest = extractor.extractWithVault(extractConfigLatest, vault)
      assert(dfLatest.count() == 150)
    }
  }

  describe("Delta Lake Schema Evolution Integration") {
    it("should handle schema evolution across pipeline runs") {
      val deltaPath = tempFilePath("users-schema-evolution")

      // Run 1: Write with 3 columns
      val df1 = (1 to 50).map(i => (i, s"user_$i", i * 10)).toDF("id", "name", "amount")

      val loader = new DeltaLakeLoader()
      val loadConfig1 = LoadConfig(
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

      val result1 = loader.load(df1, loadConfig1, WriteMode.Overwrite)
      assert(result1.isSuccess)

      // Run 2: Append with 4 columns (new column: country)
      val df2 = (51 to 100).map(i => (i, s"user_$i", i * 10, "US")).toDF("id", "name", "amount", "country")

      val loadConfig2 = LoadConfig(
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

      val result2 = loader.load(df2, loadConfig2, WriteMode.Append)
      assert(result2.isSuccess)

      // Verify merged schema
      val finalData = spark.read.format("delta").load(deltaPath)
      assert(finalData.count() == 100)
      assert(finalData.schema.fieldNames.contains("country"))

      // Old records should have null for new column
      assert(finalData.filter("id <= 50 AND country IS NULL").count() == 50)
      assert(finalData.filter("id > 50 AND country = 'US'").count() == 50)
    }
  }

  describe("Delta Lake Partition Overwrite Integration") {
    it("should overwrite specific partitions without affecting others") {
      val deltaPath = tempFilePath("users-partition-overwrite")

      // Initial data with 3 dates
      val initialData = Seq(
        (1, "Alice", "2024-01-01"),
        (2, "Bob", "2024-01-01"),
        (3, "Charlie", "2024-01-02"),
        (4, "David", "2024-01-02"),
        (5, "Eve", "2024-01-03")
      ).toDF("id", "name", "date")

      initialData.write.format("delta").partitionBy("date").mode("overwrite").save(deltaPath)

      // Overwrite only 2024-01-02 partition
      val newData = Seq(
        (10, "Frank", "2024-01-02"),
        (11, "Grace", "2024-01-02")
      ).toDF("id", "name", "date")

      val loader = new DeltaLakeLoader()
      val loadConfig = LoadConfig(
        sinkType = SinkType.DeltaLake,
        connectionParams = Map(
          "path" -> deltaPath,
          "replaceWhere" -> "date = '2024-01-02'"
        ),
        table = None,
        topic = None,
        path = Some(deltaPath),
        writeMode = None,
        upsertKeys = None,
        schemaName = None,
        credentialId = None
      )

      val result = loader.load(newData, loadConfig, WriteMode.Overwrite)
      assert(result.isSuccess)

      // Verify results
      val finalData = spark.read.format("delta").load(deltaPath)
      assert(finalData.count() == 5)  // 2 from 2024-01-01 + 2 from new 2024-01-02 + 1 from 2024-01-03

      // 2024-01-01 should be unchanged
      assert(finalData.filter("date = '2024-01-01'").count() == 2)
      assert(finalData.filter("date = '2024-01-01' AND id IN (1, 2)").count() == 2)

      // 2024-01-02 should be replaced
      assert(finalData.filter("date = '2024-01-02'").count() == 2)
      assert(finalData.filter("date = '2024-01-02' AND id IN (10, 11)").count() == 2)

      // 2024-01-03 should be unchanged
      assert(finalData.filter("date = '2024-01-03'").count() == 1)
    }
  }

  describe("Delta Lake CDC Integration") {
    it("should track changes with change data feed") {
      val deltaPath = tempFilePath("users-cdc")

      // Create table with CDC enabled
      val initialData = Seq((1, "Alice", 100), (2, "Bob", 200))
        .toDF("id", "name", "amount")

      initialData.write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .mode("overwrite")
        .save(deltaPath)

      // Make changes (version 1)
      val updates = Seq((1, "Alice", 150), (3, "Charlie", 300))
        .toDF("id", "name", "amount")

      updates.write
        .format("delta")
        .option("delta.enableChangeDataFeed", "true")
        .mode("overwrite")
        .save(deltaPath)

      // Read change feed
      val extractor = new DeltaLakeExtractor()
      val extractConfig = ExtractConfig(
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

      val changes = extractor.extractWithVault(extractConfig, vault)

      // Should have CDC metadata
      assert(changes.schema.fieldNames.contains("_change_type"))
      assert(changes.count() > 0)
    }
  }

  describe("Delta Lake Performance Features") {
    it("should support concurrent reads and writes") {
      val deltaPath = tempFilePath("users-concurrent")

      // Initial data
      val initialData = (1 to 1000).map(i => (i, s"user_$i", i * 10))
        .toDF("id", "name", "amount")
      initialData.write.format("delta").mode("overwrite").save(deltaPath)

      // Concurrent operations
      import scala.concurrent.{Await, Future}
      import scala.concurrent.ExecutionContext.Implicits.global
      import scala.concurrent.duration._

      val readFuture = Future {
        val extractor = new DeltaLakeExtractor()
        val extractConfig = ExtractConfig(
          sourceType = SourceType.DeltaLake,
          connectionParams = Map("path" -> deltaPath),
          query = None,
          topic = None,
          path = Some(deltaPath),
          schemaName = "",
          credentialId = None
        )
        extractor.extractWithVault(extractConfig, vault).count()
      }

      val writeFuture = Future {
        Thread.sleep(100)  // Small delay to ensure read starts first
        val updates = (1001 to 1100).map(i => (i, s"user_$i", i * 10))
          .toDF("id", "name", "amount")

        val loader = new DeltaLakeLoader()
        val loadConfig = LoadConfig(
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
        loader.load(updates, loadConfig, WriteMode.Append)
      }

      // Both should complete successfully
      val readCount = Await.result(readFuture, 10.seconds)
      val writeResult = Await.result(writeFuture, 10.seconds)

      assert(readCount == 1000)  // Read before append
      assert(writeResult.isSuccess)

      // Final count should be 1100
      val finalCount = spark.read.format("delta").load(deltaPath).count()
      assert(finalCount == 1100)
    }
  }
}
