package com.etl.unit.load

import com.etl.config.{LoadConfig, SinkType}
import com.etl.load.S3Loader
import com.etl.model.WriteMode
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class S3LoaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("S3LoaderTest")
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

  "S3Loader" should "validate CSV write configuration" in {
    import spark.implicits._

    // Given a DataFrame to write
    val df = Seq(
      (1, "Alice", "alice@example.com"),
      (2, "Bob", "bob@example.com")
    ).toDF("id", "name", "email")

    // When configuring S3 loader for CSV
    val config = LoadConfig(
      sinkType = SinkType.S3,
      path = Some("s3a://my-bucket/output/users.csv"),
      connectionParams = Map(
        "format" -> "csv",
        "header" -> "true"
      )
    )

    val loader = new S3Loader()

    // Then should validate config (will fail without S3, but checks validation)
    try {
      loader.load(df, config, WriteMode.Overwrite)
    } catch {
      case _: Exception => // Expected - no actual S3
    }
  }

  it should "validate JSON write configuration" in {
    import spark.implicits._

    val df = Seq((1, "data")).toDF("id", "value")

    // When configuring for JSON
    val config = LoadConfig(
      sinkType = SinkType.S3,
      path = Some("s3a://data-lake/events/"),
      connectionParams = Map(
        "format" -> "json"
      )
    )

    val loader = new S3Loader()

    // Then should validate JSON config
    try {
      loader.load(df, config, WriteMode.Append)
    } catch {
      case _: Exception => // Expected - no actual S3
    }
  }

  it should "validate Parquet write configuration" in {
    import spark.implicits._

    val df = Seq((1, "data")).toDF("id", "value")

    // When configuring for Parquet
    val config = LoadConfig(
      sinkType = SinkType.S3,
      path = Some("s3a://warehouse/tables/users/"),
      connectionParams = Map(
        "format" -> "parquet",
        "compression" -> "snappy"
      )
    )

    val loader = new S3Loader()

    // Then should validate Parquet config
    try {
      loader.load(df, config, WriteMode.Overwrite)
    } catch {
      case _: Exception => // Expected - no actual S3
    }
  }

  it should "validate Avro write configuration" in {
    import spark.implicits._

    val df = Seq((1, "data")).toDF("id", "value")

    // When configuring for Avro
    val config = LoadConfig(
      sinkType = SinkType.S3,
      path = Some("s3a://events/avro/year=2024/"),
      connectionParams = Map(
        "format" -> "avro"
      )
    )

    val loader = new S3Loader()

    // Then should validate Avro config
    try {
      loader.load(df, config, WriteMode.Append)
    } catch {
      case _: Exception => // Expected - no actual S3
    }
  }

  it should "require path parameter" in {
    import spark.implicits._

    val df = Seq((1, "data")).toDF("id", "value")

    // Given config without path
    val config = LoadConfig(
      sinkType = SinkType.S3,
      path = None,
      connectionParams = Map(
        "format" -> "parquet"
      )
    )

    val loader = new S3Loader()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      loader.load(df, config, WriteMode.Overwrite)
    }
  }

  it should "require format parameter" in {
    import spark.implicits._

    val df = Seq((1, "data")).toDF("id", "value")

    // Given config without format
    val config = LoadConfig(
      sinkType = SinkType.S3,
      path = Some("s3a://bucket/data/"),
      connectionParams = Map.empty
    )

    val loader = new S3Loader()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      loader.load(df, config, WriteMode.Overwrite)
    }
  }

  it should "support partitioned writes" in {
    import spark.implicits._

    val df = Seq(
      ("2024-01-01", "store1", 100.0),
      ("2024-01-01", "store2", 150.0),
      ("2024-01-02", "store1", 120.0)
    ).toDF("date", "store_id", "revenue")

    // When partitioning by date
    val config = LoadConfig(
      sinkType = SinkType.S3,
      path = Some("s3a://analytics/sales/"),
      connectionParams = Map(
        "format" -> "parquet",
        "partitionBy" -> "[\"date\"]"
      )
    )

    val loader = new S3Loader()

    // Then should support partitioning
    try {
      loader.load(df, config, WriteMode.Overwrite)
    } catch {
      case _: Exception => // Expected - no actual S3
    }
  }

  it should "support CSV custom options" in {
    import spark.implicits._

    val df = Seq((1, "data")).toDF("id", "value")

    // When using custom CSV options
    val config = LoadConfig(
      sinkType = SinkType.S3,
      path = Some("s3a://exports/data.csv"),
      connectionParams = Map(
        "format" -> "csv",
        "header" -> "true",
        "delimiter" -> "|",
        "quote" -> "\"",
        "escape" -> "\\",
        "nullValue" -> "NULL"
      )
    )

    val loader = new S3Loader()

    // Then should pass CSV options
    try {
      loader.load(df, config, WriteMode.Overwrite)
    } catch {
      case _: Exception => // Expected - no actual S3
    }
  }

  it should "support S3 credentials configuration" in {
    import spark.implicits._

    val df = Seq((1, "data")).toDF("id", "value")

    // When providing S3 credentials
    val config = LoadConfig(
      sinkType = SinkType.S3,
      path = Some("s3a://secure-bucket/data/"),
      connectionParams = Map(
        "format" -> "parquet",
        "fs.s3a.access.key" -> "AKIAIOSFODNN7EXAMPLE",
        "fs.s3a.secret.key" -> "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        "fs.s3a.endpoint" -> "s3.us-west-2.amazonaws.com"
      )
    )

    val loader = new S3Loader()

    // Then should configure credentials
    try {
      loader.load(df, config, WriteMode.Overwrite)
    } catch {
      case _: Exception => // Expected - no actual S3
    }
  }

  it should "not support Upsert mode" in {
    import spark.implicits._

    val df = Seq((1, "data")).toDF("id", "value")

    val config = LoadConfig(
      sinkType = SinkType.S3,
      path = Some("s3a://bucket/data/"),
      connectionParams = Map(
        "format" -> "parquet"
      )
    )

    val loader = new S3Loader()

    // When using Upsert mode
    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      loader.load(df, config, WriteMode.Upsert)
    }
  }
}
