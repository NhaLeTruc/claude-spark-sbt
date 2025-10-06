package com.etl.unit.schema

import com.etl.schema.{SchemaRegistry, SchemaValidator}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SchemaValidatorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SchemaValidatorSpec")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  "SchemaValidator" should "validate DataFrame with matching schema" in {
    // Given a DataFrame matching user-event schema
    val schema = StructType(
      Seq(
        StructField("event_id", StringType, nullable = false),
        StructField("user_id", StringType, nullable = false),
        StructField("event_type", StringType, nullable = false),
        StructField("timestamp", LongType, nullable = false),
        StructField("properties", MapType(StringType, StringType), nullable = true),
        StructField("amount", DoubleType, nullable = true)
      )
    )

    val data = Seq(
      Row("evt-123", "user-456", "PAGE_VIEW", 1234567890L, null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // When validating against user-event schema
    val result = SchemaValidator.validate(df, "user-event")

    // Then validation should pass
    result.isValid shouldBe true
    result.errors shouldBe empty
  }

  it should "detect missing required fields" in {
    // Given a DataFrame missing required field (user_id)
    val schema = StructType(
      Seq(
        StructField("event_id", StringType, nullable = false),
        StructField("event_type", StringType, nullable = false),
        StructField("timestamp", LongType, nullable = false)
      )
    )

    val data = Seq(Row("evt-123", "PAGE_VIEW", 1234567890L))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // When validating
    val result = SchemaValidator.validate(df, "user-event")

    // Then should detect missing field
    result.isValid shouldBe false
    result.errors should not be empty
    result.errors.head should include("user_id")
  }

  it should "detect type mismatches" in {
    // Given a DataFrame with wrong type for timestamp (String instead of Long)
    val schema = StructType(
      Seq(
        StructField("event_id", StringType, nullable = false),
        StructField("user_id", StringType, nullable = false),
        StructField("event_type", StringType, nullable = false),
        StructField("timestamp", StringType, nullable = false), // Wrong type
        StructField("properties", MapType(StringType, StringType), nullable = true),
        StructField("amount", DoubleType, nullable = true)
      )
    )

    val data = Seq(
      Row("evt-123", "user-456", "PAGE_VIEW", "2024-01-01", null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // When validating
    val result = SchemaValidator.validate(df, "user-event")

    // Then should detect type mismatch
    result.isValid shouldBe false
    result.errors should not be empty
    result.errors.exists(_.contains("timestamp")) shouldBe true
  }

  it should "allow nullable fields to be null" in {
    // Given a DataFrame with null optional fields (properties, amount)
    val schema = StructType(
      Seq(
        StructField("event_id", StringType, nullable = false),
        StructField("user_id", StringType, nullable = false),
        StructField("event_type", StringType, nullable = false),
        StructField("timestamp", LongType, nullable = false),
        StructField("properties", MapType(StringType, StringType), nullable = true),
        StructField("amount", DoubleType, nullable = true)
      )
    )

    val data = Seq(
      Row("evt-123", "user-456", "PAGE_VIEW", 1234567890L, null, null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // When validating
    val result = SchemaValidator.validate(df, "user-event")

    // Then should pass (nullable fields can be null)
    result.isValid shouldBe true
  }

  it should "validate transaction schema correctly" in {
    // Given a DataFrame matching transaction schema
    val schema = StructType(
      Seq(
        StructField("transaction_id", StringType, nullable = false),
        StructField("user_id", StringType, nullable = false),
        StructField("amount", DoubleType, nullable = false),
        StructField("currency", StringType, nullable = false),
        StructField("transaction_time", LongType, nullable = false),
        StructField("status", StringType, nullable = false),
        StructField(
          "metadata",
          StructType(
            Seq(
              StructField("ip_address", StringType, nullable = true),
              StructField("device_id", StringType, nullable = true),
              StructField("merchant_id", StringType, nullable = true)
            )
          ),
          nullable = true
        )
      )
    )

    val data = Seq(
      Row("txn-123", "user-456", 99.99, "USD", 1234567890L, "COMPLETED", null)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    // When validating against transaction schema
    val result = SchemaValidator.validate(df, "transaction")

    // Then validation should pass
    result.isValid shouldBe true
    result.errors shouldBe empty
  }
}
