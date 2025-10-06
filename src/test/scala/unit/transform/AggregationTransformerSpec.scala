package com.etl.unit.transform

import com.etl.config.{TransformConfig, TransformType}
import com.etl.transform.AggregationTransformer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AggregationTransformerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("AggregationTransformerTest")
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

  "AggregationTransformer" should "perform simple aggregation with single groupBy" in {
    import spark.implicits._

    // Given a DataFrame with user transactions
    val df = Seq(
      ("user1", "buy", 100.0),
      ("user1", "buy", 50.0),
      ("user2", "buy", 200.0),
      ("user2", "sell", 75.0),
      ("user1", "sell", 25.0)
    ).toDF("user_id", "action", "amount")

    // When aggregating by user_id with sum of amount
    val config = TransformConfig(
      transformType = TransformType.Aggregation,
      parameters = Map(
        "groupBy" -> "[\"user_id\"]",
        "aggregations" -> "{\"amount\": \"sum\"}"
      )
    )

    val transformer = new AggregationTransformer()
    val result = transformer.transform(df, config)

    // Then should aggregate correctly
    val collected = result.collect()
    collected should have length 2

    val user1Total = collected.find(_.getString(0) == "user1").get.getDouble(1)
    val user2Total = collected.find(_.getString(0) == "user2").get.getDouble(1)

    user1Total shouldBe 175.0 +- 0.01
    user2Total shouldBe 275.0 +- 0.01
  }

  it should "perform aggregation with multiple groupBy columns" in {
    import spark.implicits._

    // Given a DataFrame with sales data
    val df = Seq(
      ("2024-01-01", "electronics", 500.0),
      ("2024-01-01", "electronics", 300.0),
      ("2024-01-01", "books", 50.0),
      ("2024-01-02", "electronics", 700.0),
      ("2024-01-02", "books", 100.0)
    ).toDF("date", "category", "revenue")

    // When aggregating by date and category
    val config = TransformConfig(
      transformType = TransformType.Aggregation,
      parameters = Map(
        "groupBy" -> "[\"date\", \"category\"]",
        "aggregations" -> "{\"revenue\": \"sum\"}"
      )
    )

    val transformer = new AggregationTransformer()
    val result = transformer.transform(df, config)

    // Then should group by both columns
    val collected = result.collect()
    collected should have length 4

    result.schema.fieldNames should contain allOf ("date", "category", "sum(revenue)")
  }

  it should "perform multiple aggregations on same column" in {
    import spark.implicits._

    // Given a DataFrame with measurements
    val df = Seq(
      ("sensor1", 10.5),
      ("sensor1", 12.3),
      ("sensor1", 9.8),
      ("sensor2", 15.0),
      ("sensor2", 14.5)
    ).toDF("sensor_id", "temperature")

    // When applying multiple aggregations
    val config = TransformConfig(
      transformType = TransformType.Aggregation,
      parameters = Map(
        "groupBy" -> "[\"sensor_id\"]",
        "aggregations" -> "{\"temperature\": \"avg,min,max\"}"
      )
    )

    val transformer = new AggregationTransformer()
    val result = transformer.transform(df, config)

    // Then should have all aggregation columns
    val collected = result.collect()
    collected should have length 2

    result.schema.fieldNames should contain allOf (
      "sensor_id",
      "avg(temperature)",
      "min(temperature)",
      "max(temperature)"
    )
  }

  it should "perform aggregations on multiple columns" in {
    import spark.implicits._

    // Given a DataFrame with order data
    val df = Seq(
      ("order1", 3, 150.0),
      ("order1", 2, 80.0),
      ("order2", 5, 200.0),
      ("order2", 1, 50.0)
    ).toDF("order_id", "quantity", "price")

    // When aggregating different columns
    val config = TransformConfig(
      transformType = TransformType.Aggregation,
      parameters = Map(
        "groupBy" -> "[\"order_id\"]",
        "aggregations" -> "{\"quantity\": \"sum\", \"price\": \"avg\"}"
      )
    )

    val transformer = new AggregationTransformer()
    val result = transformer.transform(df, config)

    // Then should aggregate each column with its function
    val collected = result.collect()
    collected should have length 2

    result.schema.fieldNames should contain allOf (
      "order_id",
      "sum(quantity)",
      "avg(price)"
    )
  }

  it should "support count aggregation" in {
    import spark.implicits._

    // Given a DataFrame with events
    val df = Seq(
      ("user1", "login"),
      ("user1", "click"),
      ("user1", "logout"),
      ("user2", "login"),
      ("user2", "click")
    ).toDF("user_id", "event_type")

    // When counting events per user
    val config = TransformConfig(
      transformType = TransformType.Aggregation,
      parameters = Map(
        "groupBy" -> "[\"user_id\"]",
        "aggregations" -> "{\"event_type\": \"count\"}"
      )
    )

    val transformer = new AggregationTransformer()
    val result = transformer.transform(df, config)

    // Then should count correctly
    val collected = result.collect()
    collected should have length 2

    val user1Count = collected.find(_.getString(0) == "user1").get.getLong(1)
    val user2Count = collected.find(_.getString(0) == "user2").get.getLong(1)

    user1Count shouldBe 3
    user2Count shouldBe 2
  }

  it should "require groupBy parameter" in {
    import spark.implicits._

    val df = Seq(("user1", 100)).toDF("user_id", "amount")

    // Given config without groupBy
    val config = TransformConfig(
      transformType = TransformType.Aggregation,
      parameters = Map(
        "aggregations" -> "{\"amount\": \"sum\"}"
      )
    )

    val transformer = new AggregationTransformer()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      transformer.transform(df, config)
    }
  }

  it should "require aggregations parameter" in {
    import spark.implicits._

    val df = Seq(("user1", 100)).toDF("user_id", "amount")

    // Given config without aggregations
    val config = TransformConfig(
      transformType = TransformType.Aggregation,
      parameters = Map(
        "groupBy" -> "[\"user_id\"]"
      )
    )

    val transformer = new AggregationTransformer()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      transformer.transform(df, config)
    }
  }
}
