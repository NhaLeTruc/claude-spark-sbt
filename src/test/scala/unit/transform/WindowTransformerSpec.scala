package com.etl.unit.transform

import com.etl.config.{TransformConfig, TransformType}
import com.etl.transform.WindowTransformer
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WindowTransformerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("WindowTransformerTest")
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

  "WindowTransformer" should "perform row_number windowing" in {
    import spark.implicits._

    // Given a DataFrame with user events
    val df = Seq(
      ("user1", "2024-01-01 10:00:00", "login"),
      ("user1", "2024-01-01 10:05:00", "click"),
      ("user1", "2024-01-01 10:10:00", "logout"),
      ("user2", "2024-01-01 11:00:00", "login"),
      ("user2", "2024-01-01 11:05:00", "click")
    ).toDF("user_id", "timestamp", "event")

    // When applying row_number window function
    val config = TransformConfig(
      transformType = TransformType.Window,
      parameters = Map(
        "partitionBy" -> "[\"user_id\"]",
        "orderBy" -> "[\"timestamp\"]",
        "windowFunction" -> "row_number",
        "outputColumn" -> "event_sequence"
      )
    )

    val transformer = new WindowTransformer()
    val result = transformer.transform(df, config)

    // Then should add sequential numbering per user
    result.schema.fieldNames should contain("event_sequence")

    val collected = result.collect()
    collected should have length 5

    // User1 should have sequences 1, 2, 3
    val user1Rows = collected.filter(_.getString(0) == "user1")
    user1Rows should have length 3
  }

  it should "perform rank windowing" in {
    import spark.implicits._

    // Given a DataFrame with scores
    val df = Seq(
      ("2024-01-01", "Alice", 95),
      ("2024-01-01", "Bob", 90),
      ("2024-01-01", "Charlie", 95),
      ("2024-01-02", "Alice", 88),
      ("2024-01-02", "Bob", 92)
    ).toDF("date", "student", "score")

    // When applying rank function
    val config = TransformConfig(
      transformType = TransformType.Window,
      parameters = Map(
        "partitionBy" -> "[\"date\"]",
        "orderBy" -> "[\"score DESC\"]",
        "windowFunction" -> "rank",
        "outputColumn" -> "rank"
      )
    )

    val transformer = new WindowTransformer()
    val result = transformer.transform(df, config)

    // Then should rank students by score per date
    result.schema.fieldNames should contain("rank")

    val collected = result.collect()
    collected should have length 5
  }

  it should "perform sum windowing aggregation" in {
    import spark.implicits._

    // Given a DataFrame with sales
    val df = Seq(
      ("store1", "2024-01-01", 100.0),
      ("store1", "2024-01-02", 150.0),
      ("store1", "2024-01-03", 200.0),
      ("store2", "2024-01-01", 300.0),
      ("store2", "2024-01-02", 250.0)
    ).toDF("store_id", "date", "revenue")

    // When calculating running total
    val config = TransformConfig(
      transformType = TransformType.Window,
      parameters = Map(
        "partitionBy" -> "[\"store_id\"]",
        "orderBy" -> "[\"date\"]",
        "windowFunction" -> "sum",
        "aggregateColumn" -> "revenue",
        "outputColumn" -> "running_total"
      )
    )

    val transformer = new WindowTransformer()
    val result = transformer.transform(df, config)

    // Then should calculate running total per store
    result.schema.fieldNames should contain("running_total")

    val collected = result.collect()
    collected should have length 5
  }

  it should "perform avg windowing aggregation" in {
    import spark.implicits._

    // Given a DataFrame with temperatures
    val df = Seq(
      ("sensor1", "2024-01-01 00:00", 20.5),
      ("sensor1", "2024-01-01 01:00", 21.0),
      ("sensor1", "2024-01-01 02:00", 19.5),
      ("sensor2", "2024-01-01 00:00", 22.0),
      ("sensor2", "2024-01-01 01:00", 23.5)
    ).toDF("sensor_id", "timestamp", "temperature")

    // When calculating moving average
    val config = TransformConfig(
      transformType = TransformType.Window,
      parameters = Map(
        "partitionBy" -> "[\"sensor_id\"]",
        "orderBy" -> "[\"timestamp\"]",
        "windowFunction" -> "avg",
        "aggregateColumn" -> "temperature",
        "outputColumn" -> "moving_avg"
      )
    )

    val transformer = new WindowTransformer()
    val result = transformer.transform(df, config)

    // Then should calculate moving average
    result.schema.fieldNames should contain("moving_avg")

    val collected = result.collect()
    collected should have length 5
  }

  it should "support lag window function" in {
    import spark.implicits._

    // Given a DataFrame with stock prices
    val df = Seq(
      ("AAPL", "2024-01-01", 150.0),
      ("AAPL", "2024-01-02", 152.0),
      ("AAPL", "2024-01-03", 148.0),
      ("GOOGL", "2024-01-01", 2800.0),
      ("GOOGL", "2024-01-02", 2850.0)
    ).toDF("symbol", "date", "price")

    // When applying lag to get previous price
    val config = TransformConfig(
      transformType = TransformType.Window,
      parameters = Map(
        "partitionBy" -> "[\"symbol\"]",
        "orderBy" -> "[\"date\"]",
        "windowFunction" -> "lag",
        "aggregateColumn" -> "price",
        "offset" -> "1",
        "outputColumn" -> "prev_price"
      )
    )

    val transformer = new WindowTransformer()
    val result = transformer.transform(df, config)

    // Then should get previous day's price
    result.schema.fieldNames should contain("prev_price")

    val collected = result.collect()
    collected should have length 5

    // First row for each symbol should have null prev_price
    val aaplFirst = collected.filter(r => r.getString(0) == "AAPL" && r.getString(1) == "2024-01-01").head
    aaplFirst.isNullAt(result.schema.fieldIndex("prev_price")) shouldBe true
  }

  it should "support lead window function" in {
    import spark.implicits._

    // Given a DataFrame with events
    val df = Seq(
      ("session1", 1, "start"),
      ("session1", 2, "click"),
      ("session1", 3, "end"),
      ("session2", 1, "start"),
      ("session2", 2, "end")
    ).toDF("session_id", "sequence", "event")

    // When applying lead to get next event
    val config = TransformConfig(
      transformType = TransformType.Window,
      parameters = Map(
        "partitionBy" -> "[\"session_id\"]",
        "orderBy" -> "[\"sequence\"]",
        "windowFunction" -> "lead",
        "aggregateColumn" -> "event",
        "offset" -> "1",
        "outputColumn" -> "next_event"
      )
    )

    val transformer = new WindowTransformer()
    val result = transformer.transform(df, config)

    // Then should get next event
    result.schema.fieldNames should contain("next_event")

    val collected = result.collect()
    collected should have length 5
  }

  it should "require partitionBy parameter" in {
    import spark.implicits._

    val df = Seq(("user1", 100)).toDF("user_id", "value")

    // Given config without partitionBy
    val config = TransformConfig(
      transformType = TransformType.Window,
      parameters = Map(
        "orderBy" -> "[\"value\"]",
        "windowFunction" -> "row_number",
        "outputColumn" -> "rank"
      )
    )

    val transformer = new WindowTransformer()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      transformer.transform(df, config)
    }
  }

  it should "require orderBy parameter" in {
    import spark.implicits._

    val df = Seq(("user1", 100)).toDF("user_id", "value")

    // Given config without orderBy
    val config = TransformConfig(
      transformType = TransformType.Window,
      parameters = Map(
        "partitionBy" -> "[\"user_id\"]",
        "windowFunction" -> "row_number",
        "outputColumn" -> "rank"
      )
    )

    val transformer = new WindowTransformer()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      transformer.transform(df, config)
    }
  }

  it should "require windowFunction parameter" in {
    import spark.implicits._

    val df = Seq(("user1", 100)).toDF("user_id", "value")

    // Given config without windowFunction
    val config = TransformConfig(
      transformType = TransformType.Window,
      parameters = Map(
        "partitionBy" -> "[\"user_id\"]",
        "orderBy" -> "[\"value\"]",
        "outputColumn" -> "result"
      )
    )

    val transformer = new WindowTransformer()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      transformer.transform(df, config)
    }
  }
}
