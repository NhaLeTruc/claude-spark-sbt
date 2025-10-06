package com.etl.unit.transform

import com.etl.config.{TransformConfig, TransformType}
import com.etl.transform.JoinTransformer
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JoinTransformerSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("JoinTransformerTest")
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

  "JoinTransformer" should "perform inner join on single column" in {
    import spark.implicits._

    // Given two DataFrames
    val users = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
    ).toDF("user_id", "name")

    val orders = Seq(
      (101, 1, 50.0),
      (102, 2, 75.0),
      (103, 1, 100.0)
    ).toDF("order_id", "user_id", "amount")

    // When performing inner join
    val config = TransformConfig(
      transformType = TransformType.Join,
      parameters = Map(
        "joinType" -> "inner",
        "joinColumns" -> "[\"user_id\"]"
      )
    )

    val transformer = new JoinTransformer(orders)
    val result = transformer.transform(users, config)

    // Then should join on user_id
    val collected = result.collect()
    collected should have length 3

    result.schema.fieldNames should contain allOf ("user_id", "name", "order_id", "amount")
  }

  it should "perform left join" in {
    import spark.implicits._

    // Given two DataFrames
    val users = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
    ).toDF("user_id", "name")

    val orders = Seq(
      (101, 1, 50.0),
      (102, 1, 75.0)
    ).toDF("order_id", "user_id", "amount")

    // When performing left join
    val config = TransformConfig(
      transformType = TransformType.Join,
      parameters = Map(
        "joinType" -> "left",
        "joinColumns" -> "[\"user_id\"]"
      )
    )

    val transformer = new JoinTransformer(orders)
    val result = transformer.transform(users, config)

    // Then should include all users (even without orders)
    val collected = result.collect()
    collected should have length 4 // Alice(2) + Bob(0) + Charlie(0) = 4 rows

    // Charlie and Bob should have null order_id
    val charlieRow = collected.filter(_.getString(1) == "Charlie").head
    charlieRow.isNullAt(2) shouldBe true // order_id is null
  }

  it should "perform right join" in {
    import spark.implicits._

    // Given two DataFrames
    val users = Seq(
      (1, "Alice"),
      (2, "Bob")
    ).toDF("user_id", "name")

    val orders = Seq(
      (101, 1, 50.0),
      (102, 2, 75.0),
      (103, 3, 100.0) // user 3 doesn't exist in users
    ).toDF("order_id", "user_id", "amount")

    // When performing right join
    val config = TransformConfig(
      transformType = TransformType.Join,
      parameters = Map(
        "joinType" -> "right",
        "joinColumns" -> "[\"user_id\"]"
      )
    )

    val transformer = new JoinTransformer(orders)
    val result = transformer.transform(users, config)

    // Then should include all orders (even without user names)
    val collected = result.collect()
    collected should have length 3

    // Order 103 should have null name
    val order103 = collected.filter(_.getInt(2) == 103).head
    order103.isNullAt(1) shouldBe true // name is null
  }

  it should "perform outer (full) join" in {
    import spark.implicits._

    // Given two DataFrames
    val users = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (4, "David") // no orders
    ).toDF("user_id", "name")

    val orders = Seq(
      (101, 1, 50.0),
      (102, 3, 75.0) // no user
    ).toDF("order_id", "user_id", "amount")

    // When performing outer join
    val config = TransformConfig(
      transformType = TransformType.Join,
      parameters = Map(
        "joinType" -> "outer",
        "joinColumns" -> "[\"user_id\"]"
      )
    )

    val transformer = new JoinTransformer(orders)
    val result = transformer.transform(users, config)

    // Then should include all rows from both sides
    val collected = result.collect()
    collected.length should be >= 3 // At least Alice, David, and order 102
  }

  it should "perform join on multiple columns" in {
    import spark.implicits._

    // Given two DataFrames with composite keys
    val sales = Seq(
      ("2024-01-01", "store1", 1000.0),
      ("2024-01-01", "store2", 1500.0),
      ("2024-01-02", "store1", 1200.0)
    ).toDF("date", "store_id", "revenue")

    val targets = Seq(
      ("2024-01-01", "store1", 900.0),
      ("2024-01-01", "store2", 1400.0),
      ("2024-01-02", "store1", 1100.0)
    ).toDF("date", "store_id", "target")

    // When joining on multiple columns
    val config = TransformConfig(
      transformType = TransformType.Join,
      parameters = Map(
        "joinType" -> "inner",
        "joinColumns" -> "[\"date\", \"store_id\"]"
      )
    )

    val transformer = new JoinTransformer(targets)
    val result = transformer.transform(sales, config)

    // Then should join on both columns
    val collected = result.collect()
    collected should have length 3

    result.schema.fieldNames should contain allOf ("date", "store_id", "revenue", "target")
  }

  it should "handle column name conflicts with suffixes" in {
    import spark.implicits._

    // Given DataFrames with overlapping column names
    val left = Seq(
      (1, "value1"),
      (2, "value2")
    ).toDF("id", "data")

    val right = Seq(
      (1, "other1"),
      (2, "other2")
    ).toDF("id", "data") // 'data' conflicts

    // When joining (transformer should handle conflicts)
    val config = TransformConfig(
      transformType = TransformType.Join,
      parameters = Map(
        "joinType" -> "inner",
        "joinColumns" -> "[\"id\"]"
      )
    )

    val transformer = new JoinTransformer(right)
    val result = transformer.transform(left, config)

    // Then should resolve conflicts (implementation-dependent)
    result.collect() should have length 2
  }

  it should "require joinType parameter" in {
    import spark.implicits._

    val df1 = Seq((1, "A")).toDF("id", "value")
    val df2 = Seq((1, "B")).toDF("id", "other")

    // Given config without joinType
    val config = TransformConfig(
      transformType = TransformType.Join,
      parameters = Map(
        "joinColumns" -> "[\"id\"]"
      )
    )

    val transformer = new JoinTransformer(df2)

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      transformer.transform(df1, config)
    }
  }

  it should "require joinColumns parameter" in {
    import spark.implicits._

    val df1 = Seq((1, "A")).toDF("id", "value")
    val df2 = Seq((1, "B")).toDF("id", "other")

    // Given config without joinColumns
    val config = TransformConfig(
      transformType = TransformType.Join,
      parameters = Map(
        "joinType" -> "inner"
      )
    )

    val transformer = new JoinTransformer(df2)

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      transformer.transform(df1, config)
    }
  }
}
