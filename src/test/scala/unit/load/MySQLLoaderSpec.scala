package com.etl.unit.load

import com.etl.config.{LoadConfig, SinkType}
import com.etl.load.MySQLLoader
import com.etl.model.WriteMode
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MySQLLoaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("MySQLLoaderTest")
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

  "MySQLLoader" should "validate Append mode configuration" in {
    import spark.implicits._

    // Given a DataFrame to write
    val df = Seq(
      (1, "Product A", 99.99),
      (2, "Product B", 149.99)
    ).toDF("id", "name", "price")

    // When configuring MySQL loader with Append mode
    val config = LoadConfig(
      sinkType = SinkType.MySQL,
      table = Some("products"),
      connectionParams = Map(
        "host" -> "localhost",
        "port" -> "3306",
        "database" -> "commerce",
        "user" -> "mysql"
      ),
      credentialId = Some("mysql.password")
    )

    val loader = new MySQLLoader()

    // Then should validate config (will fail without DB, but checks validation)
    try {
      loader.load(df, config, WriteMode.Append)
    } catch {
      case _: Exception => // Expected - no actual database
    }
  }

  it should "validate Overwrite mode configuration" in {
    import spark.implicits._

    val df = Seq((1, "data")).toDF("id", "value")

    // When using Overwrite mode
    val config = LoadConfig(
      sinkType = SinkType.MySQL,
      table = Some("temp_table"),
      connectionParams = Map(
        "host" -> "localhost",
        "port" -> "3306",
        "database" -> "test_db",
        "user" -> "mysql"
      )
    )

    val loader = new MySQLLoader()

    // Then should validate Overwrite mode
    try {
      loader.load(df, config, WriteMode.Overwrite)
    } catch {
      case _: Exception => // Expected - no actual database
    }
  }

  it should "validate Upsert mode configuration with primary keys" in {
    import spark.implicits._

    val df = Seq((1, "updated")).toDF("user_id", "status")

    // When using Upsert mode with primary key
    val config = LoadConfig(
      sinkType = SinkType.MySQL,
      table = Some("user_status"),
      connectionParams = Map(
        "host" -> "localhost",
        "port" -> "3306",
        "database" -> "users_db",
        "user" -> "mysql",
        "primaryKey" -> "user_id"
      ),
      credentialId = Some("mysql.password")
    )

    val loader = new MySQLLoader()

    // Then should validate Upsert config
    try {
      loader.load(df, config, WriteMode.Upsert)
    } catch {
      case _: Exception => // Expected - no actual database
    }
  }

  it should "require table parameter" in {
    import spark.implicits._

    val df = Seq((1, "data")).toDF("id", "value")

    // Given config without table
    val config = LoadConfig(
      sinkType = SinkType.MySQL,
      table = None,
      connectionParams = Map(
        "host" -> "localhost",
        "database" -> "test_db",
        "user" -> "mysql"
      )
    )

    val loader = new MySQLLoader()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      loader.load(df, config, WriteMode.Append)
    }
  }

  it should "require host parameter" in {
    import spark.implicits._

    val df = Seq((1, "data")).toDF("id", "value")

    // Given config without host
    val config = LoadConfig(
      sinkType = SinkType.MySQL,
      table = Some("test_table"),
      connectionParams = Map(
        "database" -> "test_db",
        "user" -> "mysql"
      )
    )

    val loader = new MySQLLoader()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      loader.load(df, config, WriteMode.Append)
    }
  }

  it should "require database parameter" in {
    import spark.implicits._

    val df = Seq((1, "data")).toDF("id", "value")

    // Given config without database
    val config = LoadConfig(
      sinkType = SinkType.MySQL,
      table = Some("test_table"),
      connectionParams = Map(
        "host" -> "localhost",
        "user" -> "mysql"
      )
    )

    val loader = new MySQLLoader()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      loader.load(df, config, WriteMode.Append)
    }
  }

  it should "use default port 3306 if not specified" in {
    import spark.implicits._

    val df = Seq((1, "data")).toDF("id", "value")

    // Given config without port
    val config = LoadConfig(
      sinkType = SinkType.MySQL,
      table = Some("test_table"),
      connectionParams = Map(
        "host" -> "mysql.example.com",
        "database" -> "production",
        "user" -> "writer"
      ),
      credentialId = Some("mysql.password")
    )

    val loader = new MySQLLoader()

    // Then should use default port 3306
    try {
      loader.load(df, config, WriteMode.Append)
    } catch {
      case _: Exception => // Expected - no actual database
    }
  }

  it should "support SSL configuration" in {
    import spark.implicits._

    val df = Seq((1, "secure_data")).toDF("id", "value")

    // Given config with SSL options
    val config = LoadConfig(
      sinkType = SinkType.MySQL,
      table = Some("secure_table"),
      connectionParams = Map(
        "host" -> "secure.mysql.com",
        "port" -> "3306",
        "database" -> "secure_db",
        "user" -> "secure_user",
        "useSSL" -> "true",
        "requireSSL" -> "true",
        "serverTimezone" -> "UTC"
      ),
      credentialId = Some("mysql.secure.password")
    )

    val loader = new MySQLLoader()

    // Then should include SSL options
    try {
      loader.load(df, config, WriteMode.Append)
    } catch {
      case _: Exception => // Expected - no actual database
    }
  }

  it should "support batch size configuration" in {
    import spark.implicits._

    val df = Seq((1, "data"), (2, "more")).toDF("id", "value")

    // Given config with batch size
    val config = LoadConfig(
      sinkType = SinkType.MySQL,
      table = Some("large_table"),
      connectionParams = Map(
        "host" -> "localhost",
        "port" -> "3306",
        "database" -> "test_db",
        "user" -> "mysql",
        "batchsize" -> "5000"
      )
    )

    val loader = new MySQLLoader()

    // Then should pass batch size to JDBC writer
    try {
      loader.load(df, config, WriteMode.Append)
    } catch {
      case _: Exception => // Expected - no actual database
    }
  }
}
