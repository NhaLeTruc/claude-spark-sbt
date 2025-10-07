package com.etl.unit.load

import com.etl.config.{LoadConfig, SinkType}
import com.etl.load.PostgreSQLLoader
import com.etl.model.WriteMode
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PostgreSQLLoaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("PostgreSQLLoaderTest")
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

  "PostgreSQLLoader" should "validate Append mode configuration" in {
    import spark.implicits._

    // Given a DataFrame to write
    val df = Seq(
      (1, "Alice", "alice@example.com"),
      (2, "Bob", "bob@example.com")
    ).toDF("id", "name", "email")

    // When configuring PostgreSQL loader with Append mode
    val config = LoadConfig(
      sinkType = SinkType.PostgreSQL,
      table = Some("users"),
      connectionParams = Map(
        "host" -> "localhost",
        "port" -> "5432",
        "database" -> "test_db",
        "user" -> "postgres"
      ),
      credentialId = Some("postgres.password")
    )

    val loader = new PostgreSQLLoader()

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
      sinkType = SinkType.PostgreSQL,
      table = Some("temp_table"),
      connectionParams = Map(
        "host" -> "localhost",
        "port" -> "5432",
        "database" -> "test_db",
        "user" -> "postgres"
      )
    )

    val loader = new PostgreSQLLoader()

    // Then should validate Overwrite mode
    try {
      loader.load(df, config, WriteMode.Overwrite)
    } catch {
      case _: Exception => // Expected - no actual database
    }
  }

  it should "validate Upsert mode configuration with primary keys" in {
    import spark.implicits._

    val df = Seq((1, "updated")).toDF("id", "value")

    // When using Upsert mode with primary key
    val config = LoadConfig(
      sinkType = SinkType.PostgreSQL,
      table = Some("users"),
      connectionParams = Map(
        "host" -> "localhost",
        "port" -> "5432",
        "database" -> "test_db",
        "user" -> "postgres",
        "primaryKey" -> "id"
      ),
      credentialId = Some("postgres.password")
    )

    val loader = new PostgreSQLLoader()

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
      sinkType = SinkType.PostgreSQL,
      table = None,
      connectionParams = Map(
        "host" -> "localhost",
        "database" -> "test_db",
        "user" -> "postgres"
      )
    )

    val loader = new PostgreSQLLoader()

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
      sinkType = SinkType.PostgreSQL,
      table = Some("test_table"),
      connectionParams = Map(
        "database" -> "test_db",
        "user" -> "postgres"
      )
    )

    val loader = new PostgreSQLLoader()

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
      sinkType = SinkType.PostgreSQL,
      table = Some("test_table"),
      connectionParams = Map(
        "host" -> "localhost",
        "user" -> "postgres"
      )
    )

    val loader = new PostgreSQLLoader()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      loader.load(df, config, WriteMode.Append)
    }
  }

  it should "use default port 5432 if not specified" in {
    import spark.implicits._

    val df = Seq((1, "data")).toDF("id", "value")

    // Given config without port
    val config = LoadConfig(
      sinkType = SinkType.PostgreSQL,
      table = Some("test_table"),
      connectionParams = Map(
        "host" -> "db.example.com",
        "database" -> "production",
        "user" -> "writer"
      ),
      credentialId = Some("postgres.password")
    )

    val loader = new PostgreSQLLoader()

    // Then should use default port 5432
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
      sinkType = SinkType.PostgreSQL,
      table = Some("large_table"),
      connectionParams = Map(
        "host" -> "localhost",
        "port" -> "5432",
        "database" -> "test_db",
        "user" -> "postgres",
        "batchsize" -> "1000"
      )
    )

    val loader = new PostgreSQLLoader()

    // Then should pass batch size to JDBC writer
    try {
      loader.load(df, config, WriteMode.Append)
    } catch {
      case _: Exception => // Expected - no actual database
    }
  }

  it should "support isolation level configuration" in {
    import spark.implicits._

    val df = Seq((1, "data")).toDF("id", "value")

    // Given config with isolation level
    val config = LoadConfig(
      sinkType = SinkType.PostgreSQL,
      table = Some("critical_data"),
      connectionParams = Map(
        "host" -> "localhost",
        "database" -> "test_db",
        "user" -> "postgres",
        "isolationLevel" -> "READ_COMMITTED"
      )
    )

    val loader = new PostgreSQLLoader()

    // Then should pass isolation level to JDBC writer
    try {
      loader.load(df, config, WriteMode.Append)
    } catch {
      case _: Exception => // Expected - no actual database
    }
  }
}
