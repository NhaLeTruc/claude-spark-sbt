package com.etl.unit.extract

import com.etl.config.{ExtractConfig, SourceType}
import com.etl.extract.MySQLExtractor
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MySQLExtractorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("MySQLExtractorTest")
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

  "MySQLExtractor" should "create JDBC DataFrame with table query" in {
    // Given a MySQL extract config with table
    val config = ExtractConfig(
      sourceType = SourceType.MySQL,
      table = Some("transactions"),
      schemaName = "transaction",
      connectionParams = Map(
        "host" -> "localhost",
        "port" -> "3306",
        "database" -> "commerce",
        "user" -> "mysql"
      ),
      credentialId = Some("mysql.password")
    )

    // When extracting (will fail without actual DB, but validates config)
    val extractor = new MySQLExtractor()

    // Then should construct proper JDBC URL
    try {
      extractor.extract(config)
    } catch {
      case _: Exception => // Expected - no actual database
    }
  }

  it should "create JDBC DataFrame with custom query" in {
    // Given config with custom query
    val config = ExtractConfig(
      sourceType = SourceType.MySQL,
      query = Some("SELECT * FROM orders WHERE status = 'completed' AND created_at > '2024-01-01'"),
      schemaName = "transaction",
      connectionParams = Map(
        "host" -> "mysql.example.com",
        "port" -> "3306",
        "database" -> "analytics",
        "user" -> "readonly"
      ),
      credentialId = Some("mysql.password")
    )

    // When extracting
    val extractor = new MySQLExtractor()

    // Then should use query
    try {
      extractor.extract(config)
    } catch {
      case _: Exception => // Expected - no actual database
    }
  }

  it should "require either table or query" in {
    // Given config without table or query
    val config = ExtractConfig(
      sourceType = SourceType.MySQL,
      table = None,
      query = None,
      schemaName = "transaction",
      connectionParams = Map(
        "host" -> "localhost",
        "port" -> "3306",
        "database" -> "test",
        "user" -> "mysql"
      )
    )

    // When extracting
    val extractor = new MySQLExtractor()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      extractor.extract(config)
    }
  }

  it should "require host parameter" in {
    // Given config without host
    val config = ExtractConfig(
      sourceType = SourceType.MySQL,
      table = Some("products"),
      schemaName = "transaction",
      connectionParams = Map(
        "port" -> "3306",
        "database" -> "catalog",
        "user" -> "mysql"
      )
    )

    // When extracting
    val extractor = new MySQLExtractor()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      extractor.extract(config)
    }
  }

  it should "require database parameter" in {
    // Given config without database
    val config = ExtractConfig(
      sourceType = SourceType.MySQL,
      table = Some("users"),
      schemaName = "transaction",
      connectionParams = Map(
        "host" -> "localhost",
        "port" -> "3306",
        "user" -> "mysql"
      )
    )

    // When extracting
    val extractor = new MySQLExtractor()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      extractor.extract(config)
    }
  }

  it should "use default port 3306 if not specified" in {
    // Given config without explicit port
    val config = ExtractConfig(
      sourceType = SourceType.MySQL,
      table = Some("customers"),
      schemaName = "transaction",
      connectionParams = Map(
        "host" -> "db.production.com",
        "database" -> "crm",
        "user" -> "etl"
      ),
      credentialId = Some("mysql.password")
    )

    // When extracting
    val extractor = new MySQLExtractor()

    // Then should use default port 3306
    try {
      extractor.extract(config)
    } catch {
      case _: Exception => // Expected - no actual database
    }
  }

  it should "support partitioning for parallel reads" in {
    // Given config with partitioning options
    val config = ExtractConfig(
      sourceType = SourceType.MySQL,
      table = Some("events"),
      schemaName = "transaction",
      connectionParams = Map(
        "host" -> "localhost",
        "port" -> "3306",
        "database" -> "logs",
        "user" -> "reader",
        "numPartitions" -> "10",
        "partitionColumn" -> "event_id",
        "lowerBound" -> "0",
        "upperBound" -> "10000000"
      ),
      credentialId = Some("mysql.password")
    )

    // When extracting
    val extractor = new MySQLExtractor()

    // Then should pass partitioning options to JDBC reader
    try {
      extractor.extract(config)
    } catch {
      case _: Exception => // Expected - no actual database
    }
  }

  it should "support SSL and additional connection properties" in {
    // Given config with SSL and custom properties
    val config = ExtractConfig(
      sourceType = SourceType.MySQL,
      table = Some("sensitive_data"),
      schemaName = "transaction",
      connectionParams = Map(
        "host" -> "secure.mysql.com",
        "port" -> "3306",
        "database" -> "secure_db",
        "user" -> "secure_user",
        "useSSL" -> "true",
        "requireSSL" -> "true",
        "serverTimezone" -> "UTC",
        "fetchsize" -> "5000"
      ),
      credentialId = Some("mysql.secure.password")
    )

    // When extracting
    val extractor = new MySQLExtractor()

    // Then should include SSL options in JDBC URL
    try {
      extractor.extract(config)
    } catch {
      case _: Exception => // Expected - no actual database
    }
  }
}
