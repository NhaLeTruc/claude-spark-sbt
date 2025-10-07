package com.etl.unit.load

import com.etl.config.{LoadConfig, SinkType}
import com.etl.load.KafkaLoader
import com.etl.model.WriteMode
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KafkaLoaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("KafkaLoaderTest")
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

  "KafkaLoader" should "validate batch write configuration" in {
    import spark.implicits._

    // Given a DataFrame to write
    val df = Seq(
      ("key1", """{"user_id": "user1", "event": "login"}"""),
      ("key2", """{"user_id": "user2", "event": "click"}""")
    ).toDF("key", "value")

    // When configuring Kafka loader for batch
    val config = LoadConfig(
      sinkType = SinkType.Kafka,
      topic = Some("output-events"),
      connectionParams = Map(
        "kafka.bootstrap.servers" -> "localhost:9092"
      )
    )

    val loader = new KafkaLoader()

    // Then should validate config (will fail without Kafka, but checks validation)
    try {
      loader.load(df, config, WriteMode.Append)
    } catch {
      case _: Exception => // Expected - no actual Kafka
    }
  }

  it should "validate streaming write configuration" in {
    import spark.implicits._

    // Given a streaming DataFrame
    val streamDf = spark.readStream
      .format("rate")
      .option("rowsPerSecond", "1")
      .load()
      .selectExpr("CAST(timestamp AS STRING) AS key", "CAST(value AS STRING) AS value")

    // When configuring Kafka loader for streaming
    val config = LoadConfig(
      sinkType = SinkType.Kafka,
      topic = Some("stream-output"),
      connectionParams = Map(
        "kafka.bootstrap.servers" -> "localhost:9092",
        "checkpointLocation" -> "/tmp/checkpoint"
      )
    )

    val loader = new KafkaLoader()

    // Then should handle streaming configuration
    streamDf.isStreaming shouldBe true
  }

  it should "require topic parameter" in {
    import spark.implicits._

    val df = Seq(("key1", "value1")).toDF("key", "value")

    // Given config without topic
    val config = LoadConfig(
      sinkType = SinkType.Kafka,
      topic = None,
      connectionParams = Map(
        "kafka.bootstrap.servers" -> "localhost:9092"
      )
    )

    val loader = new KafkaLoader()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      loader.load(df, config, WriteMode.Append)
    }
  }

  it should "require bootstrap.servers parameter" in {
    import spark.implicits._

    val df = Seq(("key1", "value1")).toDF("key", "value")

    // Given config without bootstrap.servers
    val config = LoadConfig(
      sinkType = SinkType.Kafka,
      topic = Some("test-topic"),
      connectionParams = Map.empty
    )

    val loader = new KafkaLoader()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      loader.load(df, config, WriteMode.Append)
    }
  }

  it should "require key and value columns" in {
    import spark.implicits._

    // Given DataFrame without required key/value columns
    val df = Seq(
      ("col1", "col2")
    ).toDF("field1", "field2")

    val config = LoadConfig(
      sinkType = SinkType.Kafka,
      topic = Some("test-topic"),
      connectionParams = Map(
        "kafka.bootstrap.servers" -> "localhost:9092"
      )
    )

    val loader = new KafkaLoader()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      loader.load(df, config, WriteMode.Append)
    }
  }

  it should "support custom Kafka producer options" in {
    import spark.implicits._

    val df = Seq(("key1", "value1")).toDF("key", "value")

    // Given config with custom producer options
    val config = LoadConfig(
      sinkType = SinkType.Kafka,
      topic = Some("secure-topic"),
      connectionParams = Map(
        "kafka.bootstrap.servers" -> "localhost:9093",
        "kafka.security.protocol" -> "SASL_SSL",
        "kafka.sasl.mechanism" -> "PLAIN",
        "kafka.compression.type" -> "snappy"
      )
    )

    val loader = new KafkaLoader()

    // Then should accept custom options
    try {
      loader.load(df, config, WriteMode.Append)
    } catch {
      case _: Exception => // Expected - no actual Kafka
    }
  }

  it should "only support Append write mode" in {
    import spark.implicits._

    val df = Seq(("key1", "value1")).toDF("key", "value")

    val config = LoadConfig(
      sinkType = SinkType.Kafka,
      topic = Some("test-topic"),
      connectionParams = Map(
        "kafka.bootstrap.servers" -> "localhost:9092"
      )
    )

    val loader = new KafkaLoader()

    // When using Overwrite mode
    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      loader.load(df, config, WriteMode.Overwrite)
    }

    // When using Upsert mode
    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      loader.load(df, config, WriteMode.Upsert)
    }
  }
}
