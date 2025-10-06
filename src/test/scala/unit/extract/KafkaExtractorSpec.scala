package com.etl.unit.extract

import com.etl.config.{ExtractConfig, SourceType}
import com.etl.extract.KafkaExtractor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class KafkaExtractorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("KafkaExtractorTest")
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

  "KafkaExtractor" should "create streaming DataFrame with correct schema" in {
    // Given a Kafka extract config
    val config = ExtractConfig(
      sourceType = SourceType.Kafka,
      topic = Some("user-events"),
      schemaName = "user-event",
      connectionParams = Map(
        "kafka.bootstrap.servers" -> "localhost:9092",
        "subscribe" -> "user-events",
        "startingOffsets" -> "earliest"
      )
    )

    // When extracting
    val extractor = new KafkaExtractor()
    val df = extractor.extract(config)

    // Then should return streaming DataFrame
    df.isStreaming shouldBe true
    df.schema.fieldNames should contain allOf ("key", "value", "topic", "partition", "offset", "timestamp")
  }

  it should "handle batch mode with explicit options" in {
    // Given batch mode config
    val config = ExtractConfig(
      sourceType = SourceType.Kafka,
      topic = Some("test-topic"),
      schemaName = "user-event",
      connectionParams = Map(
        "kafka.bootstrap.servers" -> "localhost:9092",
        "subscribe" -> "test-topic",
        "startingOffsets" -> "earliest",
        "endingOffsets" -> "latest"
      )
    )

    // When extracting in batch mode (endingOffsets present)
    val extractor = new KafkaExtractor()
    val df = extractor.extract(config)

    // Then should return batch DataFrame
    df.isStreaming shouldBe false
  }

  it should "require topic or subscribe parameter" in {
    // Given config without topic/subscribe
    val config = ExtractConfig(
      sourceType = SourceType.Kafka,
      topic = None,
      schemaName = "user-event",
      connectionParams = Map(
        "kafka.bootstrap.servers" -> "localhost:9092"
      )
    )

    // When extracting
    val extractor = new KafkaExtractor()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      extractor.extract(config)
    }
  }

  it should "require bootstrap.servers parameter" in {
    // Given config without bootstrap.servers
    val config = ExtractConfig(
      sourceType = SourceType.Kafka,
      topic = Some("test-topic"),
      schemaName = "user-event",
      connectionParams = Map.empty
    )

    // When extracting
    val extractor = new KafkaExtractor()

    // Then should throw IllegalArgumentException
    an[IllegalArgumentException] should be thrownBy {
      extractor.extract(config)
    }
  }

  it should "pass custom Kafka options to reader" in {
    // Given config with custom options
    val config = ExtractConfig(
      sourceType = SourceType.Kafka,
      topic = Some("secure-topic"),
      schemaName = "user-event",
      connectionParams = Map(
        "kafka.bootstrap.servers" -> "localhost:9093",
        "subscribe" -> "secure-topic",
        "kafka.security.protocol" -> "SASL_SSL",
        "kafka.sasl.mechanism" -> "PLAIN",
        "startingOffsets" -> "latest",
        "maxOffsetsPerTrigger" -> "1000"
      )
    )

    // When extracting
    val extractor = new KafkaExtractor()
    val df = extractor.extract(config)

    // Then should create DataFrame (actual Kafka connection would validate options)
    df.isStreaming shouldBe true
  }

  it should "use topic from config if subscribe not in connectionParams" in {
    // Given config with topic field but no subscribe in params
    val config = ExtractConfig(
      sourceType = SourceType.Kafka,
      topic = Some("events-topic"),
      schemaName = "user-event",
      connectionParams = Map(
        "kafka.bootstrap.servers" -> "localhost:9092",
        "startingOffsets" -> "earliest"
      )
    )

    // When extracting
    val extractor = new KafkaExtractor()
    val df = extractor.extract(config)

    // Then should succeed
    df.isStreaming shouldBe true
  }
}
