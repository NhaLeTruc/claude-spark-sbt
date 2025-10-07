package com.etl.contract

import com.etl.config._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import play.api.libs.json._

/**
 * Contract test for pipeline configuration JSON schema.
 * Verifies that pipeline configs can be parsed and validated correctly.
 */
class PipelineConfigSchemaSpec extends AnyFlatSpec with Matchers {

  "PipelineConfig JSON schema" should "parse valid complete configuration" in {
    val validJson =
      """
        |{
        |  "pipelineId": "test-pipeline",
        |  "extract": {
        |    "sourceType": "Kafka",
        |    "topic": "test-topic",
        |    "schemaName": "user-event",
        |    "connectionParams": {
        |      "kafka.bootstrap.servers": "localhost:9092"
        |    }
        |  },
        |  "transforms": [
        |    {
        |      "transformType": "Aggregation",
        |      "parameters": {
        |        "groupBy": "[\"user_id\"]",
        |        "aggregations": "{\"amount\": \"sum\"}"
        |      }
        |    }
        |  ],
        |  "load": {
        |    "sinkType": "S3",
        |    "path": "s3a://bucket/output/",
        |    "writeMode": "Append",
        |    "connectionParams": {
        |      "format": "parquet"
        |    }
        |  },
        |  "retry": {
        |    "maxAttempts": 3,
        |    "delaySeconds": 5
        |  },
        |  "performance": {
        |    "batchSize": 10000,
        |    "parallelism": 4
        |  },
        |  "logging": {
        |    "level": "INFO",
        |    "structuredLogging": true
        |  }
        |}
      """.stripMargin

    // When parsing
    val result = ConfigLoader.parseJson(validJson)

    // Then should parse successfully
    result.isRight shouldBe true
    val config = result.right.get

    config.pipelineId shouldBe "test-pipeline"
    config.extract.sourceType shouldBe SourceType.Kafka
    config.extract.topic shouldBe Some("test-topic")
    config.transforms should have size 1
    config.load.sinkType shouldBe SinkType.S3
    config.retry.maxAttempts shouldBe 3
  }

  it should "parse minimal valid configuration" in {
    val minimalJson =
      """
        |{
        |  "pipelineId": "minimal-pipeline",
        |  "extract": {
        |    "sourceType": "S3",
        |    "path": "s3a://bucket/input/",
        |    "schemaName": "user-event",
        |    "connectionParams": {
        |      "format": "csv"
        |    }
        |  },
        |  "transforms": [],
        |  "load": {
        |    "sinkType": "S3",
        |    "path": "s3a://bucket/output/",
        |    "writeMode": "Overwrite",
        |    "connectionParams": {
        |      "format": "parquet"
        |    }
        |  },
        |  "retry": {
        |    "maxAttempts": 1,
        |    "delaySeconds": 0
        |  },
        |  "performance": {
        |    "batchSize": 1000,
        |    "parallelism": 2
        |  },
        |  "logging": {
        |    "level": "WARN",
        |    "structuredLogging": false
        |  }
        |}
      """.stripMargin

    // When parsing
    val result = ConfigLoader.parseJson(minimalJson)

    // Then should parse successfully
    result.isRight shouldBe true
    val config = result.right.get

    config.pipelineId shouldBe "minimal-pipeline"
    config.transforms shouldBe empty
  }

  it should "fail to parse invalid JSON syntax" in {
    val invalidJson =
      """
        |{
        |  "pipelineId": "test-pipeline"
        |  "extract": {
        |    "sourceType": "Kafka"
        |  }
        |}
      """.stripMargin

    // When parsing
    val result = ConfigLoader.parseJson(invalidJson)

    // Then should fail
    result.isLeft shouldBe true
  }

  it should "fail to parse config with missing required fields" in {
    val missingFieldsJson =
      """
        |{
        |  "pipelineId": "test-pipeline",
        |  "extract": {
        |    "sourceType": "Kafka"
        |  }
        |}
      """.stripMargin

    // When parsing
    val result = ConfigLoader.parseJson(missingFieldsJson)

    // Then should fail (missing transforms, load, retry, etc.)
    result.isLeft shouldBe true
  }

  it should "parse all supported source types" in {
    val sourceTypes = Seq("Kafka", "PostgreSQL", "MySQL", "S3")

    sourceTypes.foreach { sourceType =>
      val json =
        s"""
           |{
           |  "pipelineId": "test-$sourceType",
           |  "extract": {
           |    "sourceType": "$sourceType",
           |    "schemaName": "user-event",
           |    "connectionParams": {}
           |  },
           |  "transforms": [],
           |  "load": {
           |    "sinkType": "S3",
           |    "path": "s3a://bucket/output/",
           |    "writeMode": "Append",
           |    "connectionParams": {"format": "parquet"}
           |  },
           |  "retry": {"maxAttempts": 1, "delaySeconds": 0},
           |  "performance": {"batchSize": 1000, "parallelism": 2},
           |  "logging": {"level": "INFO", "structuredLogging": true}
           |}
         """.stripMargin

      val result = ConfigLoader.parseJson(json)
      result.isRight shouldBe true
    }
  }

  it should "parse all supported sink types" in {
    val sinkTypes = Seq("Kafka", "PostgreSQL", "MySQL", "S3")

    sinkTypes.foreach { sinkType =>
      val json =
        s"""
           |{
           |  "pipelineId": "test-$sinkType",
           |  "extract": {
           |    "sourceType": "S3",
           |    "path": "s3a://bucket/input/",
           |    "schemaName": "user-event",
           |    "connectionParams": {"format": "csv"}
           |  },
           |  "transforms": [],
           |  "load": {
           |    "sinkType": "$sinkType",
           |    "writeMode": "Append",
           |    "connectionParams": {}
           |  },
           |  "retry": {"maxAttempts": 1, "delaySeconds": 0},
           |  "performance": {"batchSize": 1000, "parallelism": 2},
           |  "logging": {"level": "INFO", "structuredLogging": true}
           |}
         """.stripMargin

      val result = ConfigLoader.parseJson(json)
      result.isRight shouldBe true
    }
  }

  it should "parse all supported transform types" in {
    val transformTypes = Seq("Aggregation", "Join", "Window")

    transformTypes.foreach { transformType =>
      val json =
        s"""
           |{
           |  "pipelineId": "test-$transformType",
           |  "extract": {
           |    "sourceType": "S3",
           |    "path": "s3a://bucket/input/",
           |    "schemaName": "user-event",
           |    "connectionParams": {"format": "csv"}
           |  },
           |  "transforms": [
           |    {
           |      "transformType": "$transformType",
           |      "parameters": {}
           |    }
           |  ],
           |  "load": {
           |    "sinkType": "S3",
           |    "path": "s3a://bucket/output/",
           |    "writeMode": "Append",
           |    "connectionParams": {"format": "parquet"}
           |  },
           |  "retry": {"maxAttempts": 1, "delaySeconds": 0},
           |  "performance": {"batchSize": 1000, "parallelism": 2},
           |  "logging": {"level": "INFO", "structuredLogging": true}
           |}
         """.stripMargin

      val result = ConfigLoader.parseJson(json)
      result.isRight shouldBe true
    }
  }

  it should "parse all supported write modes" in {
    val writeModes = Seq("Append", "Overwrite", "Upsert")

    writeModes.foreach { writeMode =>
      val json =
        s"""
           |{
           |  "pipelineId": "test-$writeMode",
           |  "extract": {
           |    "sourceType": "S3",
           |    "path": "s3a://bucket/input/",
           |    "schemaName": "user-event",
           |    "connectionParams": {"format": "csv"}
           |  },
           |  "transforms": [],
           |  "load": {
           |    "sinkType": "S3",
           |    "path": "s3a://bucket/output/",
           |    "writeMode": "$writeMode",
           |    "connectionParams": {"format": "parquet"}
           |  },
           |  "retry": {"maxAttempts": 1, "delaySeconds": 0},
           |  "performance": {"batchSize": 1000, "parallelism": 2},
           |  "logging": {"level": "INFO", "structuredLogging": true}
           |}
         """.stripMargin

      val result = ConfigLoader.parseJson(json)
      result.isRight shouldBe true
    }
  }

  it should "validate config with proper error messages" in {
    // Create invalid config (missing required connectionParams for Kafka)
    val config = PipelineConfig(
      pipelineId = "invalid-pipeline",
      extract = ExtractConfig(
        sourceType = SourceType.Kafka,
        topic = None, // Missing topic
        schemaName = "user-event",
        connectionParams = Map.empty // Missing bootstrap.servers
      ),
      transforms = Seq.empty,
      load = LoadConfig(
        sinkType = SinkType.S3,
        path = Some("s3a://bucket/output/"),
        connectionParams = Map.empty // Missing format
      ),
      retry = RetryConfig(maxAttempts = 3, delaySeconds = 5),
      performance = PerformanceConfig(batchSize = 10000, parallelism = 4),
      logging = LoggingConfig(level = "INFO", structuredLogging = true)
    )

    // When validating
    val errors = ConfigLoader.validate(config)

    // Then should have validation errors
    errors should not be empty
  }
}
