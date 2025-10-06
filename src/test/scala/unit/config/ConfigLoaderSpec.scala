package com.etl.unit.config

import com.etl.config._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files
import scala.util.Try

class ConfigLoaderSpec extends AnyFlatSpec with Matchers {

  "ConfigLoader" should "parse valid JSON config" in {
    // Given a valid pipeline config JSON
    val json =
      """{
        |  "pipelineId": "test-pipeline",
        |  "name": "Test Pipeline",
        |  "extract": {
        |    "type": "kafka",
        |    "topic": "test-topic",
        |    "schemaName": "user-event",
        |    "connectionParams": {
        |      "bootstrap.servers": "localhost:9092"
        |    }
        |  },
        |  "transforms": [
        |    {
        |      "type": "aggregation",
        |      "parameters": {
        |        "groupBy": ["user_id"],
        |        "aggregations": {"amount": "sum"}
        |      }
        |    }
        |  ],
        |  "load": {
        |    "type": "postgresql",
        |    "table": "user_summary",
        |    "writeMode": "upsert",
        |    "upsertKeys": ["user_id"],
        |    "schemaName": "user-summary",
        |    "connectionParams": {
        |      "host": "localhost",
        |      "port": "5432",
        |      "database": "etl_test"
        |    },
        |    "credentialId": "postgres.password"
        |  },
        |  "retry": {
        |    "maxAttempts": 3,
        |    "delaySeconds": 5
        |  }
        |}""".stripMargin

    // When parsing
    val config = ConfigLoader.parseJson(json)

    // Then should parse correctly
    config.isRight shouldBe true
    val pipelineConfig = config.right.get

    pipelineConfig.pipelineId shouldBe "test-pipeline"
    pipelineConfig.name shouldBe "Test Pipeline"
    pipelineConfig.extract.sourceType shouldBe SourceType.Kafka
    pipelineConfig.extract.topic shouldBe Some("test-topic")
    pipelineConfig.transforms should have size 1
    pipelineConfig.transforms.head.transformType shouldBe TransformType.Aggregation
    pipelineConfig.load.sinkType shouldBe SinkType.PostgreSQL
    pipelineConfig.load.writeMode shouldBe "upsert"
    pipelineConfig.load.table shouldBe Some("user_summary")
    pipelineConfig.retryConfig.maxAttempts shouldBe 3
    pipelineConfig.retryConfig.delaySeconds shouldBe 5
  }

  it should "handle minimal config without optional fields" in {
    // Given minimal config (no transforms, default retry)
    val json =
      """{
        |  "pipelineId": "minimal-pipeline",
        |  "name": "Minimal Pipeline",
        |  "extract": {
        |    "type": "s3",
        |    "path": "s3://bucket/data.csv",
        |    "schemaName": "transaction",
        |    "connectionParams": {}
        |  },
        |  "load": {
        |    "type": "s3",
        |    "path": "s3://bucket/output/",
        |    "writeMode": "append",
        |    "schemaName": "transaction",
        |    "connectionParams": {}
        |  }
        |}""".stripMargin

    // When parsing
    val config = ConfigLoader.parseJson(json)

    // Then should use defaults
    config.isRight shouldBe true
    val pipelineConfig = config.right.get

    pipelineConfig.transforms shouldBe empty
    pipelineConfig.retryConfig.maxAttempts shouldBe 3 // default
    pipelineConfig.retryConfig.delaySeconds shouldBe 5 // default
  }

  it should "fail on invalid JSON syntax" in {
    // Given invalid JSON
    val json = """{"invalid": json}"""

    // When parsing
    val config = ConfigLoader.parseJson(json)

    // Then should fail
    config.isLeft shouldBe true
    config.left.get should include("parse")
  }

  it should "fail on missing required fields" in {
    // Given JSON missing required field (pipelineId)
    val json =
      """{
        |  "name": "Test",
        |  "extract": {
        |    "type": "kafka",
        |    "topic": "test",
        |    "schemaName": "test"
        |  },
        |  "load": {
        |    "type": "kafka",
        |    "topic": "output",
        |    "writeMode": "append",
        |    "schemaName": "test"
        |  }
        |}""".stripMargin

    // When parsing
    val config = ConfigLoader.parseJson(json)

    // Then should fail
    config.isLeft shouldBe true
  }

  it should "fail on invalid source type" in {
    // Given invalid source type
    val json =
      """{
        |  "pipelineId": "test",
        |  "name": "Test",
        |  "extract": {
        |    "type": "invalid-source",
        |    "schemaName": "test",
        |    "connectionParams": {}
        |  },
        |  "load": {
        |    "type": "kafka",
        |    "topic": "output",
        |    "writeMode": "append",
        |    "schemaName": "test",
        |    "connectionParams": {}
        |  }
        |}""".stripMargin

    // When parsing
    val config = ConfigLoader.parseJson(json)

    // Then should fail
    config.isLeft shouldBe true
  }

  it should "load config from file" in {
    // Given a temporary config file
    val tempFile = Files.createTempFile("test-config", ".json").toFile
    tempFile.deleteOnExit()

    val json =
      """{
        |  "pipelineId": "file-test",
        |  "name": "File Test",
        |  "extract": {
        |    "type": "kafka",
        |    "topic": "test",
        |    "schemaName": "test",
        |    "connectionParams": {}
        |  },
        |  "load": {
        |    "type": "kafka",
        |    "topic": "output",
        |    "writeMode": "append",
        |    "schemaName": "test",
        |    "connectionParams": {}
        |  }
        |}""".stripMargin

    Files.write(tempFile.toPath, json.getBytes)

    // When loading from file
    val config = ConfigLoader.loadFromFile(tempFile.getAbsolutePath)

    // Then should succeed
    config.isRight shouldBe true
    config.right.get.pipelineId shouldBe "file-test"
  }

  it should "handle performance and logging config" in {
    // Given config with performance and logging settings
    val json =
      """{
        |  "pipelineId": "perf-test",
        |  "name": "Performance Test",
        |  "extract": {
        |    "type": "kafka",
        |    "topic": "test",
        |    "schemaName": "test",
        |    "connectionParams": {}
        |  },
        |  "load": {
        |    "type": "kafka",
        |    "topic": "output",
        |    "writeMode": "append",
        |    "schemaName": "test",
        |    "connectionParams": {}
        |  },
        |  "performance": {
        |    "shufflePartitions": 200,
        |    "broadcastThreshold": 10485760
        |  },
        |  "logging": {
        |    "logLevel": "DEBUG",
        |    "includeMetrics": false
        |  }
        |}""".stripMargin

    // When parsing
    val config = ConfigLoader.parseJson(json)

    // Then should parse custom settings
    config.isRight shouldBe true
    val pipelineConfig = config.right.get

    pipelineConfig.performanceConfig.shufflePartitions shouldBe Some(200)
    pipelineConfig.performanceConfig.broadcastThreshold shouldBe Some(10485760L)
    pipelineConfig.loggingConfig.logLevel shouldBe "DEBUG"
    pipelineConfig.loggingConfig.includeMetrics shouldBe false
  }
}
