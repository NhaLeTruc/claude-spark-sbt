package com.etl.util

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import play.api.libs.json.{Json, Writes}
import java.util.Properties
import org.slf4j.LoggerFactory
import scala.util.{Failure, Success, Try}

/**
 * Kafka-based dead letter queue.
 *
 * Publishes failed records to a Kafka topic for later analysis and reprocessing.
 * Uses a Kafka producer with acknowledgment and retries for reliability.
 *
 * Configuration:
 * - Acks: all (ensure all replicas acknowledge)
 * - Retries: 3 (retry failed sends)
 * - Compression: gzip (reduce network usage)
 *
 * @param bootstrapServers Kafka broker addresses
 * @param topic DLQ topic name
 * @param producerConfig Additional producer configuration
 */
class KafkaDeadLetterQueue(
  bootstrapServers: String,
  topic: String,
  producerConfig: Map[String, String] = Map.empty
)(implicit spark: SparkSession) extends DeadLetterQueue {

  private val logger = LoggerFactory.getLogger(getClass)

  // JSON serialization for FailedRecord
  implicit val failedRecordWrites: Writes[FailedRecord] = Json.writes[FailedRecord]

  private lazy val producer: KafkaProducer[String, String] = {
    val props = new Properties()

    // Required properties
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Reliability properties
    props.put("acks", "all") // Wait for all in-sync replicas
    props.put("retries", "3") // Retry failed sends
    props.put("max.in.flight.requests.per.connection", "1") // Ensure ordering
    props.put("enable.idempotence", "true") // Prevent duplicates

    // Performance properties
    props.put("compression.type", "gzip")
    props.put("batch.size", "16384")
    props.put("linger.ms", "10")

    // Apply custom config
    producerConfig.foreach { case (key, value) =>
      props.put(key, value)
    }

    logger.info(s"Initializing Kafka DLQ producer: topic=$topic, bootstrap.servers=$bootstrapServers")
    new KafkaProducer[String, String](props)
  }

  override def publish(record: Row, error: Throwable, context: Map[String, String]): Unit = {
    Try {
      val failedRecord = FailedRecord(record, error, context)

      // Use pipeline ID as partition key for ordering
      val key = failedRecord.pipelineId

      // Serialize to JSON
      val value = Json.toJson(failedRecord).toString()

      // Create producer record with headers
      val producerRecord = new ProducerRecord[String, String](
        topic,
        null, // partition - let Kafka decide based on key
        key,
        value
      )

      // Add headers for filtering/routing
      producerRecord.headers()
        .add("pipeline_id", failedRecord.pipelineId.getBytes)
        .add("stage", failedRecord.stage.getBytes)
        .add("error_type", failedRecord.errorType.getBytes)
        .add("timestamp", failedRecord.timestamp.toString.getBytes)

      // Send asynchronously with callback
      producer.send(producerRecord, (metadata: RecordMetadata, exception: Exception) => {
        if (exception != null) {
          logger.error(
            s"Failed to publish to DLQ topic=$topic: ${exception.getMessage}",
            exception
          )
        } else {
          logger.debug(
            s"Published failed record to DLQ: " +
              s"topic=$topic, partition=${metadata.partition()}, offset=${metadata.offset()}, " +
              s"pipeline=${failedRecord.pipelineId}, stage=${failedRecord.stage}"
          )
        }
      })

    } match {
      case Success(_) => // Successfully submitted
      case Failure(ex) =>
        logger.error(
          s"Exception publishing to Kafka DLQ topic=$topic: ${ex.getMessage}",
          ex
        )
        // Don't throw - DLQ failures shouldn't break the pipeline
    }
  }

  override def publishBatch(records: Seq[(Row, Throwable)], context: Map[String, String]): Unit = {
    logger.info(s"Publishing batch of ${records.size} failed records to Kafka DLQ")

    records.foreach { case (record, error) =>
      publish(record, error, context)
    }

    // Flush to ensure all records are sent
    Try(producer.flush()) match {
      case Success(_) =>
        logger.info(s"Successfully flushed ${records.size} records to Kafka DLQ")
      case Failure(ex) =>
        logger.error(s"Failed to flush records to Kafka DLQ: ${ex.getMessage}", ex)
    }
  }

  override def close(): Unit = {
    Try {
      logger.info("Closing Kafka DLQ producer")
      producer.flush()
      producer.close()
    } match {
      case Success(_) => logger.info("Kafka DLQ producer closed successfully")
      case Failure(ex) => logger.error(s"Error closing Kafka DLQ producer: ${ex.getMessage}", ex)
    }
  }
}

object KafkaDeadLetterQueue {
  /**
   * Create a Kafka DLQ with default configuration.
   */
  def apply(
    bootstrapServers: String,
    topic: String
  )(implicit spark: SparkSession): KafkaDeadLetterQueue = {
    new KafkaDeadLetterQueue(bootstrapServers, topic)
  }

  /**
   * Create a Kafka DLQ with custom producer configuration.
   */
  def apply(
    bootstrapServers: String,
    topic: String,
    producerConfig: Map[String, String]
  )(implicit spark: SparkSession): KafkaDeadLetterQueue = {
    new KafkaDeadLetterQueue(bootstrapServers, topic, producerConfig)
  }
}
