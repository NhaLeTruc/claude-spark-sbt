package com.etl.util

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import play.api.libs.json.{Json, Writes}
import java.time.{LocalDate, LocalDateTime, ZoneOffset}
import scala.util.{Failure, Success, Try}

/**
 * S3-based dead letter queue with partitioned storage.
 *
 * Stores failed records in S3 as Parquet files with date-based partitioning.
 * Uses buffering to batch writes for efficiency.
 *
 * Partitioning strategies:
 * - date: partition by date (YYYY-MM-DD)
 * - hour: partition by date and hour (YYYY-MM-DD/HH)
 * - pipeline: partition by pipeline ID
 * - stage: partition by pipeline stage
 *
 * @param bucketPath S3 bucket path (e.g., s3a://my-bucket/dlq/)
 * @param partitionBy Partitioning strategy
 * @param bufferSize Number of records to buffer before writing
 * @param format Output format (parquet, json)
 */
class S3DeadLetterQueue(
  bucketPath: String,
  partitionBy: String = "date",
  bufferSize: Int = 100,
  format: String = "parquet"
)(implicit spark: SparkSession) extends DeadLetterQueue {

  private val logger = LoggerFactory.getLogger(getClass)

  require(Set("date", "hour", "pipeline", "stage").contains(partitionBy),
    s"Invalid partitionBy: $partitionBy. Must be one of: date, hour, pipeline, stage")

  require(Set("parquet", "json").contains(format),
    s"Invalid format: $format. Must be one of: parquet, json")

  // Buffer for batch writes
  private val buffer = scala.collection.mutable.ArrayBuffer[(Row, Throwable, Map[String, String])]()
  private val lock = new Object()

  implicit val failedRecordWrites: Writes[FailedRecord] = Json.writes[FailedRecord]

  override def publish(record: Row, error: Throwable, context: Map[String, String]): Unit = {
    lock.synchronized {
      buffer.append((record, error, context))

      if (buffer.size >= bufferSize) {
        flush()
      }
    }
  }

  override def publishBatch(records: Seq[(Row, Throwable)], context: Map[String, String]): Unit = {
    logger.info(s"Publishing batch of ${records.size} failed records to S3 DLQ")

    lock.synchronized {
      records.foreach { case (record, error) =>
        buffer.append((record, error, context))
      }
      flush()
    }
  }

  /**
   * Flush buffered records to S3.
   */
  private def flush(): Unit = {
    if (buffer.isEmpty) return

    Try {
      val failedRecords = buffer.map { case (record, error, context) =>
        FailedRecord(record, error, context)
      }.toSeq

      logger.info(s"Flushing ${failedRecords.size} failed records to S3 DLQ")

      import spark.implicits._

      // Convert to DataFrame
      val df = spark.createDataFrame(failedRecords)

      // Determine partition column(s)
      val partitionColumns = partitionBy match {
        case "date" => Seq("date")
        case "hour" => Seq("date", "hour")
        case "pipeline" => Seq("pipelineId")
        case "stage" => Seq("stage")
      }

      // Add partition columns if not using built-in fields
      val dfWithPartitions = if (partitionBy == "date" || partitionBy == "hour") {
        val dfWithDate = df.withColumn("date",
          org.apache.spark.sql.functions.from_unixtime(
            org.apache.spark.sql.functions.col("timestamp") / 1000,
            "yyyy-MM-dd"
          )
        )

        if (partitionBy == "hour") {
          dfWithDate.withColumn("hour",
            org.apache.spark.sql.functions.from_unixtime(
              org.apache.spark.sql.functions.col("timestamp") / 1000,
              "HH"
            )
          )
        } else {
          dfWithDate
        }
      } else {
        df
      }

      // Write to S3
      val writer = dfWithPartitions.write
        .mode(SaveMode.Append)
        .partitionBy(partitionColumns: _*)

      format match {
        case "parquet" =>
          writer
            .option("compression", "snappy")
            .parquet(bucketPath)

        case "json" =>
          writer
            .option("compression", "gzip")
            .json(bucketPath)
      }

      logger.info(
        s"Successfully flushed ${failedRecords.size} records to S3 DLQ: $bucketPath " +
          s"(format=$format, partitionBy=$partitionBy)"
      )

      buffer.clear()

    } match {
      case Success(_) => // Success logged above
      case Failure(ex) =>
        logger.error(
          s"Failed to flush records to S3 DLQ: ${ex.getMessage}",
          ex
        )
        // Keep records in buffer for next attempt
    }
  }

  override def close(): Unit = {
    lock.synchronized {
      if (buffer.nonEmpty) {
        logger.info(s"Flushing remaining ${buffer.size} records before closing S3 DLQ")
        flush()
      }
    }
  }
}

object S3DeadLetterQueue {
  /**
   * Create an S3 DLQ with default configuration.
   */
  def apply(
    bucketPath: String
  )(implicit spark: SparkSession): S3DeadLetterQueue = {
    new S3DeadLetterQueue(bucketPath)
  }

  /**
   * Create an S3 DLQ with custom configuration.
   */
  def apply(
    bucketPath: String,
    partitionBy: String,
    bufferSize: Int,
    format: String
  )(implicit spark: SparkSession): S3DeadLetterQueue = {
    new S3DeadLetterQueue(bucketPath, partitionBy, bufferSize, format)
  }
}
