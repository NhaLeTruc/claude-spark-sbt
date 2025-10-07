package com.etl.util

import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory
import java.time.Instant

/**
 * Dead letter queue for failed records.
 *
 * A DLQ stores records that fail processing for later analysis and reprocessing.
 * Failed records include metadata about the failure, original data, and context.
 */
trait DeadLetterQueue {
  /**
   * Publish a failed record to the DLQ.
   *
   * @param record The failed record
   * @param error The error that caused failure
   * @param context Additional context (pipeline ID, stage, attempt number, etc.)
   */
  def publish(record: Row, error: Throwable, context: Map[String, String]): Unit

  /**
   * Publish multiple failed records in batch.
   *
   * @param records Sequence of (record, error) pairs
   * @param context Shared context for all records
   */
  def publishBatch(records: Seq[(Row, Throwable)], context: Map[String, String]): Unit = {
    records.foreach { case (record, error) =>
      publish(record, error, context)
    }
  }

  /**
   * Close the DLQ and release resources.
   */
  def close(): Unit
}

/**
 * Failed record envelope with metadata.
 *
 * Contains the original record plus extensive metadata for debugging and reprocessing.
 *
 * @param timestamp When the failure occurred (epoch millis)
 * @param pipelineId Pipeline that failed
 * @param stage Pipeline stage where failure occurred (extract/transform/load)
 * @param errorType Error class name
 * @param errorMessage Error message
 * @param stackTrace Full stack trace
 * @param originalRecord Original record as pipe-delimited string
 * @param originalSchema Schema field names
 * @param attemptNumber Which retry attempt failed
 * @param context Additional context
 */
case class FailedRecord(
  timestamp: Long,
  pipelineId: String,
  stage: String,
  errorType: String,
  errorMessage: String,
  stackTrace: String,
  originalRecord: String,
  originalSchema: String,
  attemptNumber: Int,
  context: Map[String, String]
)

object FailedRecord {
  /**
   * Create a FailedRecord from a Row and error.
   *
   * @param record The failed Spark Row
   * @param error The exception
   * @param context Pipeline context
   * @return FailedRecord with all metadata
   */
  def apply(
    record: Row,
    error: Throwable,
    context: Map[String, String]
  ): FailedRecord = {
    FailedRecord(
      timestamp = Instant.now().toEpochMilli,
      pipelineId = context.getOrElse("pipelineId", "unknown"),
      stage = context.getOrElse("stage", "unknown"),
      errorType = error.getClass.getName,
      errorMessage = Option(error.getMessage).getOrElse("No error message"),
      stackTrace = error.getStackTrace.take(10).mkString("\n"), // Limit stack trace
      originalRecord = rowToString(record),
      originalSchema = if (record.schema != null) record.schema.fieldNames.mkString(",") else "",
      attemptNumber = context.getOrElse("attemptNumber", "1").toIntOption.getOrElse(1),
      context = context
    )
  }

  private def rowToString(row: Row): String = {
    try {
      (0 until row.length).map { i =>
        val value = row.get(i)
        if (value == null) "null" else value.toString
      }.mkString("|")
    } catch {
      case _: Exception => s"[Error serializing row with ${row.length} fields]"
    }
  }
}

/**
 * No-op DLQ that discards all failed records.
 * Use for testing or when DLQ is not needed.
 */
class NoOpDeadLetterQueue extends DeadLetterQueue {
  private val logger = LoggerFactory.getLogger(getClass)

  override def publish(record: Row, error: Throwable, context: Map[String, String]): Unit = {
    logger.debug(s"DLQ (no-op): Discarding failed record from ${context.getOrElse("pipelineId", "unknown")}")
  }

  override def close(): Unit = {
    // No resources to close
  }
}

/**
 * Logging-based DLQ that writes failed records to logs.
 * Useful for development and debugging.
 */
class LoggingDeadLetterQueue extends DeadLetterQueue {
  private val logger = LoggerFactory.getLogger(getClass)

  override def publish(record: Row, error: Throwable, context: Map[String, String]): Unit = {
    val failedRecord = FailedRecord(record, error, context)

    logger.error(
      s"DLQ: Failed record published\n" +
        s"  Pipeline: ${failedRecord.pipelineId}\n" +
        s"  Stage: ${failedRecord.stage}\n" +
        s"  Error: ${failedRecord.errorType}: ${failedRecord.errorMessage}\n" +
        s"  Record: ${failedRecord.originalRecord}\n" +
        s"  Attempt: ${failedRecord.attemptNumber}\n" +
        s"  Timestamp: ${failedRecord.timestamp}"
    )
  }

  override def close(): Unit = {
    // No resources to close
  }
}

object DeadLetterQueue {
  /**
   * Create a no-op DLQ.
   */
  def noOp: DeadLetterQueue = new NoOpDeadLetterQueue()

  /**
   * Create a logging DLQ.
   */
  def logging: DeadLetterQueue = new LoggingDeadLetterQueue()
}
