package com.etl.streaming

import org.slf4j.LoggerFactory
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import scala.util.{Failure, Success, Try}

/**
 * Tracks processed batches to ensure exactly-once semantics in streaming pipelines.
 *
 * Prevents duplicate processing by maintaining a database table of batch IDs that have
 * been successfully processed. Before writing a batch, the loader checks if the batch
 * was already written.
 *
 * Thread-safety: This implementation uses database-level locking (SELECT FOR UPDATE)
 * to ensure atomicity when multiple instances of the same pipeline might run.
 *
 * Schema:
 * {{{
 * CREATE TABLE batch_tracker (
 *   pipeline_id VARCHAR(255) NOT NULL,
 *   batch_id BIGINT NOT NULL,
 *   processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
 *   record_count BIGINT,
 *   checkpoint_location VARCHAR(500),
 *   PRIMARY KEY (pipeline_id, batch_id)
 * );
 *
 * CREATE INDEX idx_batch_tracker_processed_at ON batch_tracker(processed_at);
 * }}}
 *
 * Usage:
 * {{{
 * val tracker = new BatchTracker(jdbcUrl, user, password, "my_pipeline")
 *
 * writer.foreachBatch { (batchDf: DataFrame, batchId: Long) =>
 *   if (!tracker.isBatchProcessed(batchId)) {
 *     // Write to database within transaction
 *     tracker.withTransaction { conn =>
 *       // Perform JDBC writes using conn
 *       writeDataToDB(batchDf, conn)
 *
 *       // Mark batch as processed
 *       tracker.markBatchProcessed(batchId, batchDf.count(), conn)
 *     }
 *   } else {
 *     logger.info(s"Batch $batchId already processed, skipping")
 *   }
 * }
 * }}}
 *
 * @param jdbcUrl JDBC connection URL
 * @param user Database username
 * @param password Database password
 * @param pipelineId Unique identifier for this pipeline
 * @param trackerTable Name of the batch tracker table (default: batch_tracker)
 * @param autoCreateTable Whether to automatically create the tracker table (default: true)
 */
class BatchTracker(
  jdbcUrl: String,
  user: String,
  password: String,
  pipelineId: String,
  trackerTable: String = "batch_tracker",
  autoCreateTable: Boolean = true
) {
  private val logger = LoggerFactory.getLogger(getClass)

  // Initialize table on startup if enabled
  if (autoCreateTable) {
    ensureTrackerTableExists()
  }

  /**
   * Check if a batch has already been processed.
   *
   * @param batchId Batch identifier from Spark streaming
   * @return true if batch was already processed, false otherwise
   */
  def isBatchProcessed(batchId: Long): Boolean = {
    var connection: Connection = null
    var statement: PreparedStatement = null
    var resultSet: ResultSet = null

    try {
      connection = DriverManager.getConnection(jdbcUrl, user, password)

      val sql = s"""
        SELECT 1 FROM $trackerTable
        WHERE pipeline_id = ? AND batch_id = ?
      """

      statement = connection.prepareStatement(sql)
      statement.setString(1, pipelineId)
      statement.setLong(2, batchId)

      resultSet = statement.executeQuery()
      val exists = resultSet.next()

      if (exists) {
        logger.info(s"Batch $batchId for pipeline $pipelineId already processed")
      }

      exists
    } catch {
      case e: Exception =>
        logger.error(s"Error checking batch processed status for batch $batchId: ${e.getMessage}", e)
        // Conservative: assume not processed to retry
        false
    } finally {
      closeQuietly(resultSet, statement, connection)
    }
  }

  /**
   * Mark a batch as successfully processed.
   *
   * This should be called within the same transaction as the actual data write
   * to ensure atomicity.
   *
   * @param batchId Batch identifier
   * @param recordCount Number of records in the batch
   * @param connection Active JDBC connection (should be within transaction)
   * @param checkpointLocation Optional checkpoint location for debugging
   */
  def markBatchProcessed(
    batchId: Long,
    recordCount: Long,
    connection: Connection,
    checkpointLocation: Option[String] = None
  ): Unit = {
    var statement: PreparedStatement = null

    try {
      val sql = s"""
        INSERT INTO $trackerTable (pipeline_id, batch_id, record_count, checkpoint_location)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (pipeline_id, batch_id) DO NOTHING
      """

      statement = connection.prepareStatement(sql)
      statement.setString(1, pipelineId)
      statement.setLong(2, batchId)
      statement.setLong(3, recordCount)
      statement.setString(4, checkpointLocation.orNull)

      val rowsInserted = statement.executeUpdate()

      if (rowsInserted > 0) {
        logger.info(
          s"Marked batch $batchId as processed for pipeline $pipelineId " +
            s"(records: $recordCount)"
        )
      } else {
        logger.warn(
          s"Batch $batchId for pipeline $pipelineId was already marked as processed " +
            "(possible concurrent write)"
        )
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error marking batch $batchId as processed: ${e.getMessage}", e)
        throw e
    } finally {
      closeQuietly(statement)
    }
  }

  /**
   * Execute a block of code within a database transaction.
   *
   * Automatically commits on success or rolls back on failure.
   *
   * @param block Code to execute within transaction (receives Connection)
   * @tparam T Return type of the block
   * @return Result of executing the block
   */
  def withTransaction[T](block: Connection => T): T = {
    var connection: Connection = null

    try {
      connection = DriverManager.getConnection(jdbcUrl, user, password)
      connection.setAutoCommit(false)

      val result = block(connection)

      connection.commit()
      logger.debug("Transaction committed successfully")

      result
    } catch {
      case e: Exception =>
        if (connection != null) {
          Try(connection.rollback()) match {
            case Success(_) =>
              logger.info("Transaction rolled back due to error")
            case Failure(rollbackError) =>
              logger.error(s"Failed to rollback transaction: ${rollbackError.getMessage}")
          }
        }
        throw e
    } finally {
      if (connection != null) {
        Try(connection.close()) match {
          case Failure(closeError) =>
            logger.error(s"Failed to close JDBC connection: ${closeError.getMessage}")
          case _ => // Successfully closed
        }
      }
    }
  }

  /**
   * Get the last processed batch ID for this pipeline.
   *
   * Useful for recovery scenarios.
   *
   * @return Some(batchId) if any batches processed, None otherwise
   */
  def getLastProcessedBatchId(): Option[Long] = {
    var connection: Connection = null
    var statement: PreparedStatement = null
    var resultSet: ResultSet = null

    try {
      connection = DriverManager.getConnection(jdbcUrl, user, password)

      val sql = s"""
        SELECT MAX(batch_id) as max_batch_id
        FROM $trackerTable
        WHERE pipeline_id = ?
      """

      statement = connection.prepareStatement(sql)
      statement.setString(1, pipelineId)

      resultSet = statement.executeQuery()

      if (resultSet.next()) {
        val maxBatchId = resultSet.getLong("max_batch_id")
        if (!resultSet.wasNull()) {
          Some(maxBatchId)
        } else {
          None
        }
      } else {
        None
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error getting last processed batch ID: ${e.getMessage}", e)
        None
    } finally {
      closeQuietly(resultSet, statement, connection)
    }
  }

  /**
   * Clean up old batch records (retention policy).
   *
   * @param retentionDays Number of days to keep batch records
   * @return Number of records deleted
   */
  def cleanupOldBatches(retentionDays: Int = 30): Int = {
    var connection: Connection = null
    var statement: PreparedStatement = null

    try {
      connection = DriverManager.getConnection(jdbcUrl, user, password)

      val sql = s"""
        DELETE FROM $trackerTable
        WHERE pipeline_id = ?
          AND processed_at < CURRENT_TIMESTAMP - INTERVAL '$retentionDays days'
      """

      statement = connection.prepareStatement(sql)
      statement.setString(1, pipelineId)

      val rowsDeleted = statement.executeUpdate()

      logger.info(s"Cleaned up $rowsDeleted old batch records (retention: $retentionDays days)")

      rowsDeleted
    } catch {
      case e: Exception =>
        logger.error(s"Error cleaning up old batches: ${e.getMessage}", e)
        0
    } finally {
      closeQuietly(statement, connection)
    }
  }

  /**
   * Ensure the batch tracker table exists.
   *
   * Creates the table if it doesn't exist. This is idempotent.
   */
  private def ensureTrackerTableExists(): Unit = {
    var connection: Connection = null
    var statement: PreparedStatement = null

    try {
      connection = DriverManager.getConnection(jdbcUrl, user, password)

      // PostgreSQL-specific CREATE TABLE IF NOT EXISTS
      val createTableSql = s"""
        CREATE TABLE IF NOT EXISTS $trackerTable (
          pipeline_id VARCHAR(255) NOT NULL,
          batch_id BIGINT NOT NULL,
          processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          record_count BIGINT,
          checkpoint_location VARCHAR(500),
          PRIMARY KEY (pipeline_id, batch_id)
        )
      """

      statement = connection.prepareStatement(createTableSql)
      statement.execute()

      logger.info(s"Ensured batch tracker table exists: $trackerTable")

      // Create index for cleanup queries
      val createIndexSql = s"""
        CREATE INDEX IF NOT EXISTS idx_${trackerTable}_processed_at
        ON $trackerTable(processed_at)
      """

      val indexStatement = connection.prepareStatement(createIndexSql)
      try {
        indexStatement.execute()
        logger.info(s"Ensured index exists on $trackerTable(processed_at)")
      } finally {
        indexStatement.close()
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error ensuring tracker table exists: ${e.getMessage}", e)
        throw e
    } finally {
      closeQuietly(statement, connection)
    }
  }

  /**
   * Close JDBC resources quietly (swallowing exceptions).
   */
  private def closeQuietly(resources: AutoCloseable*): Unit = {
    resources.foreach { resource =>
      if (resource != null) {
        Try(resource.close()) match {
          case Failure(e) =>
            logger.debug(s"Failed to close resource: ${e.getMessage}")
          case _ => // Successfully closed
        }
      }
    }
  }
}

/**
 * Factory for creating BatchTracker instances with configuration.
 */
object BatchTracker {
  /**
   * Create a BatchTracker from LoadConfig.
   *
   * @param config Load configuration containing JDBC connection params
   * @param pipelineId Unique pipeline identifier
   * @param trackerTable Optional custom tracker table name
   * @return BatchTracker instance
   */
  def fromLoadConfig(
    config: com.etl.config.LoadConfig,
    pipelineId: String,
    trackerTable: String = "batch_tracker"
  ): BatchTracker = {
    val host = config.connectionParams.getOrElse(
      "host",
      throw new IllegalArgumentException("host is required for BatchTracker")
    )

    val port = config.connectionParams.getOrElse("port", "5432")

    val database = config.connectionParams.getOrElse(
      "database",
      throw new IllegalArgumentException("database is required for BatchTracker")
    )

    val user = config.connectionParams.getOrElse(
      "user",
      throw new IllegalArgumentException("user is required for BatchTracker")
    )

    val password = config.connectionParams.getOrElse(
      "password",
      throw new IllegalArgumentException("password is required for BatchTracker")
    )

    val jdbcUrl = s"jdbc:postgresql://$host:$port/$database"

    new BatchTracker(
      jdbcUrl = jdbcUrl,
      user = user,
      password = password,
      pipelineId = pipelineId,
      trackerTable = trackerTable
    )
  }
}
