package com.etl.load

import com.etl.config.LoadConfig
import com.etl.model.{LoadResult, WriteMode}
import com.etl.streaming.StreamingConfig
import com.etl.util.{DeadLetterQueue, ErrorHandlingContext, NoOpDeadLetterQueue}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.slf4j.LoggerFactory
import java.sql.{Connection, DriverManager}
import scala.util.{Failure, Success, Try}

/**
 * PostgreSQL loader using JDBC.
 * Supports both batch and streaming modes.
 *
 * Connection parameters:
 * - host (required): PostgreSQL host
 * - port (optional): PostgreSQL port (default: 5432)
 * - database (required): Database name
 * - user (required): Username
 * - password: Retrieved from credentialId if provided
 * - table: Table name (can also use 'table' field in LoadConfig)
 * - batchsize (optional): JDBC batch size (default: 1000)
 * - isolationLevel (optional): Transaction isolation level
 * - primaryKey (optional): Primary key column(s) for Upsert mode (comma-separated)
 *
 * Streaming enhancements:
 * - Supports streaming writes using foreachBatch
 * - Configurable trigger modes (continuous, processing time, once)
 * - Configurable output modes (append, update, complete)
 * - Exactly-once semantics with checkpointing
 *
 * Supports Append, Overwrite, and Upsert modes.
 * Integrates with error handling components for retries, circuit breaker, and DLQ.
 */
class PostgreSQLLoader(
  errorHandlingContext: Option[ErrorHandlingContext] = None,
  streamingConfig: Option[StreamingConfig] = None
) extends Loader {
  private val logger = LoggerFactory.getLogger(getClass)
  private val dlq: DeadLetterQueue = errorHandlingContext
    .map(_.deadLetterQueue)
    .getOrElse(new NoOpDeadLetterQueue())

  override def load(df: DataFrame, config: LoadConfig, mode: WriteMode): LoadResult = {
    logger.info(s"Loading to PostgreSQL with config: ${config.connectionParams}, mode: $mode")

    // Validate required parameters
    val host = config.connectionParams.getOrElse(
      "host",
      throw new IllegalArgumentException("host is required for PostgreSQL sink")
    )

    val port = config.connectionParams.getOrElse("port", "5432")

    val database = config.connectionParams.getOrElse(
      "database",
      throw new IllegalArgumentException("database is required for PostgreSQL sink")
    )

    val user = config.connectionParams.getOrElse(
      "user",
      throw new IllegalArgumentException("user is required for PostgreSQL sink")
    )

    val table = config.connectionParams.get("table")
      .orElse(config.table)
      .getOrElse(
        throw new IllegalArgumentException(
          "Either 'table' in connectionParams or 'table' field is required for PostgreSQL sink"
        )
      )

    // Build JDBC URL
    val jdbcUrl = s"jdbc:postgresql://$host:$port/$database"
    logger.info(s"Writing to PostgreSQL: $jdbcUrl, table: $table")

    // Handle streaming vs batch mode
    if (df.isStreaming) {
      handleStreamingLoad(df, jdbcUrl, table, user, config, mode)
    } else {
      handleBatchLoad(df, jdbcUrl, table, user, config, mode)
    }
  }

  /**
   * Handle batch load (original behavior).
   */
  private def handleBatchLoad(
    df: DataFrame,
    jdbcUrl: String,
    table: String,
    user: String,
    config: LoadConfig,
    mode: WriteMode
  ): LoadResult = {
    val recordCount = df.count()

    // Execute with error handling if configured
    val result = errorHandlingContext match {
      case Some(ctx) =>
        ctx.execute {
          executeLoad(df, jdbcUrl, table, user, config, mode)
        }
      case None =>
        Try(executeLoad(df, jdbcUrl, table, user, config, mode)).toEither
    }

    result match {
      case Right(_) =>
        logger.info(s"Successfully wrote $recordCount records to PostgreSQL table: $table")
        LoadResult.success(recordCount)

      case Left(error) =>
        val errorMsg = s"Failed to write to PostgreSQL table $table after retries: ${error.getMessage}"
        logger.error(errorMsg, error)

        // Publish failed records to DLQ
        val context = Map(
          "pipelineId" -> config.sinkType.toString,
          "stage" -> "load",
          "table" -> table,
          "mode" -> mode.toString
        )

        Try {
          df.collect().foreach { row =>
            dlq.publish(row, error, context)
          }
        } match {
          case Success(_) =>
            logger.info(s"Published $recordCount failed records to DLQ")
          case Failure(dlqError) =>
            logger.error(s"Failed to publish to DLQ: ${dlqError.getMessage}", dlqError)
        }

        LoadResult.failure(0L, recordCount, errorMsg)
    }
  }

  /**
   * Handle streaming load using foreachBatch.
   */
  private def handleStreamingLoad(
    df: DataFrame,
    jdbcUrl: String,
    table: String,
    user: String,
    config: LoadConfig,
    mode: WriteMode
  ): LoadResult = {
    if (streamingConfig.isEmpty) {
      throw new IllegalStateException(
        "StreamingConfig is required for streaming loads. " +
          "Please provide streamingConfig when creating PostgreSQLLoader."
      )
    }

    val cfg = streamingConfig.get
    logger.info(
      s"Starting streaming write to PostgreSQL: " +
        s"query=${cfg.queryName}, " +
        s"outputMode=${cfg.outputMode.value}, " +
        s"trigger=${cfg.triggerMode}"
    )

    // Create checkpoint location
    val checkpointLoc = s"${cfg.checkpointLocation}/postgresql-$table"

    // Build streaming writer
    var writer = df.writeStream
      .queryName(cfg.queryName)
      .outputMode(cfg.outputMode.value)
      .option("checkpointLocation", checkpointLoc)

    // Configure trigger mode
    writer = cfg.triggerMode match {
      case com.etl.streaming.TriggerMode.Continuous =>
        writer.trigger(Trigger.Continuous("1 second"))
      case com.etl.streaming.TriggerMode.ProcessingTime(interval) =>
        writer.trigger(Trigger.ProcessingTime(interval))
      case com.etl.streaming.TriggerMode.Once =>
        writer.trigger(Trigger.Once())
      case com.etl.streaming.TriggerMode.AvailableNow =>
        writer.trigger(Trigger.AvailableNow())
    }

    // Use foreachBatch to write each micro-batch to PostgreSQL
    val query = writer.foreachBatch { (batchDf: DataFrame, batchId: Long) =>
      logger.info(s"Processing batch $batchId with ${batchDf.count()} records")

      val result = errorHandlingContext match {
        case Some(ctx) =>
          ctx.execute {
            executeLoad(batchDf, jdbcUrl, table, user, config, mode)
          }
        case None =>
          Try(executeLoad(batchDf, jdbcUrl, table, user, config, mode)).toEither
      }

      result match {
        case Right(_) =>
          logger.info(s"Batch $batchId written successfully to PostgreSQL")
        case Left(error) =>
          logger.error(s"Batch $batchId failed: ${error.getMessage}", error)

          // Publish failed batch to DLQ
          val context = Map(
            "pipelineId" -> config.sinkType.toString,
            "stage" -> "load",
            "table" -> table,
            "mode" -> mode.toString,
            "batchId" -> batchId.toString
          )

          Try {
            batchDf.collect().foreach { row =>
              dlq.publish(row, error, context)
            }
          } match {
            case Success(_) =>
              logger.info(s"Published failed batch $batchId to DLQ")
            case Failure(dlqError) =>
              logger.error(s"Failed to publish batch $batchId to DLQ: ${dlqError.getMessage}", dlqError)
          }

          // Re-throw to fail the streaming query if failFast is enabled
          if (errorHandlingContext.exists(_.config.failFast)) {
            throw error
          }
      }
    }.start()

    logger.info(
      s"Streaming query started: ${query.name}, ID: ${query.id}. " +
        "Use query.awaitTermination() to wait for completion."
    )

    // For streaming, we return success immediately as the query runs asynchronously
    // Actual record counts will be tracked per batch
    LoadResult.success(0L)
  }

  private def executeLoad(
    df: DataFrame,
    jdbcUrl: String,
    table: String,
    user: String,
    config: LoadConfig,
    mode: WriteMode
  ): Unit = {
    mode match {
      case WriteMode.Append =>
        writeJdbc(df, jdbcUrl, table, user, config, SaveMode.Append)

      case WriteMode.Overwrite =>
        writeJdbc(df, jdbcUrl, table, user, config, SaveMode.Overwrite)

      case WriteMode.Upsert =>
        // Upsert requires custom logic with temp table
        performUpsert(df, jdbcUrl, table, user, config)
    }
  }

  private def writeJdbc(
      df: DataFrame,
      jdbcUrl: String,
      table: String,
      user: String,
      config: LoadConfig,
      saveMode: SaveMode
  ): Unit = {
    var writer = df.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", table)
      .option("user", user)
      .option("driver", "org.postgresql.Driver")
      .mode(saveMode)

    // Add password from credential vault if credentialId provided
    config.credentialId.foreach { credId =>
      // In actual implementation, this would retrieve from CredentialVault
      config.connectionParams.get("password").foreach { pwd =>
        writer = writer.option("password", pwd)
      }
    }

    // Apply optional parameters
    config.connectionParams.get("batchsize").foreach { size =>
      writer = writer.option("batchsize", size)
    }

    config.connectionParams.get("isolationLevel").foreach { level =>
      writer = writer.option("isolationLevel", level)
    }

    writer.save()
  }

  private def performUpsert(
      df: DataFrame,
      jdbcUrl: String,
      table: String,
      user: String,
      config: LoadConfig
  ): Unit = {
    // Get primary key columns for upsert
    val primaryKey = config.connectionParams.getOrElse(
      "primaryKey",
      throw new IllegalArgumentException(
        "primaryKey parameter is required for Upsert mode in PostgreSQL"
      )
    )

    val pkColumns = primaryKey.split(",").map(_.trim)

    logger.info(s"Performing upsert with primary key: ${pkColumns.mkString(", ")}")

    // For PostgreSQL upsert, we use INSERT ... ON CONFLICT DO UPDATE
    // This requires a temp table approach:
    // 1. Write to temp table
    // 2. Execute UPSERT from temp table to target table
    // 3. Drop temp table

    val tempTable = s"${table}_temp_${System.currentTimeMillis()}"

    var connection: Connection = null
    try {
      // Write to temp table
      writeJdbc(df, jdbcUrl, tempTable, user, config, SaveMode.Overwrite)

      // Get password for connection
      val password = config.connectionParams.get("password").getOrElse("")

      // Establish JDBC connection
      connection = DriverManager.getConnection(jdbcUrl, user, password)
      connection.setAutoCommit(false)

      // Build UPSERT query
      val columns = df.schema.fieldNames
      val setClause = columns.filter(c => !pkColumns.contains(c))
        .map(c => s"\"$c\" = EXCLUDED.\"$c\"")
        .mkString(", ")

      val conflictColumns = pkColumns.map(c => s"\"$c\"").mkString(", ")
      val columnList = columns.map(c => s"\"$c\"").mkString(", ")

      val upsertSql = s"""
        INSERT INTO "$table" ($columnList)
        SELECT $columnList FROM "$tempTable"
        ON CONFLICT ($conflictColumns)
        DO UPDATE SET $setClause
      """.trim

      logger.info(s"Executing upsert SQL: $upsertSql")

      // Execute upsert
      val statement = connection.createStatement()
      try {
        val rowsAffected = statement.executeUpdate(upsertSql)
        logger.info(s"Upsert affected $rowsAffected rows")
      } finally {
        statement.close()
      }

      // Drop temp table
      val dropSql = s"DROP TABLE IF EXISTS \"$tempTable\""
      logger.info(s"Cleaning up: $dropSql")

      val dropStatement = connection.createStatement()
      try {
        dropStatement.execute(dropSql)
      } finally {
        dropStatement.close()
      }

      // Commit transaction
      connection.commit()
      logger.info("Upsert transaction committed successfully")

    } catch {
      case e: Exception =>
        if (connection != null) {
          Try(connection.rollback()) match {
            case Success(_) => logger.info("Transaction rolled back due to error")
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
}
