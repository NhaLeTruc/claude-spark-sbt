package com.etl.load

import com.etl.config.LoadConfig
import com.etl.model.{LoadResult, WriteMode}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

/**
 * Delta Lake loader for writing data with ACID transactions to S3/HDFS.
 * Provides advanced capabilities beyond standard file formats:
 * - ACID transactions (all-or-nothing writes)
 * - Upsert/merge operations
 * - Time travel and versioning
 * - Schema evolution
 * - Optimizations (compaction, Z-ordering)
 *
 * Connection parameters:
 * - path (required): Delta table path (e.g., s3a://bucket/delta-tables/users)
 * - fs.s3a.access.key (optional): AWS access key (or use credentialId)
 * - fs.s3a.secret.key (optional): AWS secret key (or use credentialId)
 * - fs.s3a.endpoint (optional): S3 endpoint URL
 *
 * Merge/Upsert parameters (required for WriteMode.Upsert):
 * - mergeKeys: JSON array of key columns, e.g., ["id"] or ["user_id", "date"]
 * - updateCondition (optional): Condition for updates, e.g., "source.timestamp > target.timestamp"
 * - deleteCondition (optional): Condition for deletes, e.g., "source.deleted = true"
 *
 * Schema Evolution:
 * - mergeSchema: "true" to auto-add new columns during append/merge
 *
 * Partitioning:
 * - partitionBy: JSON array of partition columns, e.g., ["date", "region"]
 * - replaceWhere: Partition filter for targeted overwrite, e.g., "date >= '2024-01-01'"
 *
 * Optimizations:
 * - optimizeWrite: "true" to enable auto-optimization during write
 * - autoCompact: "true" to enable auto-compaction
 * - optimizeAfterWrite: "true" to run OPTIMIZE after write
 * - zOrderColumns: JSON array of columns for Z-ordering, e.g., ["date", "user_id"]
 * - vacuumRetentionHours: Hours to retain old versions (default: 168 = 7 days)
 *
 * Change Data Feed:
 * - enableChangeDataFeed: "true" to track all changes for CDC
 */
class DeltaLakeLoader extends Loader {
  private val logger = LoggerFactory.getLogger(getClass)

  override def load(df: DataFrame, config: LoadConfig, mode: WriteMode): LoadResult = {
    logger.info(s"Loading to Delta Lake with mode: $mode, config: ${config.connectionParams}")

    // Get Delta table path
    val deltaPath = config.connectionParams.get("path")
      .orElse(config.path)
      .getOrElse(
        throw new IllegalArgumentException(
          "Either 'path' in connectionParams or 'path' field is required for Delta Lake sink"
        )
      )

    logger.info(s"Delta Lake table path: $deltaPath")

    try {
      // Configure S3 credentials if provided
      configureS3Credentials(df.sparkSession, config)

      // Execute write based on mode
      val result = mode match {
        case WriteMode.Append    => loadAppend(df, config, deltaPath)
        case WriteMode.Overwrite => loadOverwrite(df, config, deltaPath)
        case WriteMode.Upsert    => loadMerge(df, config, deltaPath)
      }

      // Post-write optimizations
      if (config.connectionParams.get("optimizeAfterWrite").contains("true")) {
        optimizeTable(deltaPath, df.sparkSession, config)
      }

      // Vacuum old versions if configured
      config.connectionParams.get("vacuumRetentionHours").foreach { hours =>
        vacuumTable(deltaPath, df.sparkSession, hours.toInt)
      }

      result

    } catch {
      case e: Exception =>
        val errorMsg = s"Failed to write to Delta Lake path $deltaPath: ${e.getMessage}"
        logger.error(errorMsg, e)
        LoadResult.failure(0L, df.count(), errorMsg)
    }
  }

  /**
   * Append mode - Add new records to Delta table with ACID guarantees.
   */
  private def loadAppend(df: DataFrame, config: LoadConfig, deltaPath: String): LoadResult = {
    logger.info(s"Appending ${df.count()} records to Delta Lake table: $deltaPath")

    val recordCount = df.count()

    var writer = df.write
      .format("delta")
      .mode(SaveMode.Append)

    // Apply schema evolution if configured
    if (config.connectionParams.get("mergeSchema").contains("true")) {
      writer = writer.option("mergeSchema", "true")
      logger.info("Schema evolution enabled - new columns will be added automatically")
    }

    // Apply partitioning if specified
    config.connectionParams.get("partitionBy").foreach { partitionByJson =>
      val partitionColumns = Json.parse(partitionByJson).as[Seq[String]]
      if (partitionColumns.nonEmpty) {
        logger.info(s"Partitioning by: ${partitionColumns.mkString(", ")}")
        writer = writer.partitionBy(partitionColumns: _*)
      }
    }

    // Apply write optimizations
    applyWriteOptimizations(writer, config).save(deltaPath)

    logger.info(s"Successfully appended $recordCount records to Delta Lake table: $deltaPath")
    LoadResult.success(recordCount)
  }

  /**
   * Overwrite mode - Replace data in Delta table (full or partition-level).
   */
  private def loadOverwrite(df: DataFrame, config: LoadConfig, deltaPath: String): LoadResult = {
    val recordCount = df.count()

    // Check for partition-level overwrite
    val replaceWhere = config.connectionParams.get("replaceWhere")

    if (replaceWhere.isDefined) {
      logger.info(s"Overwriting partitions matching: ${replaceWhere.get}")
    } else {
      logger.info(s"Overwriting entire Delta Lake table: $deltaPath")
    }

    var writer = df.write
      .format("delta")
      .mode(SaveMode.Overwrite)

    // Apply partition overwrite if specified
    replaceWhere.foreach { condition =>
      writer = writer.option("replaceWhere", condition)
    }

    // Apply schema evolution if configured
    if (config.connectionParams.get("mergeSchema").contains("true")) {
      writer = writer.option("mergeSchema", "true")
    }

    // Apply partitioning if specified
    config.connectionParams.get("partitionBy").foreach { partitionByJson =>
      val partitionColumns = Json.parse(partitionByJson).as[Seq[String]]
      if (partitionColumns.nonEmpty) {
        writer = writer.partitionBy(partitionColumns: _*)
      }
    }

    // Apply write optimizations
    applyWriteOptimizations(writer, config).save(deltaPath)

    val msg = if (replaceWhere.isDefined) {
      s"Successfully overwrote partitions ($recordCount records) in Delta Lake table: $deltaPath"
    } else {
      s"Successfully overwrote entire Delta Lake table with $recordCount records: $deltaPath"
    }
    logger.info(msg)
    LoadResult.success(recordCount)
  }

  /**
   * Upsert/Merge mode - Insert new records and update existing ones based on merge keys.
   * This is the key feature that Delta Lake provides over standard file formats.
   */
  private def loadMerge(df: DataFrame, config: LoadConfig, deltaPath: String)(implicit spark: SparkSession = df.sparkSession): LoadResult = {
    logger.info(s"Performing upsert/merge on Delta Lake table: $deltaPath")

    // Get merge keys (required for upsert)
    val mergeKeys = config.connectionParams.get("mergeKeys")
      .map(json => Json.parse(json).as[Seq[String]])
      .getOrElse(
        throw new IllegalArgumentException(
          "mergeKeys parameter is required for Upsert mode. " +
          "Example: {\"mergeKeys\": \"[\\\"id\\\"]\"} or {\"mergeKeys\": \"[\\\"user_id\\\", \\\"date\\\"]\"}"
        )
      )

    logger.info(s"Merge keys: ${mergeKeys.mkString(", ")}")

    val recordCount = df.count()

    // Build merge condition (e.g., "target.id = source.id AND target.date = source.date")
    val mergeCondition = mergeKeys.map(key => s"target.$key = source.$key").mkString(" AND ")
    logger.info(s"Merge condition: $mergeCondition")

    // Check if table exists, create if not
    if (!DeltaTable.isDeltaTable(spark, deltaPath)) {
      logger.info(s"Delta table does not exist at $deltaPath, creating new table")

      var writer = df.write
        .format("delta")
        .mode(SaveMode.Overwrite)

      // Apply partitioning for new table
      config.connectionParams.get("partitionBy").foreach { partitionByJson =>
        val partitionColumns = Json.parse(partitionByJson).as[Seq[String]]
        if (partitionColumns.nonEmpty) {
          writer = writer.partitionBy(partitionColumns: _*)
        }
      }

      // Enable change data feed if configured
      if (config.connectionParams.get("enableChangeDataFeed").contains("true")) {
        writer = writer.option("delta.enableChangeDataFeed", "true")
      }

      applyWriteOptimizations(writer, config).save(deltaPath)

      logger.info(s"Created new Delta table with $recordCount records")
      return LoadResult.success(recordCount)
    }

    // Table exists, perform merge
    val deltaTable = DeltaTable.forPath(spark, deltaPath)

    // Build merge operation
    var mergeBuilder = deltaTable.as("target")
      .merge(
        df.as("source"),
        mergeCondition
      )

    // Apply delete condition if specified
    val deleteCondition = config.connectionParams.get("deleteCondition")
    if (deleteCondition.isDefined) {
      logger.info(s"Delete condition: ${deleteCondition.get}")
      mergeBuilder = mergeBuilder
        .whenMatched(deleteCondition.get)
        .delete()
    }

    // Apply update condition or update all
    val updateCondition = config.connectionParams.get("updateCondition")
    if (updateCondition.isDefined) {
      logger.info(s"Update condition: ${updateCondition.get}")
      mergeBuilder = mergeBuilder
        .whenMatched(updateCondition.get)
        .updateAll()
    } else {
      mergeBuilder = mergeBuilder
        .whenMatched()
        .updateAll()
    }

    // Insert new records
    mergeBuilder = mergeBuilder
      .whenNotMatched()
      .insertAll()

    // Execute merge
    mergeBuilder.execute()

    logger.info(s"Successfully merged $recordCount records into Delta Lake table: $deltaPath")
    LoadResult.success(recordCount)
  }

  /**
   * Configure S3 credentials from config or vault.
   */
  private def configureS3Credentials(spark: SparkSession, config: LoadConfig): Unit = {
    config.connectionParams.get("fs.s3a.access.key").foreach { accessKey =>
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    }

    config.connectionParams.get("fs.s3a.secret.key").foreach { secretKey =>
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    }

    config.connectionParams.get("fs.s3a.endpoint").foreach { endpoint =>
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", endpoint)
      logger.info(s"Using custom S3 endpoint: $endpoint")
    }
  }

  /**
   * Apply write optimizations based on configuration.
   */
  private def applyWriteOptimizations(
    writer: org.apache.spark.sql.DataFrameWriter[org.apache.spark.sql.Row],
    config: LoadConfig
  ): org.apache.spark.sql.DataFrameWriter[org.apache.spark.sql.Row] = {
    var optimizedWriter = writer

    if (config.connectionParams.get("optimizeWrite").contains("true")) {
      optimizedWriter = optimizedWriter.option("optimizeWrite", "true")
      logger.info("Optimize write enabled")
    }

    if (config.connectionParams.get("autoCompact").contains("true")) {
      optimizedWriter = optimizedWriter.option("autoCompact", "true")
      logger.info("Auto-compaction enabled")
    }

    if (config.connectionParams.get("enableChangeDataFeed").contains("true")) {
      optimizedWriter = optimizedWriter.option("delta.enableChangeDataFeed", "true")
      logger.info("Change data feed enabled")
    }

    optimizedWriter
  }

  /**
   * Optimize Delta table by compacting small files and optionally Z-ordering.
   */
  private def optimizeTable(deltaPath: String, spark: SparkSession, config: LoadConfig): Unit = {
    logger.info(s"Optimizing Delta table: $deltaPath")

    val deltaTable = DeltaTable.forPath(spark, deltaPath)

    // Get Z-order columns if specified
    val zOrderColumns = config.connectionParams.get("zOrderColumns")
      .map(json => Json.parse(json).as[Seq[String]])

    if (zOrderColumns.isDefined && zOrderColumns.get.nonEmpty) {
      logger.info(s"Running OPTIMIZE with Z-ordering on columns: ${zOrderColumns.get.mkString(", ")}")
      deltaTable.optimize().executeZOrderBy(zOrderColumns.get: _*)
    } else {
      logger.info("Running OPTIMIZE (compaction)")
      deltaTable.optimize().executeCompaction()
    }

    logger.info(s"Optimization complete for: $deltaPath")
  }

  /**
   * Vacuum Delta table to remove old versions and reclaim storage.
   */
  private def vacuumTable(deltaPath: String, spark: SparkSession, retentionHours: Int): Unit = {
    logger.info(s"Vacuuming Delta table: $deltaPath (retention: $retentionHours hours)")

    val deltaTable = DeltaTable.forPath(spark, deltaPath)
    deltaTable.vacuum(retentionHours)

    logger.info(s"Vacuum complete for: $deltaPath")
  }
}
