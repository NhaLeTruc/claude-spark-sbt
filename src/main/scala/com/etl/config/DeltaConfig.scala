package com.etl.config

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Delta Lake configuration helper for setting up Spark session with Delta Lake support.
 *
 * This object provides methods to configure Spark for optimal Delta Lake performance,
 * including SQL extensions, catalog configuration, and performance optimizations.
 */
object DeltaConfig {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Configure SparkSession with Delta Lake extensions and optimizations.
   *
   * This should be called once after SparkSession creation and before any Delta operations.
   *
   * Configurations applied:
   * - Delta SQL extensions (enables Delta-specific SQL commands)
   * - Delta catalog (enables Delta table management)
   * - Optimize write (auto-optimization during write)
   * - Auto-compaction (automatic small file compaction)
   * - Schema auto-merge (automatic schema evolution)
   * - Retention duration check (prevents accidental vacuum of recent data)
   *
   * @param spark SparkSession to configure
   */
  def configure(spark: SparkSession): Unit = {
    logger.info("Configuring Spark session for Delta Lake support")

    // Enable Delta Lake SQL extensions
    // This allows SQL commands like OPTIMIZE, VACUUM, DESCRIBE HISTORY
    spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    logger.info("Enabled Delta Lake SQL extensions")

    // Configure Delta catalog
    // This enables Delta table discovery and management
    spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    logger.info("Configured Delta catalog")

    // Performance optimizations
    configurePerformanceOptimizations(spark)

    // Schema evolution
    configureSchemaEvolution(spark)

    // Data retention and vacuum
    configureRetention(spark)

    logger.info("Delta Lake configuration complete")
  }

  /**
   * Configure performance optimizations for Delta Lake.
   */
  private def configurePerformanceOptimizations(spark: SparkSession): Unit = {
    // Optimize write - automatically optimizes file sizes during writes
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    logger.info("Enabled optimize write (auto-optimization during write)")

    // Auto-compaction - automatically compacts small files
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    logger.info("Enabled auto-compaction (automatic small file merging)")

    // Adaptive query execution (AQE) - already enabled in most setups, but ensure it's on
    if (!spark.conf.get("spark.sql.adaptive.enabled", "false").toBoolean) {
      spark.conf.set("spark.sql.adaptive.enabled", "true")
      logger.info("Enabled adaptive query execution (AQE)")
    }

    // Max commit attempts for concurrent writes
    spark.conf.set("spark.databricks.delta.maxCommitAttempts", "10")
    logger.info("Set max commit attempts to 10 (for handling concurrent writes)")
  }

  /**
   * Configure schema evolution settings.
   */
  private def configureSchemaEvolution(spark: SparkSession): Unit = {
    // Auto-merge schema - automatically add new columns during append/merge
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    logger.info("Enabled schema auto-merge (automatic schema evolution)")

    // Allow arbitrary schema updates (use with caution)
    // spark.conf.set("spark.databricks.delta.schema.typeCheck.enabled", "false")
    // Note: Type checking is intentionally left enabled for data quality
  }

  /**
   * Configure data retention and vacuum settings.
   */
  private def configureRetention(spark: SparkSession): Unit = {
    // Retention duration check - prevents accidental deletion of recent data
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
    logger.info("Enabled retention duration check (prevents accidental vacuum of recent data)")

    // Default retention: 7 days (168 hours)
    // This can be overridden per-table with vacuum() method parameter
    spark.conf.set("spark.databricks.delta.deletedFileRetentionDuration", "interval 7 days")
    logger.info("Set default retention duration to 7 days")
  }

  /**
   * Configure SparkSession builder with Delta Lake settings.
   * Use this when creating a new SparkSession.
   *
   * @param builder SparkSession.Builder to configure
   * @return Configured SparkSession.Builder
   */
  def configureBuilder(builder: SparkSession.Builder): SparkSession.Builder = {
    builder
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.optimizeWrite.enabled", "true")
      .config("spark.databricks.delta.autoCompact.enabled", "true")
      .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
      .config("spark.databricks.delta.retentionDurationCheck.enabled", "true")
      .config("spark.databricks.delta.deletedFileRetentionDuration", "interval 7 days")
      .config("spark.databricks.delta.maxCommitAttempts", "10")
  }

  /**
   * Get recommended Delta Lake settings for production deployment.
   *
   * @return Map of configuration key-value pairs
   */
  def getProductionSettings: Map[String, String] = Map(
    // Core Delta settings
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",

    // Performance
    "spark.databricks.delta.optimizeWrite.enabled" -> "true",
    "spark.databricks.delta.autoCompact.enabled" -> "true",
    "spark.databricks.delta.maxCommitAttempts" -> "10",

    // Schema evolution
    "spark.databricks.delta.schema.autoMerge.enabled" -> "true",

    // Retention
    "spark.databricks.delta.retentionDurationCheck.enabled" -> "true",
    "spark.databricks.delta.deletedFileRetentionDuration" -> "interval 7 days",

    // AQE (if not already set)
    "spark.sql.adaptive.enabled" -> "true"
  )

  /**
   * Get recommended Delta Lake settings for development/testing.
   * More permissive settings for faster iteration.
   *
   * @return Map of configuration key-value pairs
   */
  def getDevelopmentSettings: Map[String, String] = Map(
    // Core Delta settings
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",

    // Performance (disabled for faster writes in dev)
    "spark.databricks.delta.optimizeWrite.enabled" -> "false",
    "spark.databricks.delta.autoCompact.enabled" -> "false",

    // Schema evolution (permissive)
    "spark.databricks.delta.schema.autoMerge.enabled" -> "true",

    // Retention (shorter for dev)
    "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
    "spark.databricks.delta.deletedFileRetentionDuration" -> "interval 1 hour"
  )
}
