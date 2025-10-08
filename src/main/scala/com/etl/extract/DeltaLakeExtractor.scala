package com.etl.extract

import com.etl.config.{CredentialVault, ExtractConfig}
import com.etl.util.CredentialHelper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * Delta Lake extractor for reading data with time travel and change data feed support.
 * Provides advanced read capabilities:
 * - Time travel (query historical versions)
 * - Change data feed (CDC - track inserts/updates/deletes)
 * - Schema evolution handling
 * - ACID read consistency
 *
 * Connection parameters:
 * - path (required): Delta table path (e.g., s3a://bucket/delta-tables/users)
 * - fs.s3a.access.key (optional): AWS access key (or use credentialId)
 * - fs.s3a.secret.key (optional): AWS secret key (or use credentialId)
 * - fs.s3a.endpoint (optional): S3 endpoint URL
 *
 * Credential Management:
 * - Preferred: Set credentialId in ExtractConfig, store AWS keys in vault
 * - Vault should contain {credentialId}.access and {credentialId}.secret
 * - Fallback: Set fs.s3a.access.key and fs.s3a.secret.key in connectionParams
 *
 * Time Travel parameters (mutually exclusive):
 * - versionAsOf: Read specific version number, e.g., "42"
 * - timestampAsOf: Read as of timestamp, e.g., "2024-01-01 12:00:00"
 *
 * Change Data Feed (CDC) parameters:
 * - readChangeFeed: "true" to read change data instead of current state
 * - startingVersion: Starting version for CDC (inclusive), e.g., "10"
 * - startingTimestamp: Starting timestamp for CDC, e.g., "2024-01-01"
 * - endingVersion: Ending version for CDC (inclusive), e.g., "50"
 * - endingTimestamp: Ending timestamp for CDC, e.g., "2024-02-01"
 *
 * Other options:
 * - ignoreDeletes: "true" to ignore deleted rows in change feed
 * - ignoreChanges: "true" to ignore updated rows in change feed
 */
class DeltaLakeExtractor extends Extractor {
  private val logger = LoggerFactory.getLogger(getClass)

  override def extractWithVault(config: ExtractConfig, vault: CredentialVault)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Extracting from Delta Lake with config: ${config.connectionParams}")

    // Get Delta table path
    val deltaPath = config.connectionParams.get("path")
      .orElse(config.path)
      .getOrElse(
        throw new IllegalArgumentException(
          "Either 'path' in connectionParams or 'path' field is required for Delta Lake source"
        )
      )

    logger.info(s"Reading from Delta Lake table: $deltaPath")

    // Configure S3 credentials
    configureS3Credentials(config, vault)

    // Determine read mode and execute
    val df = if (isChangeFeedQuery(config)) {
      extractChangeFeed(config, deltaPath)
    } else if (isTimeTravelQuery(config)) {
      extractTimeTravel(config, deltaPath)
    } else {
      extractLatest(config, deltaPath)
    }

    logger.info(
      s"Successfully loaded Delta Lake data. " +
      s"Row count: ${df.count()}, " +
      s"Schema: ${df.schema.fieldNames.mkString(", ")}"
    )

    df
  }

  /**
   * Extract latest version of Delta table.
   */
  private def extractLatest(config: ExtractConfig, deltaPath: String)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Reading latest version from Delta Lake: $deltaPath")

    val df = spark.read
      .format("delta")
      .load(deltaPath)

    logger.info("Successfully read latest Delta Lake version")
    df
  }

  /**
   * Extract historical version using time travel.
   */
  private def extractTimeTravel(config: ExtractConfig, deltaPath: String)(implicit spark: SparkSession): DataFrame = {
    val versionAsOf = config.connectionParams.get("versionAsOf")
    val timestampAsOf = config.connectionParams.get("timestampAsOf")

    if (versionAsOf.isDefined && timestampAsOf.isDefined) {
      throw new IllegalArgumentException(
        "Cannot specify both versionAsOf and timestampAsOf. Choose one for time travel."
      )
    }

    var reader = spark.read.format("delta")

    if (versionAsOf.isDefined) {
      val version = versionAsOf.get.toLong
      logger.info(s"Reading Delta Lake version: $version")
      reader = reader.option("versionAsOf", version)
    } else if (timestampAsOf.isDefined) {
      val timestamp = timestampAsOf.get
      logger.info(s"Reading Delta Lake as of timestamp: $timestamp")
      reader = reader.option("timestampAsOf", timestamp)
    }

    val df = reader.load(deltaPath)

    logger.info(s"Successfully read Delta Lake historical version")
    df
  }

  /**
   * Extract change data feed (CDC) for incremental processing.
   */
  private def extractChangeFeed(config: ExtractConfig, deltaPath: String)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Reading change data feed from Delta Lake: $deltaPath")

    var reader = spark.read
      .format("delta")
      .option("readChangeFeed", "true")

    // Apply starting version/timestamp
    config.connectionParams.get("startingVersion").foreach { version =>
      logger.info(s"Change feed starting version: $version")
      reader = reader.option("startingVersion", version)
    }

    config.connectionParams.get("startingTimestamp").foreach { timestamp =>
      logger.info(s"Change feed starting timestamp: $timestamp")
      reader = reader.option("startingTimestamp", timestamp)
    }

    // Apply ending version/timestamp
    config.connectionParams.get("endingVersion").foreach { version =>
      logger.info(s"Change feed ending version: $version")
      reader = reader.option("endingVersion", version)
    }

    config.connectionParams.get("endingTimestamp").foreach { timestamp =>
      logger.info(s"Change feed ending timestamp: $timestamp")
      reader = reader.option("endingTimestamp", timestamp)
    }

    // Apply filters for deletes/changes
    if (config.connectionParams.get("ignoreDeletes").contains("true")) {
      logger.info("Ignoring deleted rows in change feed")
      reader = reader.option("ignoreDeletes", "true")
    }

    if (config.connectionParams.get("ignoreChanges").contains("true")) {
      logger.info("Ignoring changed rows in change feed")
      reader = reader.option("ignoreChanges", "true")
    }

    val df = reader.load(deltaPath)

    logger.info(
      s"Successfully read change data feed. " +
      s"Change columns: ${df.schema.fieldNames.filter(_.startsWith("_")).mkString(", ")}"
    )

    df
  }

  /**
   * Check if this is a time travel query.
   */
  private def isTimeTravelQuery(config: ExtractConfig): Boolean = {
    config.connectionParams.contains("versionAsOf") ||
    config.connectionParams.contains("timestampAsOf")
  }

  /**
   * Check if this is a change feed query.
   */
  private def isChangeFeedQuery(config: ExtractConfig): Boolean = {
    config.connectionParams.get("readChangeFeed").contains("true")
  }

  /**
   * Configure S3 credentials from vault or config.
   */
  private def configureS3Credentials(config: ExtractConfig, vault: CredentialVault)(implicit spark: SparkSession): Unit = {
    // Get S3 credentials from vault (preferred) or config (fallback)
    val (accessKey, secretKey) = try {
      val creds = CredentialHelper.getS3Credentials(
        config.connectionParams,
        config.credentialId,
        vault
      )
      logger.info("Using S3 credentials from vault/config (consider using IAM roles for production)")
      creds
    } catch {
      case e: IllegalArgumentException =>
        logger.info("No S3 credentials provided. Assuming IAM role-based authentication.")
        ("", "")
    }

    // Configure S3 credentials at Hadoop configuration level
    if (accessKey.nonEmpty && secretKey.nonEmpty) {
      logger.warn(
        "Setting S3 credentials in global Hadoop configuration. " +
        "For production, use IAM instance roles to avoid credential exposure."
      )
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    }

    // Configure optional endpoint
    config.connectionParams.get("fs.s3a.endpoint").foreach { endpoint =>
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", endpoint)
      logger.info(s"Using custom S3 endpoint: $endpoint")
    }
  }

  /**
   * Legacy extract method for backward compatibility.
   * Delegates to extractWithVault with empty vault.
   */
  override def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame = {
    logger.warn("Using legacy extract() without vault. Consider using extractWithVault() for secure credentials.")

    // Create empty vault for backward compatibility
    val tempVault = com.etl.config.InMemoryVault()

    extractWithVault(config, tempVault)(spark)
  }
}
