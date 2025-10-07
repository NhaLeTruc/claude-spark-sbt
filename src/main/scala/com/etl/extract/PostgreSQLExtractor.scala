package com.etl.extract

import com.etl.config.{CredentialVault, ExtractConfig}
import com.etl.util.CredentialHelper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * PostgreSQL extractor using JDBC.
 *
 * Connection parameters:
 * - host (required): PostgreSQL host
 * - port (optional): PostgreSQL port (default: 5432)
 * - database (required): Database name
 * - user (required): Username
 * - password: Retrieved from credentialId via vault (preferred) or plain-text from config
 *
 * Credential Management:
 * - Preferred: Set credentialId in ExtractConfig, store password in encrypted vault
 * - Fallback: Set password in connectionParams (plain-text, not recommended)
 *
 * Performance parameters:
 * - fetchsize (optional): JDBC fetch size for performance
 * - numPartitions (optional): Number of partitions for parallel reads
 * - partitionColumn (optional): Column to partition on
 * - lowerBound (optional): Partition lower bound
 * - upperBound (optional): Partition upper bound
 *
 * Either 'table' or 'query' must be specified in ExtractConfig.
 */
class PostgreSQLExtractor extends Extractor {
  private val logger = LoggerFactory.getLogger(getClass)

  override def extractWithVault(config: ExtractConfig, vault: CredentialVault)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Extracting from PostgreSQL with config: ${config.connectionParams}")

    // Validate required parameters
    val host = config.connectionParams.getOrElse(
      "host",
      throw new IllegalArgumentException("host is required for PostgreSQL source")
    )

    val port = config.connectionParams.getOrElse("port", "5432")

    val database = config.connectionParams.getOrElse(
      "database",
      throw new IllegalArgumentException("database is required for PostgreSQL source")
    )

    val user = config.connectionParams.getOrElse(
      "user",
      throw new IllegalArgumentException("user is required for PostgreSQL source")
    )

    // Validate table or query
    val dbtable = (config.table, config.query) match {
      case (Some(table), None) =>
        logger.info(s"Using table: $table")
        table
      case (None, Some(query)) =>
        logger.info(s"Using custom query")
        s"($query) AS query_result"
      case (Some(_), Some(_)) =>
        throw new IllegalArgumentException(
          "Cannot specify both 'table' and 'query'. Choose one."
        )
      case (None, None) =>
        throw new IllegalArgumentException(
          "Either 'table' or 'query' must be specified for PostgreSQL source"
        )
    }

    // Build JDBC URL
    val jdbcUrl = s"jdbc:postgresql://$host:$port/$database"
    logger.info(s"Connecting to PostgreSQL: $jdbcUrl")

    // Get password from vault (preferred) or config (fallback)
    val password = CredentialHelper.getPasswordFromExtractConfig(config, vault)

    // Build JDBC reader
    var reader = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", dbtable)
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver")

    // Apply optional performance parameters
    config.connectionParams.get("fetchsize").foreach { size =>
      reader = reader.option("fetchsize", size)
    }

    config.connectionParams.get("numPartitions").foreach { num =>
      reader = reader.option("numPartitions", num)
    }

    config.connectionParams.get("partitionColumn").foreach { col =>
      reader = reader.option("partitionColumn", col)
    }

    config.connectionParams.get("lowerBound").foreach { bound =>
      reader = reader.option("lowerBound", bound)
    }

    config.connectionParams.get("upperBound").foreach { bound =>
      reader = reader.option("upperBound", bound)
    }

    // Load DataFrame
    val df = reader.load()

    logger.info(
      s"Successfully loaded PostgreSQL data. " +
        s"Row count: ${df.count()}, " +
        s"Schema: ${df.schema.fieldNames.mkString(", ")}"
    )

    df
  }

  /**
   * Legacy extract method for backward compatibility.
   * Delegates to extractWithVault with InMemoryVault containing password from config.
   */
  override def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame = {
    logger.warn("Using legacy extract() without vault. Consider using extractWithVault() for secure credentials.")

    // Create temporary vault with password from config
    val tempVault = config.connectionParams.get("password") match {
      case Some(pwd) =>
        com.etl.config.InMemoryVault("temp.password" -> pwd)
      case None =>
        com.etl.config.InMemoryVault()
    }

    // Use credentialId if present, otherwise create temp one
    val configWithCredId = config.credentialId match {
      case Some(_) => config
      case None if config.connectionParams.contains("password") =>
        config.copy(credentialId = Some("temp.password"))
      case None =>
        config
    }

    extractWithVault(configWithCredId, tempVault)(spark)
  }
}
