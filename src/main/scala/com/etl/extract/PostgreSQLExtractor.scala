package com.etl.extract

import com.etl.config.ExtractConfig
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
 * - password: Retrieved from credentialId if provided
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

  override def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame = {
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

    // Build JDBC reader
    var reader = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", dbtable)
      .option("user", user)
      .option("driver", "org.postgresql.Driver")

    // Add password from credential vault if credentialId provided
    config.credentialId.foreach { credId =>
      // In actual implementation, this would retrieve from CredentialVault
      // For now, we'll look for it in connectionParams
      config.connectionParams.get("password").foreach { pwd =>
        reader = reader.option("password", pwd)
      }
    }

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
}
