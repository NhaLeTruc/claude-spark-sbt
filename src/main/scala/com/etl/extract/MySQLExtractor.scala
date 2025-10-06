package com.etl.extract

import com.etl.config.ExtractConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * MySQL extractor using JDBC.
 *
 * Connection parameters:
 * - host (required): MySQL host
 * - port (optional): MySQL port (default: 3306)
 * - database (required): Database name
 * - user (required): Username
 * - password: Retrieved from credentialId if provided
 * - fetchsize (optional): JDBC fetch size for performance
 * - numPartitions (optional): Number of partitions for parallel reads
 * - partitionColumn (optional): Column to partition on
 * - lowerBound (optional): Partition lower bound
 * - upperBound (optional): Partition upper bound
 * - useSSL (optional): Enable SSL connection
 * - serverTimezone (optional): Server timezone (e.g., UTC)
 *
 * Either 'table' or 'query' must be specified in ExtractConfig.
 */
class MySQLExtractor extends Extractor {
  private val logger = LoggerFactory.getLogger(getClass)

  override def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Extracting from MySQL with config: ${config.connectionParams}")

    // Validate required parameters
    val host = config.connectionParams.getOrElse(
      "host",
      throw new IllegalArgumentException("host is required for MySQL source")
    )

    val port = config.connectionParams.getOrElse("port", "3306")

    val database = config.connectionParams.getOrElse(
      "database",
      throw new IllegalArgumentException("database is required for MySQL source")
    )

    val user = config.connectionParams.getOrElse(
      "user",
      throw new IllegalArgumentException("user is required for MySQL source")
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
          "Either 'table' or 'query' must be specified for MySQL source"
        )
    }

    // Build JDBC URL with optional parameters
    val baseUrl = s"jdbc:mysql://$host:$port/$database"

    val urlParams = Seq(
      config.connectionParams.get("useSSL").map(v => s"useSSL=$v"),
      config.connectionParams.get("requireSSL").map(v => s"requireSSL=$v"),
      config.connectionParams.get("serverTimezone").map(v => s"serverTimezone=$v")
    ).flatten

    val jdbcUrl = if (urlParams.nonEmpty) {
      s"$baseUrl?${urlParams.mkString("&")}"
    } else {
      baseUrl
    }

    logger.info(s"Connecting to MySQL: $baseUrl")

    // Build JDBC reader
    var reader = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", dbtable)
      .option("user", user)
      .option("driver", "com.mysql.cj.jdbc.Driver")

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
      s"Successfully loaded MySQL data. " +
        s"Row count: ${df.count()}, " +
        s"Schema: ${df.schema.fieldNames.mkString(", ")}"
    )

    df
  }
}
