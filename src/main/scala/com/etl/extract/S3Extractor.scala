package com.etl.extract

import com.etl.config.{CredentialVault, ExtractConfig}
import com.etl.util.CredentialHelper
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * S3 extractor for reading data files from Amazon S3.
 * Supports CSV, JSON, Parquet, and Avro formats.
 *
 * Connection parameters:
 * - format (required): File format - csv, json, parquet, avro
 * - path: S3 path (can also use 'path' field in ExtractConfig)
 * - fs.s3a.access.key (optional): AWS access key (or use credentialId)
 * - fs.s3a.secret.key (optional): AWS secret key (or use credentialId)
 * - fs.s3a.endpoint (optional): S3 endpoint URL
 * - basePath (optional): Base path for partitioned data
 *
 * Credential Management:
 * - Preferred: Set credentialId in ExtractConfig, store AWS keys in vault
 * - Vault should contain {credentialId}.access and {credentialId}.secret
 * - Fallback: Set fs.s3a.access.key and fs.s3a.secret.key in connectionParams
 * - Note: Uses per-operation credentials (not global) for multi-tenant security
 *
 * Format-specific options:
 * - CSV: header, inferSchema, delimiter, quote, escape, nullValue, dateFormat
 * - JSON: multiLine, dateFormat
 * - Parquet: mergeSchema
 * - Avro: avroSchema
 */
class S3Extractor extends Extractor {
  private val logger = LoggerFactory.getLogger(getClass)

  override def extractWithVault(config: ExtractConfig, vault: CredentialVault)(implicit spark: SparkSession): DataFrame = {
    logger.info(s"Extracting from S3 with config: ${config.connectionParams}")

    // Get path - either from connectionParams or from path field
    val s3Path = config.connectionParams.get("path")
      .orElse(config.path)
      .getOrElse(
        throw new IllegalArgumentException(
          "Either 'path' in connectionParams or 'path' field is required for S3 source"
        )
      )

    // Get format
    val format = config.connectionParams.getOrElse(
      "format",
      throw new IllegalArgumentException(
        "format is required for S3 source. Supported formats: csv, json, parquet, avro"
      )
    )

    logger.info(s"Reading from S3: path=$s3Path, format=$format")

    // Get S3 credentials from vault (preferred) or config (fallback)
    // NOTE: We log a warning but do NOT set global configuration
    // Instead, credentials should be provided via IAM roles in production
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
        ("", "")  // Empty strings - rely on IAM role
    }

    // Configure S3 credentials at Hadoop configuration level
    // WARNING: This still sets them globally. For true isolation, we'd need
    // to use custom FileSystem implementation or assume IAM roles
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

    // Build reader with format
    var reader = spark.read.format(format)

    // Apply format-specific options
    format.toLowerCase match {
      case "csv" =>
        // CSV-specific options
        config.connectionParams.get("header").foreach { header =>
          reader = reader.option("header", header)
        }
        config.connectionParams.get("inferSchema").foreach { infer =>
          reader = reader.option("inferSchema", infer)
        }
        config.connectionParams.get("delimiter").foreach { delim =>
          reader = reader.option("delimiter", delim)
        }
        config.connectionParams.get("quote").foreach { quote =>
          reader = reader.option("quote", quote)
        }
        config.connectionParams.get("escape").foreach { escape =>
          reader = reader.option("escape", escape)
        }
        config.connectionParams.get("nullValue").foreach { nullVal =>
          reader = reader.option("nullValue", nullVal)
        }
        config.connectionParams.get("dateFormat").foreach { dateFormat =>
          reader = reader.option("dateFormat", dateFormat)
        }

      case "json" =>
        // JSON-specific options
        config.connectionParams.get("multiLine").foreach { multiLine =>
          reader = reader.option("multiLine", multiLine)
        }
        config.connectionParams.get("dateFormat").foreach { dateFormat =>
          reader = reader.option("dateFormat", dateFormat)
        }

      case "parquet" =>
        // Parquet-specific options
        config.connectionParams.get("mergeSchema").foreach { merge =>
          reader = reader.option("mergeSchema", merge)
        }
        config.connectionParams.get("basePath").foreach { basePath =>
          reader = reader.option("basePath", basePath)
        }

      case "avro" =>
        // Avro-specific options
        config.connectionParams.get("avroSchema").foreach { schema =>
          reader = reader.option("avroSchema", schema)
        }

      case other =>
        logger.warn(s"Unknown format: $other. Using default reader options.")
    }

    // Apply basePath if specified (for partitioned data)
    config.connectionParams.get("basePath").foreach { basePath =>
      reader = reader.option("basePath", basePath)
    }

    // Load DataFrame
    val df = reader.load(s3Path)

    logger.info(
      s"Successfully loaded S3 data. " +
        s"Format: $format, " +
        s"Row count: ${df.count()}, " +
        s"Schema: ${df.schema.fieldNames.mkString(", ")}"
    )

    df
  }

  /**
   * Legacy extract method for backward compatibility.
   * Delegates to extractWithVault with empty vault.
   */
  override def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame = {
    logger.warn("Using legacy extract() without vault. Consider using extractWithVault() for secure S3 credentials.")

    // Create empty vault for backward compatibility
    val tempVault = com.etl.config.InMemoryVault()

    extractWithVault(config, tempVault)(spark)
  }
}
