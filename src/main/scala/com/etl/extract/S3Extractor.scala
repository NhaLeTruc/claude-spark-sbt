package com.etl.extract

import com.etl.config.ExtractConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
 * S3 extractor for reading data files from Amazon S3.
 * Supports CSV, JSON, Parquet, and Avro formats.
 *
 * Connection parameters:
 * - format (required): File format - csv, json, parquet, avro
 * - path: S3 path (can also use 'path' field in ExtractConfig)
 * - fs.s3a.access.key (optional): AWS access key
 * - fs.s3a.secret.key (optional): AWS secret key
 * - fs.s3a.endpoint (optional): S3 endpoint URL
 * - basePath (optional): Base path for partitioned data
 *
 * Format-specific options:
 * - CSV: header, inferSchema, delimiter, quote, escape, nullValue, dateFormat
 * - JSON: multiLine, dateFormat
 * - Parquet: mergeSchema
 * - Avro: avroSchema
 */
class S3Extractor extends Extractor {
  private val logger = LoggerFactory.getLogger(getClass)

  override def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame = {
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

    // Configure S3 credentials if provided
    config.connectionParams.get("fs.s3a.access.key").foreach { accessKey =>
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
    }

    config.connectionParams.get("fs.s3a.secret.key").foreach { secretKey =>
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
    }

    config.connectionParams.get("fs.s3a.endpoint").foreach { endpoint =>
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", endpoint)
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
        s"Schema: ${df.schema.fieldNames.mkString(", ")}"
    )

    df
  }
}
