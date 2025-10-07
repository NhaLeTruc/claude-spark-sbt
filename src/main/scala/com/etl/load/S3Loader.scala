package com.etl.load

import com.etl.config.LoadConfig
import com.etl.model.{LoadResult, WriteMode}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

/**
 * S3 loader for writing data files to Amazon S3.
 * Supports CSV, JSON, Parquet, and Avro formats.
 *
 * Connection parameters:
 * - format (required): File format - csv, json, parquet, avro
 * - path: S3 path (can also use 'path' field in LoadConfig)
 * - fs.s3a.access.key (optional): AWS access key
 * - fs.s3a.secret.key (optional): AWS secret key
 * - fs.s3a.endpoint (optional): S3 endpoint URL
 * - partitionBy (optional): JSON array of columns to partition by, e.g., ["date", "region"]
 * - compression (optional): Compression codec (snappy, gzip, lz4, etc.)
 *
 * Format-specific options:
 * - CSV: header, delimiter, quote, escape, nullValue
 * - JSON: compression, dateFormat
 * - Parquet: compression (default: snappy)
 * - Avro: compression
 *
 * Note: S3 does not support Upsert mode. Only Append and Overwrite are supported.
 */
class S3Loader extends Loader {
  private val logger = LoggerFactory.getLogger(getClass)

  override def load(df: DataFrame, config: LoadConfig, mode: WriteMode): LoadResult = {
    logger.info(s"Loading to S3 with config: ${config.connectionParams}, mode: $mode")

    // Validate write mode - S3 doesn't support Upsert
    if (mode == WriteMode.Upsert) {
      throw new IllegalArgumentException(
        "S3 does not support Upsert mode. Use Append or Overwrite."
      )
    }

    // Get path - either from connectionParams or from path field
    val s3Path = config.connectionParams.get("path")
      .orElse(config.path)
      .getOrElse(
        throw new IllegalArgumentException(
          "Either 'path' in connectionParams or 'path' field is required for S3 sink"
        )
      )

    // Get format
    val format = config.connectionParams.getOrElse(
      "format",
      throw new IllegalArgumentException(
        "format is required for S3 sink. Supported formats: csv, json, parquet, avro"
      )
    )

    logger.info(s"Writing to S3: path=$s3Path, format=$format, mode=$mode")

    try {
      // Configure S3 credentials if provided
      config.connectionParams.get("fs.s3a.access.key").foreach { accessKey =>
        df.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", accessKey)
      }

      config.connectionParams.get("fs.s3a.secret.key").foreach { secretKey =>
        df.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secretKey)
      }

      config.connectionParams.get("fs.s3a.endpoint").foreach { endpoint =>
        df.sparkSession.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", endpoint)
      }

      val recordCount = df.count()

      // Determine SaveMode
      val saveMode = mode match {
        case WriteMode.Append => SaveMode.Append
        case WriteMode.Overwrite => SaveMode.Overwrite
        case WriteMode.Upsert => throw new IllegalArgumentException("Upsert not supported for S3")
      }

      // Build writer with format
      var writer = df.write.format(format).mode(saveMode)

      // Apply partitioning if specified
      config.connectionParams.get("partitionBy").foreach { partitionByJson =>
        val partitionColumns = Json.parse(partitionByJson).as[Seq[String]]
        if (partitionColumns.nonEmpty) {
          logger.info(s"Partitioning by: ${partitionColumns.mkString(", ")}")
          writer = writer.partitionBy(partitionColumns: _*)
        }
      }

      // Apply format-specific options
      format.toLowerCase match {
        case "csv" =>
          config.connectionParams.get("header").foreach { header =>
            writer = writer.option("header", header)
          }
          config.connectionParams.get("delimiter").foreach { delim =>
            writer = writer.option("delimiter", delim)
          }
          config.connectionParams.get("quote").foreach { quote =>
            writer = writer.option("quote", quote)
          }
          config.connectionParams.get("escape").foreach { escape =>
            writer = writer.option("escape", escape)
          }
          config.connectionParams.get("nullValue").foreach { nullVal =>
            writer = writer.option("nullValue", nullVal)
          }

        case "json" =>
          config.connectionParams.get("compression").foreach { comp =>
            writer = writer.option("compression", comp)
          }
          config.connectionParams.get("dateFormat").foreach { dateFormat =>
            writer = writer.option("dateFormat", dateFormat)
          }

        case "parquet" =>
          val compression = config.connectionParams.getOrElse("compression", "snappy")
          writer = writer.option("compression", compression)

        case "avro" =>
          config.connectionParams.get("compression").foreach { comp =>
            writer = writer.option("compression", comp)
          }

        case other =>
          logger.warn(s"Unknown format: $other. Using default writer options.")
      }

      // Execute write
      writer.save(s3Path)

      logger.info(
        s"Successfully wrote $recordCount records to S3. " +
          s"Path: $s3Path, Format: $format, Mode: $mode"
      )

      LoadResult.success(recordCount)

    } catch {
      case e: Exception =>
        val errorMsg = s"Failed to write to S3 path $s3Path: ${e.getMessage}"
        logger.error(errorMsg, e)
        LoadResult.failure(0L, df.count(), errorMsg)
    }
  }
}
