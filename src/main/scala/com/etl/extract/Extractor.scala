package com.etl.extract

import com.etl.config.{CredentialVault, ExtractConfig}
import com.etl.core.ExecutionContext
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Extractor interface (Strategy pattern).
 * All data source extractors must implement this trait to read data
 * from external systems into Spark DataFrames.
 *
 * Implementations should:
 * - Read data from configured source (Kafka, JDBC, S3, etc.)
 * - Handle authentication via CredentialVault
 * - Return DataFrame with data matching expected schema
 * - Throw exceptions on connection/read failures
 *
 * Example implementations:
 * - KafkaExtractor: Read from Kafka topics
 * - PostgreSQLExtractor: Execute SQL queries on PostgreSQL
 * - MySQLExtractor: Execute SQL queries on MySQL
 * - S3Extractor: Read files from S3 (CSV, Avro, etc.)
 */
trait Extractor {

  /**
   * Extract data from configured source.
   *
   * @param config Extract configuration (connection params, query, topic, path)
   * @param spark Implicit SparkSession for DataFrame operations
   * @return DataFrame containing extracted data
   * @throws Exception if extraction fails (connection error, invalid config, etc.)
   */
  def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame

  /**
   * Extract data from configured source with vault access.
   * This method provides access to CredentialVault for secure credential retrieval.
   * Default implementation delegates to extract(config) for backward compatibility.
   *
   * @param config Extract configuration
   * @param vault Credential vault for secure credential access
   * @param spark Implicit SparkSession
   * @return DataFrame containing extracted data
   */
  def extractWithVault(config: ExtractConfig, vault: CredentialVault)(implicit spark: SparkSession): DataFrame = {
    extract(config)(spark)
  }
}
