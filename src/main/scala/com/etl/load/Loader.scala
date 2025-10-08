package com.etl.load

import com.etl.config.{CredentialVault, LoadConfig}
import com.etl.model.{LoadResult, WriteMode}
import org.apache.spark.sql.DataFrame

/**
 * Loader interface (Strategy pattern).
 * All data sink loaders must implement this trait to write DataFrames
 * to external systems.
 *
 * Implementations should:
 * - Write DataFrame to configured sink (Kafka, JDBC, S3, etc.)
 * - Handle WriteMode (Append, Overwrite, Upsert)
 * - Use CredentialVault for authentication
 * - Return LoadResult with record counts and any errors
 * - For Upsert mode: use temporary tables + merge (JDBC) or deterministic IDs (others)
 *
 * Example implementations:
 * - KafkaLoader: Publish to Kafka topics
 * - PostgreSQLLoader: Write to PostgreSQL tables (supports upsert via MERGE)
 * - MySQLLoader: Write to MySQL tables
 * - S3Loader: Write files to S3 (Avro/Parquet with compression)
 *
 * Write mode semantics:
 * - Append: Add new records, keep existing data
 * - Overwrite: Replace all existing data
 * - Upsert: Insert new records, update existing (requires upsertKeys in config)
 */
trait Loader {

  /**
   * Load DataFrame to configured sink.
   *
   * @param df DataFrame to write
   * @param config Load configuration (connection params, table, topic, path)
   * @param mode Write mode (Append, Overwrite, Upsert)
   * @return LoadResult with record counts and any errors
   * @throws Exception if load fails (connection error, permission denied, etc.)
   */
  def load(df: DataFrame, config: LoadConfig, mode: WriteMode): LoadResult

  /**
   * Load DataFrame to configured sink with vault access.
   * This method provides access to CredentialVault for secure credential retrieval.
   * Default implementation delegates to load() for backward compatibility.
   *
   * @param df DataFrame to write
   * @param config Load configuration
   * @param mode Write mode
   * @param vault Credential vault for secure credential access
   * @return LoadResult with record counts and any errors
   */
  def loadWithVault(df: DataFrame, config: LoadConfig, mode: WriteMode, vault: CredentialVault): LoadResult = {
    load(df, config, mode)
  }
}
