package com.etl.core

import com.etl.config.{CredentialVault, PipelineConfig}
import com.etl.model.ExecutionMetrics
import org.apache.spark.sql.SparkSession

import java.util.UUID

/**
 * Execution context for pipeline runs.
 * Contains shared resources and state for a single pipeline execution.
 *
 * @param spark SparkSession for DataFrame operations
 * @param config Pipeline configuration
 * @param vault Credential vault for secure credential access
 * @param metrics Execution metrics (mutable during execution)
 * @param traceId Unique trace identifier for this execution
 */
case class ExecutionContext(
  spark: SparkSession,
  config: PipelineConfig,
  vault: CredentialVault,
  var metrics: ExecutionMetrics,
  traceId: String = UUID.randomUUID().toString
) {

  /**
   * Update metrics with new values.
   *
   * @param updatedMetrics New metrics
   */
  def updateMetrics(updatedMetrics: ExecutionMetrics): Unit = {
    metrics = updatedMetrics
  }

  /**
   * Get MDC context for logging.
   * Returns map of contextual fields to be added to logs.
   *
   * @return Map of MDC context fields
   */
  def getMDCContext: Map[String, String] = Map(
    "pipelineId"  -> config.pipelineId,
    "traceId"     -> traceId,
    "executionId" -> metrics.executionId
  )
}

object ExecutionContext {

  /**
   * Create execution context for a pipeline run.
   *
   * @param spark SparkSession
   * @param config Pipeline configuration
   * @param vault Credential vault
   * @return ExecutionContext with initialized metrics
   */
  def create(spark: SparkSession, config: PipelineConfig, vault: CredentialVault): ExecutionContext = {
    val executionId = UUID.randomUUID().toString
    val metrics = ExecutionMetrics.initial(config.pipelineId, executionId)

    ExecutionContext(
      spark = spark,
      config = config,
      vault = vault,
      metrics = metrics,
      traceId = UUID.randomUUID().toString
    )
  }
}
