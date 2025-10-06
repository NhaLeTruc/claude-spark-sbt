package com.etl.model

/**
 * Outcome of a pipeline execution.
 * Either success with metrics, or failure with error and metrics.
 */
sealed trait PipelineResult {
  def metrics: ExecutionMetrics
  def isSuccess: Boolean
}

/**
 * Pipeline completed successfully.
 *
 * @param metrics Execution telemetry data
 */
case class PipelineSuccess(metrics: ExecutionMetrics) extends PipelineResult {
  override def isSuccess: Boolean = true
}

/**
 * Pipeline failed after exhausting retries.
 *
 * @param metrics Execution telemetry data
 * @param error Exception that caused the failure
 */
case class PipelineFailure(metrics: ExecutionMetrics, error: Throwable) extends PipelineResult {
  override def isSuccess: Boolean = false
}
