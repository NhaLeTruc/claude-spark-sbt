package com.etl.model

/**
 * Pipeline execution state.
 * Represents the current status of a pipeline execution.
 */
sealed trait PipelineState

object PipelineState {
  /** Pipeline has been created but not yet started */
  case object Created extends PipelineState

  /** Pipeline is currently executing */
  case object Running extends PipelineState

  /** Pipeline failed and is being retried */
  case object Retrying extends PipelineState

  /** Pipeline completed successfully */
  case object Success extends PipelineState

  /**
   * Pipeline failed after exhausting retries.
   *
   * @param error The exception that caused the failure
   * @param retryCount Number of retry attempts made
   */
  case class Failed(error: Throwable, retryCount: Int) extends PipelineState
}
