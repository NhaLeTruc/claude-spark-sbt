package com.etl.model

/**
 * Telemetry data collected during pipeline execution.
 *
 * @param pipelineId Unique identifier of the pipeline
 * @param executionId Unique identifier for this execution instance
 * @param startTime Execution start timestamp (epoch millis)
 * @param endTime Execution end timestamp (epoch millis), None if still running
 * @param recordsExtracted Count of records extracted from source
 * @param recordsTransformed Count of records after transformation
 * @param recordsLoaded Count of records successfully loaded to sink
 * @param recordsFailed Count of records that failed validation or loading
 * @param retryCount Number of retry attempts made
 * @param errors List of error messages encountered
 */
case class ExecutionMetrics(
  pipelineId: String,
  executionId: String,
  startTime: Long,
  endTime: Option[Long] = None,
  recordsExtracted: Long = 0L,
  recordsTransformed: Long = 0L,
  recordsLoaded: Long = 0L,
  recordsFailed: Long = 0L,
  retryCount: Int = 0,
  errors: Seq[String] = Seq.empty
) {

  /**
   * Calculate execution duration in milliseconds.
   *
   * @return Duration in millis, None if execution not yet complete
   */
  def duration: Option[Long] = endTime.map(_ - startTime)

  /**
   * Calculate success rate as percentage of records successfully loaded.
   *
   * @return Success rate between 0.0 and 1.0, or 0.0 if no records extracted
   */
  def successRate: Double =
    if (recordsExtracted > 0) recordsLoaded.toDouble / recordsExtracted.toDouble else 0.0

  /**
   * Mark execution as complete with current timestamp.
   *
   * @return Updated metrics with endTime set
   */
  def complete(): ExecutionMetrics = this.copy(endTime = Some(System.currentTimeMillis()))

  /**
   * Increment retry count.
   *
   * @return Updated metrics with incremented retryCount
   */
  def incrementRetry(): ExecutionMetrics = this.copy(retryCount = retryCount + 1)

  /**
   * Add error message to errors list.
   *
   * @param errorMessage Error message to add
   * @return Updated metrics with error appended
   */
  def addError(errorMessage: String): ExecutionMetrics =
    this.copy(errors = errors :+ errorMessage)
}

object ExecutionMetrics {

  /**
   * Create initial metrics for a new pipeline execution.
   *
   * @param pipelineId Pipeline identifier
   * @param executionId Execution identifier
   * @return ExecutionMetrics with startTime set to current time
   */
  def initial(pipelineId: String, executionId: String): ExecutionMetrics =
    ExecutionMetrics(
      pipelineId = pipelineId,
      executionId = executionId,
      startTime = System.currentTimeMillis()
    )
}
