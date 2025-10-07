package com.etl.core

import com.etl.config.PipelineConfig
import com.etl.model.{PipelineFailure, PipelineResult, PipelineSuccess}
import com.etl.util.{Logging, Retry}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Executes pipelines with retry logic and metrics tracking.
 * Integrates retry utility and wraps pipeline execution with proper error handling.
 */
class PipelineExecutor extends Logging {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Execute a pipeline with retry logic.
   *
   * @param pipeline Pipeline to execute
   * @param config Pipeline configuration (includes retry settings)
   * @param spark SparkSession for execution
   * @return PipelineResult with success or failure details
   */
  def execute(pipeline: Pipeline, config: PipelineConfig)(implicit spark: SparkSession): PipelineResult = {
    logger.info(s"Starting pipeline execution: ${config.pipelineId}")

    // Create execution context
    val context = ExecutionContext.create(spark, config)

    // Log with MDC context
    withMDC(context.getMDCContext) {
      logger.info(
        s"Pipeline execution started. " +
          s"Retry config: maxAttempts=${config.retry.maxAttempts}, " +
          s"delaySeconds=${config.retry.delaySeconds}"
      )

      // Execute with retry logic
      val result = Retry.withRetry(
        maxAttempts = config.retry.maxAttempts,
        delayMillis = config.retry.delaySeconds * 1000L
      ) {
        executePipeline(pipeline, context)
      }

      // Handle result
      result match {
        case Right(pipelineResult) =>
          logger.info(
            s"Pipeline execution completed successfully. " +
              s"Records: extracted=${pipelineResult.metrics.recordsExtracted}, " +
              s"transformed=${pipelineResult.metrics.recordsTransformed}, " +
              s"loaded=${pipelineResult.metrics.recordsLoaded}, " +
              s"failed=${pipelineResult.metrics.recordsFailed}, " +
              s"retries=${pipelineResult.metrics.retryCount}, " +
              s"duration=${pipelineResult.metrics.duration}ms"
          )
          pipelineResult

        case Left(error) =>
          val finalMetrics = context.metrics.complete()
          logger.error(
            s"Pipeline execution failed after ${config.retry.maxAttempts} attempts. " +
              s"Error: ${error.getMessage}",
            Some(error)
          )
          PipelineFailure(finalMetrics, error)
      }
    }
  }

  /**
   * Execute pipeline once (called by retry logic).
   *
   * @param pipeline Pipeline to execute
   * @param context Execution context
   * @return PipelineResult
   */
  private def executePipeline(pipeline: Pipeline, context: ExecutionContext): PipelineResult = {
    withMDC(context.getMDCContext) {
      logger.info("Executing pipeline run")

      try {
        // Execute pipeline
        val result = pipeline.run(context)

        // Update context metrics with result
        context.updateMetrics(result.metrics)

        result match {
          case success: PipelineSuccess =>
            logger.info(
              s"Pipeline run succeeded. " +
                s"Records loaded: ${success.metrics.recordsLoaded}"
            )
            success

          case failure: PipelineFailure =>
            logger.error(
              s"Pipeline run failed: ${failure.error.getMessage}",
              Some(failure.error)
            )
            throw failure.error // Throw to trigger retry
        }

      } catch {
        case e: Exception =>
          logger.error(s"Pipeline run failed with exception: ${e.getMessage}", Some(e))
          throw e // Re-throw to trigger retry
      }
    }
  }
}

object PipelineExecutor {
  /**
   * Create a new PipelineExecutor instance.
   *
   * @return PipelineExecutor
   */
  def apply(): PipelineExecutor = new PipelineExecutor()
}
