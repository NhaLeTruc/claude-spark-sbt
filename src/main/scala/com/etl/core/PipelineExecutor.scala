package com.etl.core

import com.etl.config.PipelineConfig
import com.etl.model.{PipelineFailure, PipelineResult, PipelineSuccess}
import com.etl.util.{CircuitBreaker, Logging, Retry}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap

/**
 * Executes pipelines with retry logic, circuit breaker, and metrics tracking.
 * Integrates retry utility and wraps pipeline execution with proper error handling.
 * Circuit breakers are maintained per pipeline ID for isolation.
 */
class PipelineExecutor extends Logging {
  private val logger = LoggerFactory.getLogger(getClass)

  // Circuit breakers per pipeline ID (thread-safe)
  private val circuitBreakers = new TrieMap[String, CircuitBreaker]()

  /**
   * Execute a pipeline with retry logic and circuit breaker.
   *
   * @param pipeline Pipeline to execute
   * @param config Pipeline configuration (includes retry and circuit breaker settings)
   * @param spark SparkSession for execution
   * @return PipelineResult with success or failure details
   */
  def execute(pipeline: Pipeline, config: PipelineConfig)(implicit spark: SparkSession): PipelineResult = {
    logger.info(s"Starting pipeline execution: ${config.pipelineId}")

    // Create execution context
    val context = ExecutionContext.create(spark, config)

    // Get or create circuit breaker for this pipeline
    val circuitBreaker = getCircuitBreaker(config)

    // Log with MDC context
    withMDC(context.getMDCContext) {
      val cbConfig = config.errorHandlingConfig.circuitBreakerConfig

      logger.info(
        s"Pipeline execution started. " +
          s"Retry config: maxAttempts=${config.errorHandlingConfig.retryConfig.maxAttempts}, " +
          s"delaySeconds=${config.errorHandlingConfig.retryConfig.initialDelaySeconds}, " +
          s"Circuit breaker: enabled=${cbConfig.enabled}, " +
          s"failureThreshold=${cbConfig.failureThreshold}"
      )

      // Execute with circuit breaker and retry logic
      val result = if (cbConfig.enabled) {
        executeWithCircuitBreaker(pipeline, context, circuitBreaker, config)
      } else {
        executeWithRetryOnly(pipeline, context, config)
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
            s"Pipeline execution failed after ${config.errorHandlingConfig.retryConfig.maxAttempts} attempts. " +
              s"Error: ${error.getMessage}",
            Some(error)
          )
          PipelineFailure(finalMetrics, error)
      }
    }
  }

  /**
   * Get or create circuit breaker for a pipeline.
   */
  private def getCircuitBreaker(config: PipelineConfig): CircuitBreaker = {
    circuitBreakers.getOrElseUpdate(
      config.pipelineId,
      {
        val cbConfig = config.errorHandlingConfig.circuitBreakerConfig
        val cb = new CircuitBreaker(
          name = s"pipeline-${config.pipelineId}",
          failureThreshold = cbConfig.failureThreshold,
          resetTimeoutMillis = cbConfig.resetTimeoutSeconds * 1000L,
          halfOpenMaxAttempts = cbConfig.halfOpenMaxAttempts
        )
        logger.info(
          s"Created circuit breaker for pipeline ${config.pipelineId}: " +
            s"threshold=${cbConfig.failureThreshold}, " +
            s"resetTimeout=${cbConfig.resetTimeoutSeconds}s"
        )
        cb
      }
    )
  }

  /**
   * Execute with circuit breaker protection.
   */
  private def executeWithCircuitBreaker(
    pipeline: Pipeline,
    context: ExecutionContext,
    circuitBreaker: CircuitBreaker,
    config: PipelineConfig
  ): Either[Throwable, PipelineResult] = {
    // Check circuit breaker state before executing
    if (!circuitBreaker.canExecute) {
      val error = new RuntimeException(
        s"Circuit breaker is OPEN for pipeline ${config.pipelineId}. " +
          s"Failure rate: ${circuitBreaker.getFailureRate}%, " +
          s"State: ${circuitBreaker.getState}"
      )
      logger.error(error.getMessage)
      return Left(error)
    }

    // Execute with retry
    Retry.withRetry(
      maxAttempts = config.errorHandlingConfig.retryConfig.maxAttempts,
      delayMillis = config.errorHandlingConfig.retryConfig.initialDelaySeconds * 1000L
    ) {
      // Wrap in circuit breaker
      circuitBreaker.execute {
        executePipeline(pipeline, context)
      } match {
        case Right(result) => result
        case Left(error) => throw error
      }
    }
  }

  /**
   * Execute with retry only (no circuit breaker).
   */
  private def executeWithRetryOnly(
    pipeline: Pipeline,
    context: ExecutionContext,
    config: PipelineConfig
  ): Either[Throwable, PipelineResult] = {
    Retry.withRetry(
      maxAttempts = config.errorHandlingConfig.retryConfig.maxAttempts,
      delayMillis = config.errorHandlingConfig.retryConfig.initialDelaySeconds * 1000L
    ) {
      executePipeline(pipeline, context)
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
