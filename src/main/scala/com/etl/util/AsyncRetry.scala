package com.etl.util

import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Random, Success}

/**
 * Non-blocking retry utility using Scala Futures.
 * Prevents thread blocking that occurs with Thread.sleep().
 *
 * Example usage:
 * {{{
 *   implicit val ec: ExecutionContext = ExecutionContext.global
 *
 *   val result: Future[DataFrame] = AsyncRetry.withRetry(
 *     maxAttempts = 3,
 *     initialDelay = 1.second,
 *     maxDelay = 30.seconds
 *   ) {
 *     // Your operation that might fail
 *     extractor.extract(config)
 *   }
 * }}}
 */
object AsyncRetry {
  private val logger = LoggerFactory.getLogger(getClass)
  private val random = new Random()

  /**
   * Configuration for async retry behavior.
   *
   * @param maxAttempts Maximum number of retry attempts (including initial try)
   * @param initialDelay Initial delay before first retry
   * @param maxDelay Maximum delay between retries (caps exponential backoff)
   * @param backoffMultiplier Multiplier for exponential backoff (default: 2.0)
   * @param jitter Whether to add random jitter to delays (default: true)
   * @param retryableExceptions Optional set of exception types to retry (None = retry all)
   */
  case class RetryConfig(
    maxAttempts: Int = 3,
    initialDelay: FiniteDuration = 1.second,
    maxDelay: FiniteDuration = 1.minute,
    backoffMultiplier: Double = 2.0,
    jitter: Boolean = true,
    retryableExceptions: Option[Set[Class[_ <: Throwable]]] = None
  ) {
    require(maxAttempts > 0, "maxAttempts must be positive")
    require(initialDelay.toMillis > 0, "initialDelay must be positive")
    require(maxDelay >= initialDelay, "maxDelay must be >= initialDelay")
    require(backoffMultiplier >= 1.0, "backoffMultiplier must be >= 1.0")
  }

  /**
   * Execute operation with async retry logic.
   *
   * @param config Retry configuration
   * @param operation Operation to execute (by-name parameter)
   * @param ec Execution context for async operations
   * @tparam T Return type of operation
   * @return Future containing result or final error
   */
  def withRetry[T](config: RetryConfig)(operation: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    def attemptOperation(attemptNumber: Int, lastError: Option[Throwable]): Future[T] = {
      if (attemptNumber > config.maxAttempts) {
        // Exhausted all retries
        val error = lastError.getOrElse(
          new RuntimeException(s"Operation failed after ${config.maxAttempts} attempts")
        )
        logger.error(s"All retry attempts exhausted: ${error.getMessage}")
        Future.failed(error)
      } else {
        // Try operation
        operation.recoverWith {
          case error: Throwable if shouldRetry(error, config) =>
            val remainingAttempts = config.maxAttempts - attemptNumber

            if (remainingAttempts > 0) {
              val delay = calculateDelay(attemptNumber, config)

              logger.warn(
                s"Attempt $attemptNumber failed: ${error.getMessage}. " +
                  s"Retrying in ${delay.toMillis}ms... ($remainingAttempts attempts remaining)"
              )

              // Non-blocking delay using Future
              delayedFuture(delay).flatMap { _ =>
                attemptOperation(attemptNumber + 1, Some(error))
              }
            } else {
              logger.error(s"Operation failed on final attempt $attemptNumber: ${error.getMessage}")
              Future.failed(error)
            }

          case error: Throwable =>
            // Non-retryable error
            logger.error(s"Non-retryable error: ${error.getMessage}")
            Future.failed(error)
        }
      }
    }

    if (config.maxAttempts == 1) {
      logger.info("Executing operation without retry (maxAttempts = 1)")
    } else {
      logger.info(s"Executing operation with retry (maxAttempts = ${config.maxAttempts})")
    }

    attemptOperation(attemptNumber = 1, lastError = None)
  }

  /**
   * Convenience method with common defaults.
   *
   * @param maxAttempts Maximum retry attempts
   * @param initialDelay Initial delay before retry
   * @param maxDelay Maximum delay cap
   * @param operation Operation to execute
   * @param ec Execution context
   * @tparam T Return type
   * @return Future with result or error
   */
  def withRetry[T](
    maxAttempts: Int,
    initialDelay: FiniteDuration,
    maxDelay: FiniteDuration
  )(operation: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    withRetry(RetryConfig(
      maxAttempts = maxAttempts,
      initialDelay = initialDelay,
      maxDelay = maxDelay
    ))(operation)
  }

  /**
   * Create a non-blocking delayed future.
   * Uses scheduled executor instead of Thread.sleep().
   *
   * @param delay Duration to delay
   * @param ec Execution context
   * @return Future that completes after delay
   */
  private def delayedFuture(delay: FiniteDuration)(implicit ec: ExecutionContext): Future[Unit] = {
    val promise = Promise[Unit]()

    // Use Java's ScheduledExecutorService for non-blocking delay
    val scheduler = java.util.concurrent.Executors.newScheduledThreadPool(1)

    scheduler.schedule(
      new Runnable {
        override def run(): Unit = {
          promise.success(())
          scheduler.shutdown()
        }
      },
      delay.toMillis,
      java.util.concurrent.TimeUnit.MILLISECONDS
    )

    promise.future
  }

  /**
   * Calculate delay for current attempt using exponential backoff with jitter.
   *
   * @param attemptNumber Current attempt number (1-based)
   * @param config Retry configuration
   * @return Calculated delay duration
   */
  private def calculateDelay(attemptNumber: Int, config: RetryConfig): FiniteDuration = {
    // Exponential backoff: initialDelay * (backoffMultiplier ^ (attemptNumber - 1))
    val exponentialDelay = config.initialDelay.toMillis * math.pow(
      config.backoffMultiplier,
      attemptNumber - 1
    ).toLong

    // Cap at maxDelay
    val cappedDelay = math.min(exponentialDelay, config.maxDelay.toMillis)

    // Add jitter if enabled (random 0-25% reduction)
    val finalDelay = if (config.jitter) {
      val jitterFactor = 0.75 + (random.nextDouble() * 0.25) // 0.75 to 1.0
      (cappedDelay * jitterFactor).toLong
    } else {
      cappedDelay
    }

    FiniteDuration(finalDelay, MILLISECONDS)
  }

  /**
   * Determine if an exception should trigger a retry.
   *
   * @param error The exception that occurred
   * @param config Retry configuration
   * @return true if should retry, false otherwise
   */
  private def shouldRetry(error: Throwable, config: RetryConfig): Boolean = {
    config.retryableExceptions match {
      case Some(retryableTypes) =>
        // Only retry if exception type matches configured list
        retryableTypes.exists(_.isInstance(error))
      case None =>
        // Retry all exceptions by default
        true
    }
  }

  /**
   * Retry with custom predicate for determining retry eligibility.
   *
   * @param config Retry configuration
   * @param shouldRetryFn Custom function to determine if error is retryable
   * @param operation Operation to execute
   * @param ec Execution context
   * @tparam T Return type
   * @return Future with result or error
   */
  def withRetryPredicate[T](
    config: RetryConfig,
    shouldRetryFn: Throwable => Boolean
  )(operation: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
    def attemptOperation(attemptNumber: Int, lastError: Option[Throwable]): Future[T] = {
      if (attemptNumber > config.maxAttempts) {
        val error = lastError.getOrElse(
          new RuntimeException(s"Operation failed after ${config.maxAttempts} attempts")
        )
        Future.failed(error)
      } else {
        operation.recoverWith {
          case error: Throwable if shouldRetryFn(error) =>
            val remainingAttempts = config.maxAttempts - attemptNumber

            if (remainingAttempts > 0) {
              val delay = calculateDelay(attemptNumber, config)

              logger.warn(
                s"Attempt $attemptNumber failed (retryable): ${error.getMessage}. " +
                  s"Retrying in ${delay.toMillis}ms..."
              )

              delayedFuture(delay).flatMap { _ =>
                attemptOperation(attemptNumber + 1, Some(error))
              }
            } else {
              Future.failed(error)
            }
        }
      }
    }

    attemptOperation(attemptNumber = 1, lastError = None)
  }
}

/**
 * Implicit conversions and helpers for AsyncRetry.
 */
object AsyncRetryImplicits {

  /**
   * Enrichment for Future to add retry capability.
   */
  implicit class FutureRetryOps[T](future: => Future[T]) {

    /**
     * Retry this future with specified configuration.
     */
    def withAsyncRetry(config: AsyncRetry.RetryConfig)(implicit ec: ExecutionContext): Future[T] = {
      AsyncRetry.withRetry(config)(future)
    }

    /**
     * Retry this future with simple parameters.
     */
    def withAsyncRetry(
      maxAttempts: Int,
      initialDelay: FiniteDuration,
      maxDelay: FiniteDuration
    )(implicit ec: ExecutionContext): Future[T] = {
      AsyncRetry.withRetry(maxAttempts, initialDelay, maxDelay)(future)
    }
  }
}
