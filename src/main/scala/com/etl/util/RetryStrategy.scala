package com.etl.util

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.util.Random
import org.slf4j.LoggerFactory

/**
 * Advanced retry strategy with exponential backoff and jitter.
 *
 * Provides configurable retry behavior for transient failures in distributed systems.
 * Supports exponential backoff to avoid overwhelming failing systems and jitter to
 * prevent thundering herd problems.
 */
sealed trait RetryStrategy {
  /**
   * Execute an operation with retry logic.
   *
   * @param operation The operation to execute
   * @tparam T Return type of the operation
   * @return Either the successful result or the final error
   */
  def execute[T](operation: => T): Either[Throwable, T]
}

/**
 * Retry configuration with exponential backoff.
 *
 * @param maxAttempts Maximum number of retry attempts (including initial attempt)
 * @param initialDelay Initial delay before first retry
 * @param maxDelay Maximum delay between retries
 * @param backoffMultiplier Multiplier for exponential backoff (default: 2.0)
 * @param jitter Whether to add random jitter to delay (default: true)
 */
case class ExponentialBackoffConfig(
  maxAttempts: Int,
  initialDelay: FiniteDuration,
  maxDelay: FiniteDuration,
  backoffMultiplier: Double = 2.0,
  jitter: Boolean = true
) {
  require(maxAttempts > 0, "maxAttempts must be positive")
  require(initialDelay.toMillis > 0, "initialDelay must be positive")
  require(maxDelay.toMillis >= initialDelay.toMillis, "maxDelay must be >= initialDelay")
  require(backoffMultiplier >= 1.0, "backoffMultiplier must be >= 1.0")
}

/**
 * Retry strategy with exponential backoff and optional jitter.
 *
 * Delay calculation:
 * - Without jitter: delay = min(initialDelay * (multiplier ^ attempt), maxDelay)
 * - With jitter: delay = delay * random(0.5, 1.0)
 *
 * Example: initialDelay=1s, multiplier=2.0
 * - Attempt 1: 1s
 * - Attempt 2: 2s
 * - Attempt 3: 4s
 * - Attempt 4: 8s
 * - ...
 */
class ExponentialBackoffRetry(config: ExponentialBackoffConfig) extends RetryStrategy {
  private val logger = LoggerFactory.getLogger(getClass)
  private val random = new Random()

  override def execute[T](operation: => T): Either[Throwable, T] = {
    @scala.annotation.tailrec
    def attempt(attemptNumber: Int, currentDelay: FiniteDuration): Either[Throwable, T] = {
      Try(operation) match {
        case Success(result) =>
          if (attemptNumber > 1) {
            logger.info(s"Operation succeeded after $attemptNumber attempts")
          }
          Right(result)

        case Failure(error) if attemptNumber < config.maxAttempts =>
          val delayWithJitter = if (config.jitter) {
            // Add jitter: 50-100% of calculated delay
            val jitterFactor = 0.5 + (random.nextDouble() * 0.5)
            (currentDelay.toMillis * jitterFactor).toLong.millis
          } else {
            currentDelay
          }

          logger.warn(
            s"Operation failed (attempt $attemptNumber/${config.maxAttempts}). " +
              s"Retrying in ${delayWithJitter.toMillis}ms. " +
              s"Error: ${error.getClass.getSimpleName}: ${error.getMessage}"
          )

          Thread.sleep(delayWithJitter.toMillis)

          // Calculate next delay with exponential backoff
          val nextDelay = FiniteDuration(
            math.min(
              (currentDelay.toMillis * config.backoffMultiplier).toLong,
              config.maxDelay.toMillis
            ),
            MILLISECONDS
          )

          attempt(attemptNumber + 1, nextDelay)

        case Failure(error) =>
          logger.error(
            s"Operation failed after ${config.maxAttempts} attempts. " +
              s"Final error: ${error.getClass.getSimpleName}: ${error.getMessage}"
          )
          Left(error)
      }
    }

    attempt(1, config.initialDelay)
  }
}

/**
 * Fixed delay retry strategy (legacy compatibility).
 */
class FixedDelayRetry(maxAttempts: Int, delay: FiniteDuration) extends RetryStrategy {
  private val logger = LoggerFactory.getLogger(getClass)

  override def execute[T](operation: => T): Either[Throwable, T] = {
    @scala.annotation.tailrec
    def attempt(attemptNumber: Int): Either[Throwable, T] = {
      Try(operation) match {
        case Success(result) => Right(result)

        case Failure(error) if attemptNumber < maxAttempts =>
          logger.warn(s"Retry attempt $attemptNumber/$maxAttempts after ${delay.toMillis}ms")
          Thread.sleep(delay.toMillis)
          attempt(attemptNumber + 1)

        case Failure(error) => Left(error)
      }
    }

    attempt(1)
  }
}

/**
 * No-retry strategy (fail immediately).
 */
class NoRetryStrategy extends RetryStrategy {
  override def execute[T](operation: => T): Either[Throwable, T] = {
    Try(operation).toEither
  }
}

object RetryStrategy {
  /**
   * Create exponential backoff retry strategy.
   */
  def exponentialBackoff(config: ExponentialBackoffConfig): RetryStrategy = {
    new ExponentialBackoffRetry(config)
  }

  /**
   * Create exponential backoff retry strategy with simplified parameters.
   */
  def exponentialBackoff(
    maxAttempts: Int,
    initialDelayMs: Long,
    maxDelayMs: Long,
    backoffMultiplier: Double = 2.0,
    jitter: Boolean = true
  ): RetryStrategy = {
    exponentialBackoff(ExponentialBackoffConfig(
      maxAttempts = maxAttempts,
      initialDelay = initialDelayMs.millis,
      maxDelay = maxDelayMs.millis,
      backoffMultiplier = backoffMultiplier,
      jitter = jitter
    ))
  }

  /**
   * Create fixed delay retry strategy.
   */
  def fixedDelay(maxAttempts: Int, delayMs: Long): RetryStrategy = {
    new FixedDelayRetry(maxAttempts, delayMs.millis)
  }

  /**
   * No retry - fail immediately on first error.
   */
  def noRetry: RetryStrategy = new NoRetryStrategy()
}
