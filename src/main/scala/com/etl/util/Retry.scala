package com.etl.util

import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
 * Retry utility for fault-tolerant operations.
 * Provides configurable retry logic with delays between attempts.
 */
object Retry {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Execute operation with retry logic.
   * Returns Either with success value or final error after exhausting retries.
   *
   * @param maxAttempts Maximum number of attempts (including first try)
   * @param delayMillis Delay between retry attempts in milliseconds
   * @param operation Operation to execute (by-name parameter, evaluated each attempt)
   * @tparam A Return type of operation
   * @return Right(result) on success, Left(exception) on failure after all retries
   */
  def withRetry[A](maxAttempts: Int, delayMillis: Long)(operation: => A): Either[Throwable, A] = {
    @tailrec
    def attempt(remainingAttempts: Int, attemptNumber: Int, lastError: Option[Throwable]): Either[Throwable, A] = {
      if (remainingAttempts <= 0) {
        // No attempts left
        lastError match {
          case Some(error) =>
            logger.error(s"Operation failed after $attemptNumber attempts: ${error.getMessage}")
            Left(error)
          case None =>
            Left(new RuntimeException("No attempts configured (maxAttempts = 0)"))
        }
      } else {
        // Try operation
        Try(operation) match {
          case Success(result) =>
            if (attemptNumber > 1) {
              logger.info(s"Operation succeeded on attempt $attemptNumber")
            }
            Right(result)

          case Failure(error) =>
            val newAttemptNumber = attemptNumber + 1
            val newRemainingAttempts = remainingAttempts - 1

            if (newRemainingAttempts > 0) {
              logger.warn(
                s"Attempt $attemptNumber failed: ${error.getMessage}. " +
                  s"Retrying in ${delayMillis}ms... ($newRemainingAttempts attempts remaining)"
              )
              Thread.sleep(delayMillis)
              attempt(newRemainingAttempts, newAttemptNumber, Some(error))
            } else {
              logger.error(s"Operation failed on final attempt $attemptNumber: ${error.getMessage}")
              Left(error)
            }
        }
      }
    }

    attempt(remainingAttempts = maxAttempts, attemptNumber = 1, lastError = None)
  }

  /**
   * Execute operation with retry using seconds instead of milliseconds.
   *
   * @param maxAttempts Maximum number of attempts
   * @param delaySeconds Delay between retries in seconds
   * @param operation Operation to execute
   * @tparam A Return type
   * @return Either with result or error
   */
  def withRetrySeconds[A](maxAttempts: Int, delaySeconds: Int)(operation: => A): Either[Throwable, A] = {
    withRetry(maxAttempts, delaySeconds * 1000L)(operation)
  }

  /**
   * Execute operation with retry, throwing exception on failure.
   * Convenience method for contexts where exceptions are preferred over Either.
   *
   * @param maxAttempts Maximum number of attempts
   * @param delayMillis Delay between retries in milliseconds
   * @param operation Operation to execute
   * @tparam A Return type
   * @return Result of operation
   * @throws Throwable if all retries exhausted
   */
  def withRetryOrThrow[A](maxAttempts: Int, delayMillis: Long)(operation: => A): A = {
    withRetry(maxAttempts, delayMillis)(operation) match {
      case Right(result) => result
      case Left(error)   => throw error
    }
  }
}
