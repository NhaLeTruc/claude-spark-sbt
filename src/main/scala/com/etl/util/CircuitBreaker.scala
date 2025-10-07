package com.etl.util

import java.time.Instant
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import org.slf4j.LoggerFactory

/**
 * Circuit breaker states.
 *
 * State transitions:
 * - Closed → Open: After failureThreshold consecutive failures
 * - Open → HalfOpen: After resetTimeout has elapsed
 * - HalfOpen → Closed: After halfOpenMaxAttempts successful calls
 * - HalfOpen → Open: On any failure during half-open state
 */
sealed trait CircuitBreakerState {
  def name: String
}

case object Closed extends CircuitBreakerState {
  override def name: String = "CLOSED"
}

case object Open extends CircuitBreakerState {
  override def name: String = "OPEN"
}

case object HalfOpen extends CircuitBreakerState {
  override def name: String = "HALF_OPEN"
}

/**
 * Circuit breaker exception thrown when circuit is open.
 */
class CircuitBreakerOpenException(message: String) extends RuntimeException(message)

/**
 * Circuit breaker for protecting external systems from cascading failures.
 *
 * The circuit breaker wraps operations to external systems and monitors for failures.
 * When failures exceed a threshold, the circuit "opens" and immediately fails subsequent
 * calls without attempting the operation, giving the external system time to recover.
 *
 * States:
 * - **Closed**: Normal operation, all requests pass through
 * - **Open**: Too many failures detected, reject all requests immediately
 * - **HalfOpen**: After timeout, allow limited requests to test if system recovered
 *
 * Example usage:
 * {{{
 * val breaker = CircuitBreaker(
 *   failureThreshold = 5,
 *   resetTimeout = 60.seconds,
 *   halfOpenMaxAttempts = 1
 * )
 *
 * breaker.execute {
 *   // Call to external system
 *   database.query("SELECT * FROM users")
 * }
 * }}}
 *
 * @param failureThreshold Number of consecutive failures before opening circuit
 * @param resetTimeout Time before attempting recovery from open state
 * @param halfOpenMaxAttempts Number of successful calls needed to close from half-open
 */
class CircuitBreaker(
  failureThreshold: Int,
  resetTimeout: FiniteDuration,
  halfOpenMaxAttempts: Int = 1
) {
  require(failureThreshold > 0, "failureThreshold must be positive")
  require(resetTimeout.toMillis > 0, "resetTimeout must be positive")
  require(halfOpenMaxAttempts > 0, "halfOpenMaxAttempts must be positive")

  private val logger = LoggerFactory.getLogger(getClass)

  private val state = new AtomicReference[CircuitBreakerState](Closed)
  private val failureCount = new AtomicInteger(0)
  private val successCount = new AtomicInteger(0)
  private val lastFailureTime = new AtomicLong(0L)
  private val totalFailures = new AtomicLong(0L)
  private val totalSuccesses = new AtomicLong(0L)
  private val totalRejections = new AtomicLong(0L)

  /**
   * Execute operation with circuit breaker protection.
   *
   * @param operation The operation to execute
   * @tparam T Return type of the operation
   * @return Try containing the result or failure
   */
  def execute[T](operation: => T): Try[T] = {
    state.get() match {
      case Closed =>
        executeClosed(operation)

      case Open =>
        if (shouldAttemptReset()) {
          logger.info("Circuit breaker transitioning to HalfOpen state")
          state.set(HalfOpen)
          successCount.set(0)
          executeHalfOpen(operation)
        } else {
          val timeUntilReset = resetTimeout.toMillis -
            (System.currentTimeMillis() - lastFailureTime.get())
          totalRejections.incrementAndGet()

          Failure(new CircuitBreakerOpenException(
            s"Circuit breaker is OPEN. Retry in ${timeUntilReset}ms. " +
              s"Total failures: ${totalFailures.get()}"
          ))
        }

      case HalfOpen =>
        executeHalfOpen(operation)
    }
  }

  /**
   * Execute operation in closed state (normal operation).
   */
  private def executeClosed[T](operation: => T): Try[T] = {
    Try(operation) match {
      case success @ Success(_) =>
        resetFailures()
        totalSuccesses.incrementAndGet()
        success

      case failure @ Failure(error) =>
        val currentFailures = failureCount.incrementAndGet()
        totalFailures.incrementAndGet()
        lastFailureTime.set(System.currentTimeMillis())

        if (currentFailures >= failureThreshold) {
          logger.error(
            s"Circuit breaker opening after $currentFailures consecutive failures. " +
              s"Error: ${error.getClass.getSimpleName}: ${error.getMessage}"
          )
          state.set(Open)
        } else {
          logger.warn(
            s"Circuit breaker failure $currentFailures/$failureThreshold. " +
              s"Error: ${error.getClass.getSimpleName}"
          )
        }

        failure
    }
  }

  /**
   * Execute operation in half-open state (testing recovery).
   */
  private def executeHalfOpen[T](operation: => T): Try[T] = {
    Try(operation) match {
      case success @ Success(_) =>
        val currentSuccess = successCount.incrementAndGet()
        totalSuccesses.incrementAndGet()

        if (currentSuccess >= halfOpenMaxAttempts) {
          logger.info(
            s"Circuit breaker closing after $currentSuccess successful recovery attempts. " +
              s"Total successes: ${totalSuccesses.get()}"
          )
          state.set(Closed)
          resetFailures()
          successCount.set(0)
        } else {
          logger.info(
            s"Circuit breaker recovery progress: $currentSuccess/$halfOpenMaxAttempts successful attempts"
          )
        }

        success

      case failure @ Failure(error) =>
        logger.warn(
          s"Circuit breaker reopening after failed recovery attempt. " +
              s"Error: ${error.getClass.getSimpleName}: ${error.getMessage}"
        )
        state.set(Open)
        lastFailureTime.set(System.currentTimeMillis())
        totalFailures.incrementAndGet()
        successCount.set(0)
        failure
    }
  }

  /**
   * Check if enough time has passed to attempt reset.
   */
  private def shouldAttemptReset(): Boolean = {
    val lastFailure = lastFailureTime.get()
    if (lastFailure == 0) return false

    val elapsed = System.currentTimeMillis() - lastFailure
    elapsed >= resetTimeout.toMillis
  }

  /**
   * Reset failure counter.
   */
  private def resetFailures(): Unit = {
    failureCount.set(0)
  }

  /**
   * Get current circuit breaker state.
   */
  def getState: CircuitBreakerState = state.get()

  /**
   * Get current failure count.
   */
  def getFailureCount: Int = failureCount.get()

  /**
   * Get metrics for monitoring.
   */
  def getMetrics: CircuitBreakerMetrics = {
    CircuitBreakerMetrics(
      state = state.get(),
      failureCount = failureCount.get(),
      totalFailures = totalFailures.get(),
      totalSuccesses = totalSuccesses.get(),
      totalRejections = totalRejections.get(),
      lastFailureTime = if (lastFailureTime.get() == 0) None else Some(lastFailureTime.get())
    )
  }

  /**
   * Manually reset circuit breaker (for testing/admin purposes).
   */
  def reset(): Unit = {
    logger.info("Circuit breaker manually reset")
    state.set(Closed)
    resetFailures()
    successCount.set(0)
  }
}

/**
 * Circuit breaker metrics for monitoring.
 */
case class CircuitBreakerMetrics(
  state: CircuitBreakerState,
  failureCount: Int,
  totalFailures: Long,
  totalSuccesses: Long,
  totalRejections: Long,
  lastFailureTime: Option[Long]
) {
  def successRate: Double = {
    val total = totalSuccesses + totalFailures
    if (total == 0) 0.0 else (totalSuccesses.toDouble / total.toDouble) * 100.0
  }

  def isHealthy: Boolean = state == Closed && failureCount == 0
}

object CircuitBreaker {
  /**
   * Create a new circuit breaker.
   *
   * @param failureThreshold Number of failures before opening
   * @param resetTimeout Time before attempting recovery
   * @param halfOpenMaxAttempts Successful calls needed to close
   * @return CircuitBreaker instance
   */
  def apply(
    failureThreshold: Int,
    resetTimeout: FiniteDuration,
    halfOpenMaxAttempts: Int = 1
  ): CircuitBreaker = {
    new CircuitBreaker(failureThreshold, resetTimeout, halfOpenMaxAttempts)
  }

  /**
   * Create a circuit breaker with simplified parameters.
   *
   * @param failureThreshold Number of failures before opening
   * @param resetTimeoutSeconds Timeout in seconds
   * @return CircuitBreaker instance
   */
  def apply(
    failureThreshold: Int,
    resetTimeoutSeconds: Int
  ): CircuitBreaker = {
    new CircuitBreaker(failureThreshold, resetTimeoutSeconds.seconds, 1)
  }
}
