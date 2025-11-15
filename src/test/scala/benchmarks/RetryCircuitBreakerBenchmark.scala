package benchmarks

import com.etl.config.{CircuitBreakerConfig, RetryConfig, RetryStrategyType}
import com.etl.util.{AsyncRetry, CircuitBreaker}
import org.openjdk.jmh.annotations._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Random

/**
 * JMH benchmarks for retry mechanisms and circuit breaker.
 *
 * Run with: sbt "Jmh/run -i 10 -wi 5 -f 1 -t 1 benchmarks.RetryCircuitBreakerBenchmark"
 *
 * These benchmarks measure:
 * - Retry mechanism overhead
 * - Circuit breaker overhead
 * - Async vs blocking retry performance
 * - Different retry strategies
 */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class RetryCircuitBreakerBenchmark {

  implicit val ec: ExecutionContext = ExecutionContext.global

  /**
   * Successful operation (no retries needed).
   */
  def successfulOperation(): Int = {
    Thread.sleep(1) // Simulate minimal work
    42
  }

  /**
   * Successful async operation.
   */
  def successfulAsyncOperation(): Future[Int] = {
    Future {
      Thread.sleep(1) // Simulate minimal work
      42
    }
  }

  /**
   * Operation that fails N times before succeeding.
   */
  class PartialFailureOperation(failuresBeforeSuccess: Int) {
    private var attemptCount = 0

    def execute(): Int = {
      attemptCount += 1
      if (attemptCount <= failuresBeforeSuccess) {
        throw new RuntimeException(s"Attempt $attemptCount failed")
      }
      42
    }

    def executeAsync(): Future[Int] = {
      Future {
        execute()
      }
    }

    def reset(): Unit = {
      attemptCount = 0
    }
  }

  /**
   * Benchmark: Successful operation with no retry overhead.
   */
  @Benchmark
  def benchmarkNoRetry(): Int = {
    successfulOperation()
  }

  /**
   * Benchmark: Successful operation with async retry (no failures).
   */
  @Benchmark
  def benchmarkAsyncRetry_NoFailure(): Int = {
    val config = RetryConfig(
      strategy = RetryStrategyType.ExponentialBackoff,
      maxAttempts = 3,
      initialDelaySeconds = 0
    )

    val result = AsyncRetry.withRetry(config) {
      successfulAsyncOperation()
    }

    Await.result(result, 5.seconds)
  }

  /**
   * Benchmark: Operation that succeeds on 2nd attempt.
   */
  @Benchmark
  def benchmarkAsyncRetry_1Failure(): Int = {
    val config = RetryConfig(
      strategy = RetryStrategyType.FixedDelay,
      maxAttempts = 3,
      initialDelaySeconds = 0
    )

    val operation = new PartialFailureOperation(1)

    val result = AsyncRetry.withRetry(config) {
      operation.executeAsync()
    }

    Await.result(result, 5.seconds)
  }

  /**
   * Benchmark: Operation that succeeds on 3rd attempt.
   */
  @Benchmark
  def benchmarkAsyncRetry_2Failures(): Int = {
    val config = RetryConfig(
      strategy = RetryStrategyType.FixedDelay,
      maxAttempts = 3,
      initialDelaySeconds = 0
    )

    val operation = new PartialFailureOperation(2)

    val result = AsyncRetry.withRetry(config) {
      operation.executeAsync()
    }

    Await.result(result, 5.seconds)
  }

  /**
   * Benchmark: Fixed delay retry strategy.
   */
  @Benchmark
  def benchmarkRetryStrategy_FixedDelay(): Int = {
    val config = RetryConfig(
      strategy = RetryStrategyType.FixedDelay,
      maxAttempts = 3,
      initialDelaySeconds = 0
    )

    val operation = new PartialFailureOperation(1)

    val result = AsyncRetry.withRetry(config) {
      operation.executeAsync()
    }

    Await.result(result, 5.seconds)
  }

  /**
   * Benchmark: Exponential backoff retry strategy.
   */
  @Benchmark
  def benchmarkRetryStrategy_ExponentialBackoff(): Int = {
    val config = RetryConfig(
      strategy = RetryStrategyType.ExponentialBackoff,
      maxAttempts = 3,
      initialDelaySeconds = 0
    )

    val operation = new PartialFailureOperation(1)

    val result = AsyncRetry.withRetry(config) {
      operation.executeAsync()
    }

    Await.result(result, 5.seconds)
  }

  /**
   * Benchmark: Circuit breaker overhead when closed (healthy).
   */
  @Benchmark
  def benchmarkCircuitBreaker_Closed(): Option[Int] = {
    val cb = new CircuitBreaker(
      name = "benchmark-cb",
      failureThreshold = 5,
      resetTimeoutMillis = 60000,
      halfOpenMaxAttempts = 1
    )

    cb.execute {
      successfulOperation()
    }
  }

  /**
   * Benchmark: Circuit breaker overhead when open (failing fast).
   */
  @Benchmark
  def benchmarkCircuitBreaker_Open(): Option[Int] = {
    val cb = new CircuitBreaker(
      name = "benchmark-cb-open",
      failureThreshold = 2,
      resetTimeoutMillis = 60000,
      halfOpenMaxAttempts = 1
    )

    // Trip the circuit breaker
    (1 to 5).foreach { _ =>
      cb.execute {
        throw new RuntimeException("Fail")
      }
    }

    // Now measure fail-fast behavior
    cb.execute {
      successfulOperation()
    }
  }

  /**
   * Benchmark: Circuit breaker state transitions.
   */
  @Benchmark
  @OperationsPerInvocation(10)
  def benchmarkCircuitBreaker_StateTransitions(): Unit = {
    val cb = new CircuitBreaker(
      name = "benchmark-cb-transitions",
      failureThreshold = 3,
      resetTimeoutMillis = 100,
      halfOpenMaxAttempts = 1
    )

    // Execute successful operations (CLOSED state)
    (1 to 3).foreach { _ =>
      cb.execute(successfulOperation())
    }

    // Cause failures to trip circuit breaker (CLOSED -> OPEN)
    (1 to 5).foreach { _ =>
      cb.execute {
        throw new RuntimeException("Fail")
      }
    }

    // Wait for timeout (OPEN -> HALF_OPEN)
    Thread.sleep(150)

    // Execute operation to test half-open (HALF_OPEN -> CLOSED or OPEN)
    cb.execute(successfulOperation())

    // Execute successful operations again
    cb.execute(successfulOperation())
  }

  /**
   * Benchmark: Multiple circuit breakers (concurrent usage).
   */
  @Benchmark
  @Threads(4)
  def benchmarkCircuitBreaker_Concurrent(): Option[Int] = {
    val cb = new CircuitBreaker(
      name = "benchmark-cb-concurrent",
      failureThreshold = 10,
      resetTimeoutMillis = 60000,
      halfOpenMaxAttempts = 1
    )

    cb.execute {
      successfulOperation()
    }
  }

  /**
   * Benchmark: Retry with circuit breaker combined.
   */
  @Benchmark
  def benchmarkRetryWithCircuitBreaker(): Option[Int] = {
    val cb = new CircuitBreaker(
      name = "benchmark-cb-retry",
      failureThreshold = 5,
      resetTimeoutMillis = 60000,
      halfOpenMaxAttempts = 1
    )

    val config = RetryConfig(
      strategy = RetryStrategyType.FixedDelay,
      maxAttempts = 3,
      initialDelaySeconds = 0
    )

    val operation = new PartialFailureOperation(1)

    cb.execute {
      val result = AsyncRetry.withRetry(config) {
        operation.executeAsync()
      }
      Await.result(result, 5.seconds)
    }
  }

  /**
   * Benchmark: Calculate delay overhead (exponential backoff).
   */
  @Benchmark
  def benchmarkDelayCalculation_ExponentialBackoff(): Long = {
    val config = RetryConfig(
      strategy = RetryStrategyType.ExponentialBackoff,
      maxAttempts = 10,
      initialDelaySeconds = 1,
      maxDelaySeconds = Some(60),
      jitterFactor = Some(0.1)
    )

    // Benchmark the delay calculation logic
    val delays = (1 to 10).map { attempt =>
      AsyncRetry.calculateDelay(attempt, config)
    }

    delays.sum
  }

  /**
   * Benchmark: Calculate delay overhead (fixed delay).
   */
  @Benchmark
  def benchmarkDelayCalculation_FixedDelay(): Long = {
    val config = RetryConfig(
      strategy = RetryStrategyType.FixedDelay,
      maxAttempts = 10,
      initialDelaySeconds = 1
    )

    val delays = (1 to 10).map { attempt =>
      AsyncRetry.calculateDelay(attempt, config)
    }

    delays.sum
  }

  /**
   * Benchmark: Failure rate calculation in circuit breaker.
   */
  @Benchmark
  @OperationsPerInvocation(100)
  def benchmarkCircuitBreaker_FailureRateCalculation(): Double = {
    val cb = new CircuitBreaker(
      name = "benchmark-cb-failure-rate",
      failureThreshold = 50,
      resetTimeoutMillis = 60000,
      halfOpenMaxAttempts = 1
    )

    val random = new Random(42)

    // Execute mixed success/failure operations
    (1 to 100).foreach { _ =>
      cb.execute {
        if (random.nextDouble() < 0.3) {
          throw new RuntimeException("Random failure")
        }
        successfulOperation()
      }
    }

    cb.getFailureRate
  }
}
