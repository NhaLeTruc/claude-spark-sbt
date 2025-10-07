package com.etl.util

import com.etl.config._
import org.apache.spark.sql.SparkSession
import scala.concurrent.duration._

/**
 * Factory for creating error handling components from configuration.
 *
 * Instantiates retry strategies, circuit breakers, and dead letter queues
 * based on pipeline configuration settings.
 */
object ErrorHandlingFactory {

  /**
   * Create a retry strategy from configuration.
   *
   * @param config Retry configuration
   * @return Retry strategy instance
   */
  def createRetryStrategy(config: RetryConfig): RetryStrategy = {
    config.strategy match {
      case RetryStrategyType.ExponentialBackoff =>
        val backoffConfig = ExponentialBackoffConfig(
          maxAttempts = config.maxAttempts,
          initialDelay = config.initialDelaySeconds.seconds,
          maxDelay = config.maxDelaySeconds.seconds,
          backoffMultiplier = config.backoffMultiplier,
          jitter = config.jitter
        )
        new ExponentialBackoffRetry(backoffConfig)

      case RetryStrategyType.FixedDelay =>
        new FixedDelayRetry(
          maxAttempts = config.maxAttempts,
          delay = config.initialDelaySeconds.seconds
        )

      case RetryStrategyType.NoRetry =>
        NoRetryStrategy
    }
  }

  /**
   * Create a circuit breaker from configuration.
   *
   * @param config Circuit breaker configuration
   * @return Circuit breaker instance, or None if disabled
   */
  def createCircuitBreaker(config: CircuitBreakerConfig): Option[CircuitBreaker] = {
    if (config.enabled) {
      Some(new CircuitBreaker(
        failureThreshold = config.failureThreshold,
        resetTimeout = config.resetTimeoutSeconds.seconds,
        halfOpenMaxAttempts = config.halfOpenMaxAttempts
      ))
    } else {
      None
    }
  }

  /**
   * Create a dead letter queue from configuration.
   *
   * @param config DLQ configuration
   * @param spark Implicit SparkSession
   * @return Dead letter queue instance
   */
  def createDeadLetterQueue(config: DLQConfig)(implicit spark: SparkSession): DeadLetterQueue = {
    config.dlqType match {
      case DLQType.Kafka =>
        val bootstrapServers = config.bootstrapServers.getOrElse(
          throw new IllegalArgumentException("bootstrapServers is required for Kafka DLQ")
        )
        val topic = config.topic.getOrElse(
          throw new IllegalArgumentException("topic is required for Kafka DLQ")
        )
        new KafkaDeadLetterQueue(bootstrapServers, topic, config.producerConfig)

      case DLQType.S3 =>
        val bucketPath = config.bucketPath.getOrElse(
          throw new IllegalArgumentException("bucketPath is required for S3 DLQ")
        )
        new S3DeadLetterQueue(
          bucketPath,
          config.partitionBy,
          config.bufferSize,
          config.format
        )

      case DLQType.Logging =>
        new LoggingDeadLetterQueue()

      case DLQType.None =>
        new NoOpDeadLetterQueue()
    }
  }

  /**
   * Create a complete error handling context from pipeline configuration.
   *
   * @param config Error handling configuration
   * @param spark Implicit SparkSession
   * @return Error handling context with all components
   */
  def createErrorHandlingContext(
    config: ErrorHandlingConfig
  )(implicit spark: SparkSession): ErrorHandlingContext = {
    ErrorHandlingContext(
      retryStrategy = createRetryStrategy(config.retryConfig),
      circuitBreaker = createCircuitBreaker(config.circuitBreakerConfig),
      deadLetterQueue = createDeadLetterQueue(config.dlqConfig),
      failFast = config.failFast
    )
  }
}

/**
 * Context holding all error handling components for a pipeline.
 *
 * @param retryStrategy Retry strategy for transient failures
 * @param circuitBreaker Circuit breaker to prevent cascading failures (optional)
 * @param deadLetterQueue Dead letter queue for failed records
 * @param failFast Whether to fail immediately on errors
 */
case class ErrorHandlingContext(
  retryStrategy: RetryStrategy,
  circuitBreaker: Option[CircuitBreaker],
  deadLetterQueue: DeadLetterQueue,
  failFast: Boolean
) {
  /**
   * Execute an operation with full error handling.
   *
   * Applies retry strategy and circuit breaker (if configured).
   *
   * @param operation The operation to execute
   * @tparam T Return type
   * @return Either the result or the error
   */
  def execute[T](operation: => T): Either[Throwable, T] = {
    val wrappedOperation = () => {
      circuitBreaker match {
        case Some(cb) =>
          cb.execute(operation).toEither
        case None =>
          retryStrategy.execute(operation)
      }
    }

    circuitBreaker match {
      case Some(cb) =>
        retryStrategy.execute(wrappedOperation().fold(throw _, identity))
      case None =>
        retryStrategy.execute(operation)
    }
  }

  /**
   * Close all resources.
   */
  def close(): Unit = {
    deadLetterQueue.close()
  }
}
