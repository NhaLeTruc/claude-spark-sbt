# Important Features Implementation Plan

**Date**: 2025-10-07
**Priority**: 🟡 Important (Production Enhancement)
**Total Effort**: 10-14 days (80-112 hours)

This document provides detailed implementation plans for the 4 important priority features identified in the improvement roadmap.

---

## Overview

| Feature | Priority | Effort | Impact | Dependencies |
|---------|----------|--------|--------|--------------|
| 1. Advanced Error Handling | MEDIUM | 2-3 days | HIGH | None |
| 2. Data Quality Validation | MEDIUM | 3-4 days | HIGH | SchemaValidator |
| 3. Monitoring & Observability | MEDIUM | 2-3 days | HIGH | None |
| 4. Streaming Enhancements | MEDIUM | 3-4 days | MEDIUM | Fix streaming query mgmt |

**Total**: 10-14 days for all features

---

# Feature 1: Advanced Error Handling & Recovery

**Priority**: MEDIUM
**Effort**: 16-24 hours (2-3 days)
**Impact**: HIGH - Production resilience

## Current State

**Limitations**:
- ✅ Basic retry with fixed delay (exists in [Retry.scala](src/main/scala/com/etl/util/Retry.scala:1))
- ❌ No exponential backoff with jitter
- ❌ No circuit breaker pattern
- ❌ No dead letter queue for failed records
- ❌ Limited error context in metrics

**Current Retry Implementation**:
```scala
// src/main/scala/com/etl/util/Retry.scala
def withRetry[T](maxAttempts: Int, delayMillis: Long)(operation: => T): Either[Throwable, T] = {
  @tailrec
  def attempt(remainingAttempts: Int): Either[Throwable, T] = {
    Try(operation) match {
      case Success(result) => Right(result)
      case Failure(error) if remainingAttempts > 0 =>
        Thread.sleep(delayMillis) // Fixed delay - no backoff
        attempt(remainingAttempts - 1)
      case Failure(error) => Left(error)
    }
  }
  attempt(maxAttempts)
}
```

## Proposed Solution

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Error Handling Layer                      │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   Retry      │    │   Circuit    │    │  Dead Letter │  │
│  │  Strategy    │───▶│   Breaker    │───▶│    Queue     │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│         │                    │                    │          │
│         ▼                    ▼                    ▼          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  Exponential │    │    State     │    │   Publish    │  │
│  │   Backoff    │    │  Management  │    │   Failed     │  │
│  │  + Jitter    │    │              │    │   Records    │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Implementation Tasks

#### Task 1.1: Enhanced Retry with Exponential Backoff (6 hours)

**Files to Create**:
- `src/main/scala/com/etl/util/RetryStrategy.scala`
- `src/test/scala/unit/util/RetryStrategySpec.scala`

**Implementation**:

```scala
// src/main/scala/com/etl/util/RetryStrategy.scala
package com.etl.util

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.util.Random
import org.slf4j.LoggerFactory

/**
 * Advanced retry strategy with exponential backoff and jitter.
 */
sealed trait RetryStrategy {
  def execute[T](operation: => T): Either[Throwable, T]
}

/**
 * Retry configuration with exponential backoff.
 *
 * @param maxAttempts Maximum number of retry attempts
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
)

class ExponentialBackoffRetry(config: ExponentialBackoffConfig) extends RetryStrategy {
  private val logger = LoggerFactory.getLogger(getClass)
  private val random = new Random()

  override def execute[T](operation: => T): Either[Throwable, T] = {
    @scala.annotation.tailrec
    def attempt(attemptNumber: Int, currentDelay: FiniteDuration): Either[Throwable, T] = {
      Try(operation) match {
        case Success(result) =>
          if (attemptNumber > 1) {
            logger.info(s"Operation succeeded after ${attemptNumber} attempts")
          }
          Right(result)

        case Failure(error) if attemptNumber < config.maxAttempts =>
          val delayWithJitter = if (config.jitter) {
            val jitterFactor = 0.5 + (random.nextDouble() * 0.5) // 50-100% of delay
            (currentDelay.toMillis * jitterFactor).toLong.millis
          } else {
            currentDelay
          }

          logger.warn(
            s"Operation failed (attempt $attemptNumber/${config.maxAttempts}). " +
              s"Retrying in ${delayWithJitter.toMillis}ms. Error: ${error.getMessage}"
          )

          Thread.sleep(delayWithJitter.toMillis)

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
              s"Final error: ${error.getMessage}",
            error
          )
          Left(error)
      }
    }

    attempt(1, config.initialDelay)
  }
}

object RetryStrategy {
  def exponentialBackoff(config: ExponentialBackoffConfig): RetryStrategy = {
    new ExponentialBackoffRetry(config)
  }

  def noRetry: RetryStrategy = new RetryStrategy {
    override def execute[T](operation: => T): Either[Throwable, T] = {
      Try(operation).toEither
    }
  }
}
```

**Configuration Integration**:
```scala
// Update PipelineConfig.scala
case class RetryConfig(
  maxAttempts: Int,
  delaySeconds: Int, // Deprecated - kept for backward compatibility
  backoffMultiplier: Double,

  // New fields
  initialDelayMs: Option[Long] = None,
  maxDelayMs: Option[Long] = None,
  useExponentialBackoff: Boolean = false,
  jitter: Boolean = true
)
```

---

#### Task 1.2: Circuit Breaker Pattern (8 hours)

**Files to Create**:
- `src/main/scala/com/etl/util/CircuitBreaker.scala`
- `src/test/scala/unit/util/CircuitBreakerSpec.scala`

**Implementation**:

```scala
// src/main/scala/com/etl/util/CircuitBreaker.scala
package com.etl.util

import java.time.Instant
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import org.slf4j.LoggerFactory

/**
 * Circuit breaker states.
 */
sealed trait CircuitBreakerState
case object Closed extends CircuitBreakerState      // Normal operation
case object Open extends CircuitBreakerState        // Failing, reject requests
case object HalfOpen extends CircuitBreakerState    // Testing if recovered

/**
 * Circuit breaker for protecting external systems.
 *
 * States:
 * - Closed: Normal operation, requests pass through
 * - Open: Too many failures, reject all requests immediately
 * - HalfOpen: After timeout, allow one request to test recovery
 *
 * @param failureThreshold Number of failures before opening
 * @param resetTimeout Time before attempting recovery
 * @param halfOpenMaxAttempts Number of successful calls needed to close
 */
class CircuitBreaker(
  failureThreshold: Int,
  resetTimeout: FiniteDuration,
  halfOpenMaxAttempts: Int = 1
) {
  private val logger = LoggerFactory.getLogger(getClass)

  private val state = new AtomicReference[CircuitBreakerState](Closed)
  private val failureCount = new AtomicInteger(0)
  private val successCount = new AtomicInteger(0)
  private val lastFailureTime = new AtomicReference[Option[Instant]](None)

  /**
   * Execute operation with circuit breaker protection.
   */
  def execute[T](operation: => T): Try[T] = {
    state.get() match {
      case Closed =>
        executeClosed(operation)

      case Open =>
        checkIfShouldAttemptReset() match {
          case true =>
            logger.info("Circuit breaker transitioning to HalfOpen")
            state.set(HalfOpen)
            executeHalfOpen(operation)

          case false =>
            val timeUntilReset = resetTimeout.toMillis -
              (System.currentTimeMillis() - lastFailureTime.get().get.toEpochMilli)
            Failure(new CircuitBreakerOpenException(
              s"Circuit breaker is OPEN. Retry in ${timeUntilReset}ms"
            ))
        }

      case HalfOpen =>
        executeHalfOpen(operation)
    }
  }

  private def executeClosed[T](operation: => T): Try[T] = {
    Try(operation) match {
      case success @ Success(_) =>
        resetFailures()
        success

      case failure @ Failure(error) =>
        recordFailure()

        if (failureCount.get() >= failureThreshold) {
          logger.error(
            s"Circuit breaker opening after $failureThreshold failures. " +
              s"Last error: ${error.getMessage}"
          )
          state.set(Open)
          lastFailureTime.set(Some(Instant.now()))
        }

        failure
    }
  }

  private def executeHalfOpen[T](operation: => T): Try[T] = {
    Try(operation) match {
      case success @ Success(_) =>
        val currentSuccess = successCount.incrementAndGet()

        if (currentSuccess >= halfOpenMaxAttempts) {
          logger.info(
            s"Circuit breaker closing after $currentSuccess successful attempts"
          )
          state.set(Closed)
          resetFailures()
          successCount.set(0)
        }

        success

      case failure @ Failure(error) =>
        logger.warn(
          s"Circuit breaker reopening after failed recovery attempt: ${error.getMessage}"
        )
        state.set(Open)
        lastFailureTime.set(Some(Instant.now()))
        successCount.set(0)
        failure
    }
  }

  private def recordFailure(): Unit = {
    failureCount.incrementAndGet()
  }

  private def resetFailures(): Unit = {
    failureCount.set(0)
  }

  private def checkIfShouldAttemptReset(): Boolean = {
    lastFailureTime.get() match {
      case Some(lastFailure) =>
        val elapsed = System.currentTimeMillis() - lastFailure.toEpochMilli
        elapsed >= resetTimeout.toMillis

      case None => false
    }
  }

  def getState: CircuitBreakerState = state.get()
  def getFailureCount: Int = failureCount.get()
}

class CircuitBreakerOpenException(message: String) extends RuntimeException(message)

object CircuitBreaker {
  def apply(
    failureThreshold: Int,
    resetTimeout: FiniteDuration,
    halfOpenMaxAttempts: Int = 1
  ): CircuitBreaker = {
    new CircuitBreaker(failureThreshold, resetTimeout, halfOpenMaxAttempts)
  }
}
```

**Integration with Extractors/Loaders**:
```scala
// Add circuit breaker to JDBC loaders
class PostgreSQLLoader(
  credentialVault: Option[CredentialVault] = None,
  circuitBreaker: Option[CircuitBreaker] = None
) extends Loader {

  override def load(...): LoadResult = {
    val operation = () => performLoad(...)

    circuitBreaker match {
      case Some(cb) =>
        cb.execute(operation()) match {
          case Success(result) => result
          case Failure(ex: CircuitBreakerOpenException) =>
            LoadResult.failure(0L, df.count(), ex.getMessage)
          case Failure(ex) =>
            LoadResult.failure(0L, df.count(), ex.getMessage)
        }

      case None => operation()
    }
  }
}
```

---

#### Task 1.3: Dead Letter Queue (DLQ) (8 hours)

**Files to Create**:
- `src/main/scala/com/etl/util/DeadLetterQueue.scala`
- `src/main/scala/com/etl/util/KafkaDeadLetterQueue.scala`
- `src/main/scala/com/etl/util/S3DeadLetterQueue.scala`
- `src/test/scala/unit/util/DeadLetterQueueSpec.scala`

**Implementation**:

```scala
// src/main/scala/com/etl/util/DeadLetterQueue.scala
package com.etl.util

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import play.api.libs.json.Json
import java.time.Instant

/**
 * Dead letter queue for failed records.
 */
trait DeadLetterQueue {
  /**
   * Publish a failed record to DLQ.
   *
   * @param record The failed record
   * @param error The error that caused failure
   * @param context Additional context (pipeline ID, stage, etc.)
   */
  def publish(record: Row, error: Throwable, context: Map[String, String]): Unit

  /**
   * Publish multiple failed records in batch.
   */
  def publishBatch(records: Seq[(Row, Throwable)], context: Map[String, String]): Unit = {
    records.foreach { case (record, error) =>
      publish(record, error, context)
    }
  }
}

/**
 * Failed record envelope with metadata.
 */
case class FailedRecord(
  timestamp: Long,
  pipelineId: String,
  stage: String,
  error: String,
  stackTrace: String,
  originalRecord: String,
  attemptNumber: Int,
  context: Map[String, String]
)

object FailedRecord {
  def apply(
    record: Row,
    error: Throwable,
    context: Map[String, String]
  ): FailedRecord = {
    FailedRecord(
      timestamp = Instant.now().toEpochMilli,
      pipelineId = context.getOrElse("pipelineId", "unknown"),
      stage = context.getOrElse("stage", "unknown"),
      error = error.getMessage,
      stackTrace = error.getStackTrace.mkString("\n"),
      originalRecord = record.mkString("|"),
      attemptNumber = context.getOrElse("attemptNumber", "1").toInt,
      context = context
    )
  }
}
```

```scala
// src/main/scala/com/etl/util/KafkaDeadLetterQueue.scala
package com.etl.util

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import play.api.libs.json.Json
import java.util.Properties
import org.slf4j.LoggerFactory

/**
 * Kafka-based dead letter queue.
 */
class KafkaDeadLetterQueue(
  bootstrapServers: String,
  topic: String
)(implicit spark: SparkSession) extends DeadLetterQueue {

  private val logger = LoggerFactory.getLogger(getClass)

  private lazy val producer: KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", bootstrapServers)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    props.put("retries", "3")

    new KafkaProducer[String, String](props)
  }

  override def publish(record: Row, error: Throwable, context: Map[String, String]): Unit = {
    try {
      val failedRecord = FailedRecord(record, error, context)
      val key = context.getOrElse("pipelineId", "unknown")
      val value = Json.toJson(failedRecord).toString()

      val producerRecord = new ProducerRecord[String, String](topic, key, value)
      val metadata = producer.send(producerRecord).get()

      logger.debug(
        s"Published failed record to DLQ: topic=$topic, " +
          s"partition=${metadata.partition()}, offset=${metadata.offset()}"
      )

    } catch {
      case ex: Exception =>
        logger.error(s"Failed to publish to DLQ: ${ex.getMessage}", ex)
        // Don't throw - DLQ failures shouldn't break the pipeline
    }
  }

  def close(): Unit = {
    producer.close()
  }
}
```

```scala
// src/main/scala/com/etl/util/S3DeadLetterQueue.scala
package com.etl.util

import org.apache.spark.sql.{Row, SparkSession, SaveMode}
import play.api.libs.json.Json
import org.slf4j.LoggerFactory
import java.time.{LocalDate, Instant}

/**
 * S3-based dead letter queue with daily partitioning.
 */
class S3DeadLetterQueue(
  bucketPath: String,
  partitionBy: String = "date" // date, hour, pipeline
)(implicit spark: SparkSession) extends DeadLetterQueue {

  private val logger = LoggerFactory.getLogger(getClass)

  // Buffer for batch writes
  private val buffer = scala.collection.mutable.ArrayBuffer[(Row, Throwable, Map[String, String])]()
  private val bufferSize = 100

  override def publish(record: Row, error: Throwable, context: Map[String, String]): Unit = {
    synchronized {
      buffer.append((record, error, context))

      if (buffer.size >= bufferSize) {
        flush()
      }
    }
  }

  private def flush(): Unit = {
    if (buffer.isEmpty) return

    try {
      val failedRecords = buffer.map { case (record, error, context) =>
        FailedRecord(record, error, context)
      }

      import spark.implicits._
      val df = failedRecords.toSeq.toDF()

      val partitionPath = partitionBy match {
        case "date" => LocalDate.now().toString
        case "hour" => LocalDate.now().toString + "/" + Instant.now().getEpochSecond / 3600
        case "pipeline" => "${pipelineId}" // Will be replaced per partition
        case _ => "default"
      }

      val outputPath = s"$bucketPath/$partitionPath"

      df.write
        .mode(SaveMode.Append)
        .parquet(outputPath)

      logger.info(s"Flushed ${buffer.size} failed records to S3: $outputPath")
      buffer.clear()

    } catch {
      case ex: Exception =>
        logger.error(s"Failed to flush DLQ to S3: ${ex.getMessage}", ex)
        // Keep records in buffer for next attempt
    }
  }

  def close(): Unit = {
    flush()
  }
}
```

**Integration with Loaders**:
```scala
// Update LoadResult to include DLQ support
case class LoadResult(
  recordsLoaded: Long,
  recordsFailed: Long,
  errors: Seq[String],
  streamingQuery: Option[StreamingQuery] = None,
  dlqRecords: Long = 0L // New field
)

// In loader implementation
class PostgreSQLLoader(
  credentialVault: Option[CredentialVault] = None,
  circuitBreaker: Option[CircuitBreaker] = None,
  deadLetterQueue: Option[DeadLetterQueue] = None
) extends Loader {

  override def load(...): LoadResult = {
    try {
      // Attempt load
      performLoad(...)
    } catch {
      case ex: Exception =>
        // Send to DLQ if configured
        deadLetterQueue.foreach { dlq =>
          df.collect().foreach { row =>
            dlq.publish(row, ex, Map(
              "pipelineId" -> "...",
              "stage" -> "load",
              "sinkType" -> "PostgreSQL"
            ))
          }
        }
        LoadResult.failure(0L, df.count(), ex.getMessage)
    }
  }
}
```

---

### Configuration Schema

```json
{
  "pipelineId": "example-pipeline",
  "errorHandling": {
    "retry": {
      "strategy": "exponential-backoff",
      "maxAttempts": 5,
      "initialDelayMs": 1000,
      "maxDelayMs": 60000,
      "backoffMultiplier": 2.0,
      "jitter": true
    },
    "circuitBreaker": {
      "enabled": true,
      "failureThreshold": 5,
      "resetTimeoutSeconds": 60,
      "halfOpenMaxAttempts": 1
    },
    "deadLetterQueue": {
      "enabled": true,
      "type": "kafka",
      "config": {
        "bootstrap.servers": "localhost:9092",
        "topic": "dlq-failed-records"
      }
    }
  }
}
```

### Testing Strategy

1. **Unit Tests**:
   - RetryStrategy with various backoff configurations
   - CircuitBreaker state transitions
   - DLQ message format and publishing

2. **Integration Tests**:
   - Retry with real failures
   - Circuit breaker with real external system
   - DLQ with real Kafka/S3

3. **Performance Tests**:
   - Retry overhead
   - Circuit breaker latency
   - DLQ throughput

### Deliverables

- [ ] Enhanced Retry utility with exponential backoff
- [ ] Circuit Breaker implementation
- [ ] Dead Letter Queue (Kafka and S3)
- [ ] Configuration schema updates
- [ ] Unit tests (15+ test cases)
- [ ] Integration tests (5+ scenarios)
- [ ] Documentation updates

### Success Criteria

- ✅ Retry delays increase exponentially up to max delay
- ✅ Circuit breaker opens after threshold failures
- ✅ Circuit breaker recovers automatically after timeout
- ✅ Failed records published to DLQ
- ✅ DLQ doesn't block main pipeline
- ✅ <5% performance overhead

---

# Feature 2: Data Quality Validation

**Priority**: MEDIUM
**Effort**: 24-32 hours (3-4 days)
**Impact**: HIGH - Data confidence

## Current State

**Gaps**:
- ✅ Schema validation (structure only)
- ❌ No business logic validation
- ❌ No duplicate detection
- ❌ No range/format validation
- ❌ No data profiling

## Proposed Solution

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                 Data Quality Framework                       │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │   Quality    │───▶│   Execute    │───▶│   Report     │  │
│  │    Rules     │    │    Rules     │    │   Results    │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
│         │                    │                    │          │
│         ▼                    ▼                    ▼          │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │  Built-in    │    │   Sample     │    │   Metrics    │  │
│  │   Rules      │    │   Failures   │    │  + Alerts    │  │
│  │  + Custom    │    │              │    │              │  │
│  └──────────────┘    └──────────────┘    └──────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Implementation (Detailed in next section)

Due to length constraints, see [DATA_QUALITY_IMPLEMENTATION_PLAN.md] for full implementation.

**Summary**:
- Task 2.1: Core framework (8h)
- Task 2.2: Built-in rules (10h)
- Task 2.3: Custom SQL rules (6h)
- Task 2.4: Integration (8h)

---

# Feature 3: Monitoring & Observability

**Priority**: MEDIUM
**Effort**: 16-24 hours (2-3 days)
**Impact**: HIGH - Production operations

## Implementation Summary

**Tasks**:
- Task 3.1: Metrics Reporter Interface (4h)
- Task 3.2: Prometheus Integration (6h)
- Task 3.3: CloudWatch Integration (6h)
- Task 3.4: Distributed Tracing (8h)

See [MONITORING_IMPLEMENTATION_PLAN.md] for full details.

---

# Feature 4: Streaming Enhancements

**Priority**: MEDIUM
**Effort**: 24-32 hours (3-4 days)
**Impact**: MEDIUM - Streaming reliability

## Implementation Summary

**Tasks**:
- Task 4.1: Watermark Configuration (8h)
- Task 4.2: Stateful Aggregations (10h)
- Task 4.3: Exactly-Once Semantics (8h)
- Task 4.4: Late Data Handling (8h)

See [STREAMING_IMPLEMENTATION_PLAN.md] for full details.

---

# Implementation Timeline

## Sprint 1 (Week 1-2): Foundation
- Week 1: Advanced Error Handling (Feature 1)
- Week 2: Data Quality Validation (Feature 2)

## Sprint 2 (Week 3-4): Observability
- Week 3: Monitoring & Observability (Feature 3)
- Week 4: Streaming Enhancements (Feature 4)

## Total Timeline: 4 weeks (10-14 days effort)

---

# Dependencies & Prerequisites

**Must Complete First**:
1. ✅ Fix streaming query management (Critical #5)
2. ✅ Integrate SchemaValidator (Critical #2)
3. ✅ Fix upsert implementation (Critical #1)

**Recommended**:
1. Integration test infrastructure
2. Performance test baseline
3. Production monitoring setup

---

# Success Metrics

| Feature | Metric | Target |
|---------|--------|--------|
| Error Handling | Retry success rate | >80% |
| Error Handling | Circuit breaker trips | <5/day |
| Error Handling | DLQ latency | <100ms |
| Data Quality | Rules evaluated | >10 per pipeline |
| Data Quality | False positive rate | <5% |
| Monitoring | Metrics export latency | <10s |
| Monitoring | Alert delivery time | <60s |
| Streaming | Late data handled | >95% |
| Streaming | Watermark drift | <5 min |

---

# Next Steps

1. **Review this plan** with team
2. **Prioritize features** based on business needs
3. **Create detailed implementation docs** for each feature
4. **Set up development branches**
5. **Begin Sprint 1** with Feature 1 (Error Handling)

---

**Plan Created**: 2025-10-07
**Next Review**: After Sprint 1 completion
