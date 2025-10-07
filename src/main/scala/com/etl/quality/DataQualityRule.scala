package com.etl.quality

import org.apache.spark.sql.{DataFrame, Row}
import org.slf4j.LoggerFactory

/**
 * Severity level for data quality violations.
 */
sealed trait Severity
object Severity {
  case object Error extends Severity   // Blocks pipeline execution
  case object Warning extends Severity // Logs but continues
  case object Info extends Severity    // Informational only

  def fromString(s: String): Severity = s.toLowerCase match {
    case "error"   => Error
    case "warning" => Warning
    case "info"    => Info
    case _         => throw new IllegalArgumentException(s"Unknown severity: $s")
  }
}

/**
 * Result of a data quality rule validation.
 *
 * @param ruleName Name of the rule that was executed
 * @param passed Whether the rule passed (all records valid)
 * @param totalRecords Total number of records evaluated
 * @param failedRecords Number of records that violated the rule
 * @param failureRate Percentage of records that failed (0.0 to 1.0)
 * @param sampleFailures Sample of failed records for debugging (max 10)
 * @param errorMessage Optional error message if rule execution failed
 * @param severity Severity level of this rule
 * @param executionTimeMs Time taken to execute the rule in milliseconds
 */
case class ValidationResult(
  ruleName: String,
  passed: Boolean,
  totalRecords: Long,
  failedRecords: Long,
  failureRate: Double,
  sampleFailures: Seq[Row],
  errorMessage: Option[String] = None,
  severity: Severity = Severity.Error,
  executionTimeMs: Long = 0L
) {
  /**
   * Check if this result should block pipeline execution.
   */
  def shouldBlockPipeline: Boolean = !passed && severity == Severity.Error

  /**
   * Get a summary message for logging.
   */
  def summaryMessage: String = {
    if (passed) {
      s"✓ $ruleName: PASSED ($totalRecords records)"
    } else {
      val failurePercent = (failureRate * 100).formatted("%.2f")
      s"✗ $ruleName: FAILED ($failedRecords/$totalRecords records, $failurePercent%) [${severity}]"
    }
  }
}

/**
 * Abstract base class for data quality rules.
 *
 * Data quality rules validate business logic constraints on DataFrames.
 * Each rule should be focused on a single validation concern.
 */
trait DataQualityRule {
  protected val logger = LoggerFactory.getLogger(getClass)

  /**
   * Name of this rule (should be unique within a pipeline).
   */
  def name: String

  /**
   * Severity level of violations.
   */
  def severity: Severity

  /**
   * Validate the DataFrame and return results.
   *
   * @param df DataFrame to validate
   * @return ValidationResult with pass/fail status and metrics
   */
  def validate(df: DataFrame): ValidationResult

  /**
   * Helper method to collect sample failures (max 10 rows).
   */
  protected def collectSampleFailures(failedDf: DataFrame, maxSamples: Int = 10): Seq[Row] = {
    try {
      failedDf.limit(maxSamples).collect().toSeq
    } catch {
      case ex: Exception =>
        logger.warn(s"Failed to collect sample failures: ${ex.getMessage}")
        Seq.empty
    }
  }

  /**
   * Helper method to create a successful validation result.
   */
  protected def success(totalRecords: Long, executionTimeMs: Long): ValidationResult = {
    ValidationResult(
      ruleName = name,
      passed = true,
      totalRecords = totalRecords,
      failedRecords = 0L,
      failureRate = 0.0,
      sampleFailures = Seq.empty,
      severity = severity,
      executionTimeMs = executionTimeMs
    )
  }

  /**
   * Helper method to create a failed validation result.
   */
  protected def failure(
    totalRecords: Long,
    failedRecords: Long,
    sampleFailures: Seq[Row],
    executionTimeMs: Long,
    errorMessage: Option[String] = None
  ): ValidationResult = {
    ValidationResult(
      ruleName = name,
      passed = false,
      totalRecords = totalRecords,
      failedRecords = failedRecords,
      failureRate = if (totalRecords > 0) failedRecords.toDouble / totalRecords else 0.0,
      sampleFailures = sampleFailures,
      errorMessage = errorMessage,
      severity = severity,
      executionTimeMs = executionTimeMs
    )
  }
}
