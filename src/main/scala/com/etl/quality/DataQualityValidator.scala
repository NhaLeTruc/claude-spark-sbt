package com.etl.quality

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

/**
 * Action to take when data quality validation fails.
 */
sealed trait OnFailureAction
object OnFailureAction {
  case object Abort extends OnFailureAction    // Stop pipeline execution
  case object Continue extends OnFailureAction  // Log errors and continue
  case object Warn extends OnFailureAction      // Log warnings only

  def fromString(s: String): OnFailureAction = s.toLowerCase match {
    case "abort"    => Abort
    case "continue" => Continue
    case "warn"     => Warn
    case _          => throw new IllegalArgumentException(s"Unknown onFailure action: $s")
  }
}

/**
 * Comprehensive report of data quality validation results.
 *
 * @param totalRules Total number of rules executed
 * @param passedRules Number of rules that passed
 * @param failedRules Number of rules that failed
 * @param results Individual validation results
 * @param executionTimeMs Total execution time in milliseconds
 * @param overallPassed Whether all critical rules passed
 */
case class DataQualityReport(
  totalRules: Int,
  passedRules: Int,
  failedRules: Int,
  results: Seq[ValidationResult],
  executionTimeMs: Long,
  overallPassed: Boolean
) {
  /**
   * Get summary statistics.
   */
  def summary: String = {
    val status = if (overallPassed) "✓ PASSED" else "✗ FAILED"
    s"Data Quality Report: $status ($passedRules/$totalRules rules passed in ${executionTimeMs}ms)"
  }

  /**
   * Get results by severity.
   */
  def errorResults: Seq[ValidationResult] = results.filter(_.severity == Severity.Error)
  def warningResults: Seq[ValidationResult] = results.filter(_.severity == Severity.Warning)
  def infoResults: Seq[ValidationResult] = results.filter(_.severity == Severity.Info)

  /**
   * Get failed results only.
   */
  def failures: Seq[ValidationResult] = results.filterNot(_.passed)

  /**
   * Check if any error-level rules failed.
   */
  def hasErrors: Boolean = errorResults.exists(!_.passed)

  /**
   * Check if any warning-level rules failed.
   */
  def hasWarnings: Boolean = warningResults.exists(!_.passed)
}

/**
 * Orchestrator for executing data quality rules on DataFrames.
 *
 * The validator executes a collection of data quality rules and produces
 * a comprehensive report. It can be configured to either abort or continue
 * on validation failures.
 */
object DataQualityValidator {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Validate a DataFrame against a set of rules.
   *
   * @param df DataFrame to validate
   * @param rules Data quality rules to execute
   * @param onFailure Action to take on validation failure
   * @return DataQualityReport with results
   * @throws DataQualityException if onFailure is Abort and validation fails
   */
  def validate(
    df: DataFrame,
    rules: Seq[DataQualityRule],
    onFailure: OnFailureAction = OnFailureAction.Abort
  ): DataQualityReport = {

    if (rules.isEmpty) {
      logger.info("No data quality rules configured, skipping validation")
      return DataQualityReport(
        totalRules = 0,
        passedRules = 0,
        failedRules = 0,
        results = Seq.empty,
        executionTimeMs = 0L,
        overallPassed = true
      )
    }

    val startTime = System.currentTimeMillis()

    logger.info(s"Starting data quality validation with ${rules.size} rules")
    logger.info(s"OnFailure action: $onFailure")

    // Execute all rules
    val results = rules.map { rule =>
      try {
        logger.debug(s"Executing rule: ${rule.name}")
        val result = rule.validate(df)
        logger.debug(s"Rule ${rule.name} completed: ${result.summaryMessage}")
        result
      } catch {
        case ex: Exception =>
          logger.error(s"Rule ${rule.name} threw exception: ${ex.getMessage}", ex)
          ValidationResult(
            ruleName = rule.name,
            passed = false,
            totalRecords = 0L,
            failedRecords = 0L,
            failureRate = 0.0,
            sampleFailures = Seq.empty,
            errorMessage = Some(s"Rule execution exception: ${ex.getMessage}"),
            severity = rule.severity,
            executionTimeMs = 0L
          )
      }
    }

    val executionTimeMs = System.currentTimeMillis() - startTime

    // Calculate summary statistics
    val passedRules = results.count(_.passed)
    val failedRules = results.count(!_.passed)
    val errorFailures = results.filter(r => !r.passed && r.severity == Severity.Error)

    val overallPassed = errorFailures.isEmpty

    val report = DataQualityReport(
      totalRules = rules.size,
      passedRules = passedRules,
      failedRules = failedRules,
      results = results,
      executionTimeMs = executionTimeMs,
      overallPassed = overallPassed
    )

    // Log report summary
    logger.info(s"Data quality validation completed: ${report.summary}")

    // Log individual results
    results.foreach { result =>
      val logLevel = if (result.passed) "INFO" else {
        result.severity match {
          case Severity.Error   => "ERROR"
          case Severity.Warning => "WARN"
          case Severity.Info    => "INFO"
        }
      }

      val message = result.summaryMessage
      logLevel match {
        case "ERROR" => logger.error(message)
        case "WARN"  => logger.warn(message)
        case _       => logger.info(message)
      }

      // Log error message if present
      result.errorMessage.foreach(msg => logger.error(s"  ${result.ruleName}: $msg"))

      // Log sample failures
      if (result.sampleFailures.nonEmpty && !result.passed) {
        logger.warn(s"  ${result.ruleName}: Sample failures (showing ${result.sampleFailures.size}):")
        result.sampleFailures.take(3).foreach { row =>
          logger.warn(s"    - ${row.mkString(", ")}")
        }
      }
    }

    // Handle failure action
    if (!overallPassed) {
      val failureCount = errorFailures.size
      val failureMessage = s"Data quality validation failed: $failureCount error-level rule(s) failed"

      onFailure match {
        case OnFailureAction.Abort =>
          logger.error(s"$failureMessage. Aborting pipeline.")
          throw new DataQualityException(failureMessage, report)

        case OnFailureAction.Continue =>
          logger.warn(s"$failureMessage. Continuing pipeline (onFailure=Continue).")

        case OnFailureAction.Warn =>
          logger.warn(s"$failureMessage. Treating as warning (onFailure=Warn).")
      }
    }

    report
  }

  /**
   * Validate with default onFailure action (Abort).
   */
  def validate(df: DataFrame, rules: Seq[DataQualityRule]): DataQualityReport = {
    validate(df, rules, OnFailureAction.Abort)
  }

  /**
   * Validate and throw exception if any rule fails.
   */
  def validateOrThrow(df: DataFrame, rules: Seq[DataQualityRule]): Unit = {
    validate(df, rules, OnFailureAction.Abort)
  }

  /**
   * Validate and log warnings but continue on failure.
   */
  def validateWithWarnings(df: DataFrame, rules: Seq[DataQualityRule]): DataQualityReport = {
    validate(df, rules, OnFailureAction.Continue)
  }
}

/**
 * Exception thrown when data quality validation fails with Abort action.
 *
 * @param message Error message
 * @param report Full validation report
 */
class DataQualityException(
  message: String,
  val report: DataQualityReport
) extends RuntimeException(message) {

  /**
   * Get detailed failure summary.
   */
  def getFailureSummary: String = {
    val failures = report.failures
    val summary = new StringBuilder()
    summary.append(s"Data Quality Validation Failed:\n")
    summary.append(s"  Total Rules: ${report.totalRules}\n")
    summary.append(s"  Failed Rules: ${report.failedRules}\n")
    summary.append(s"  Failures:\n")

    failures.foreach { result =>
      summary.append(s"    - ${result.summaryMessage}\n")
      result.errorMessage.foreach(msg => summary.append(s"      Error: $msg\n"))
    }

    summary.toString()
  }
}
