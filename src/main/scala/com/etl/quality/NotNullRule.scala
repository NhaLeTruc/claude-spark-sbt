package com.etl.quality

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Data quality rule that validates columns do not contain null values.
 *
 * This rule checks that specified columns have no null or missing values.
 * Common use case: Ensuring primary keys, foreign keys, and required fields are always populated.
 *
 * Example business rules:
 * - User ID must never be null
 * - Transaction timestamp must always be present
 * - Product SKU is required for all orders
 *
 * @param columns List of column names to check for nulls
 * @param severity Severity level (Error, Warning, Info)
 * @param ruleName Optional custom name (default: "NotNullRule")
 */
class NotNullRule(
  val columns: Seq[String],
  val severity: Severity = Severity.Error,
  override val name: String = "NotNullRule"
) extends DataQualityRule {

  require(columns.nonEmpty, "NotNullRule requires at least one column")

  /**
   * Validate that specified columns contain no null values.
   */
  override def validate(df: DataFrame): ValidationResult = {
    val startTime = System.currentTimeMillis()

    try {
      logger.info(s"Executing $name on columns: ${columns.mkString(", ")}")

      // Validate that all columns exist in DataFrame
      val dfColumns = df.schema.fieldNames.toSet
      val missingColumns = columns.filterNot(dfColumns.contains)
      if (missingColumns.nonEmpty) {
        val errorMsg = s"Columns not found in DataFrame: ${missingColumns.mkString(", ")}"
        logger.error(errorMsg)
        return failure(
          totalRecords = 0L,
          failedRecords = 0L,
          sampleFailures = Seq.empty,
          executionTimeMs = System.currentTimeMillis() - startTime,
          errorMessage = Some(errorMsg)
        )
      }

      val totalRecords = df.count()

      // Build condition: any column is null
      val nullCondition = columns.map(colName => col(colName).isNull).reduce(_ or _)

      // Find rows with null values
      val failedDf = df.filter(nullCondition)
      val failedRecords = failedDf.count()

      val executionTimeMs = System.currentTimeMillis() - startTime

      if (failedRecords == 0) {
        logger.info(s"$name: PASSED - No null values found in columns: ${columns.mkString(", ")}")
        success(totalRecords, executionTimeMs)
      } else {
        val failurePercent = (failedRecords.toDouble / totalRecords * 100).formatted("%.2f")
        logger.warn(
          s"$name: FAILED - Found $failedRecords null values ($failurePercent%) in columns: ${columns.mkString(", ")}"
        )

        val sampleFailures = collectSampleFailures(failedDf)
        failure(totalRecords, failedRecords, sampleFailures, executionTimeMs)
      }

    } catch {
      case ex: Exception =>
        val executionTimeMs = System.currentTimeMillis() - startTime
        val errorMsg = s"Rule execution failed: ${ex.getMessage}"
        logger.error(errorMsg, ex)
        failure(
          totalRecords = 0L,
          failedRecords = 0L,
          sampleFailures = Seq.empty,
          executionTimeMs = executionTimeMs,
          errorMessage = Some(errorMsg)
        )
    }
  }

  override def toString: String = {
    s"NotNullRule(columns=[${columns.mkString(", ")}], severity=$severity)"
  }
}

object NotNullRule {
  /**
   * Create a NotNullRule for a single column.
   */
  def apply(column: String, severity: Severity = Severity.Error): NotNullRule = {
    new NotNullRule(Seq(column), severity, s"NotNullRule($column)")
  }

  /**
   * Create a NotNullRule for multiple columns.
   */
  def apply(columns: Seq[String], severity: Severity): NotNullRule = {
    new NotNullRule(columns, severity, s"NotNullRule(${columns.mkString(",")})")
  }

  /**
   * Create a NotNullRule with custom name.
   */
  def apply(columns: Seq[String], severity: Severity, name: String): NotNullRule = {
    new NotNullRule(columns, severity, name)
  }
}
