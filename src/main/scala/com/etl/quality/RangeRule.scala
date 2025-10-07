package com.etl.quality

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Data quality rule that validates numeric columns fall within a specified range.
 *
 * This rule checks that numeric values are between min and max (inclusive).
 * Common use cases:
 * - Transaction amounts must be positive
 * - User age must be between 0 and 150
 * - Product prices must be within business constraints
 * - Percentage values must be between 0 and 100
 *
 * Example business rules:
 * - Order amount must be between $0.01 and $1,000,000
 * - Temperature readings must be between -50°C and 50°C
 * - Discount percentage must be between 0% and 75%
 *
 * @param column Column name to validate
 * @param min Minimum allowed value (inclusive)
 * @param max Maximum allowed value (inclusive)
 * @param severity Severity level (Error, Warning, Info)
 * @param ruleName Optional custom name (default: "RangeRule")
 */
class RangeRule(
  val column: String,
  val min: Double,
  val max: Double,
  val severity: Severity = Severity.Error,
  override val name: String = "RangeRule"
) extends DataQualityRule {

  require(min <= max, s"Min value ($min) must be <= max value ($max)")

  /**
   * Validate that the column values fall within the specified range.
   */
  override def validate(df: DataFrame): ValidationResult = {
    val startTime = System.currentTimeMillis()

    try {
      logger.info(s"Executing $name on column '$column' (range: [$min, $max])")

      // Validate that column exists
      if (!df.schema.fieldNames.contains(column)) {
        val errorMsg = s"Column '$column' not found in DataFrame. Available: ${df.schema.fieldNames.mkString(", ")}"
        logger.error(errorMsg)
        return failure(
          totalRecords = 0L,
          failedRecords = 0L,
          sampleFailures = Seq.empty,
          executionTimeMs = System.currentTimeMillis() - startTime,
          errorMessage = Some(errorMsg)
        )
      }

      // Validate that column is numeric
      val dataType = df.schema(column).dataType
      if (!isNumericType(dataType)) {
        val errorMsg = s"Column '$column' has non-numeric type: $dataType. RangeRule requires numeric columns."
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

      // Find rows outside the range (including nulls)
      val outOfRangeCondition = col(column).isNull or
                                 col(column) < lit(min) or
                                 col(column) > lit(max)

      val failedDf = df.filter(outOfRangeCondition)
      val failedRecords = failedDf.count()

      val executionTimeMs = System.currentTimeMillis() - startTime

      if (failedRecords == 0) {
        logger.info(s"$name: PASSED - All values in column '$column' are within range [$min, $max]")
        success(totalRecords, executionTimeMs)
      } else {
        val failurePercent = (failedRecords.toDouble / totalRecords * 100).formatted("%.2f")
        logger.warn(
          s"$name: FAILED - Found $failedRecords values ($failurePercent%) outside range [$min, $max] in column '$column'"
        )

        // Collect sample failures with stats
        val sampleFailures = collectSampleFailures(failedDf)

        // Log statistics about violations
        val stats = failedDf.agg(
          min(column).as("min_value"),
          max(column).as("max_value"),
          count(when(col(column).isNull, 1)).as("null_count")
        ).first()

        logger.info(
          s"$name: Range violation stats - " +
          s"Min found: ${stats.getAs[Any]("min_value")}, " +
          s"Max found: ${stats.getAs[Any]("max_value")}, " +
          s"Nulls: ${stats.getAs[Long]("null_count")}"
        )

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

  /**
   * Check if a Spark data type is numeric.
   */
  private def isNumericType(dataType: DataType): Boolean = dataType match {
    case _: ByteType | _: ShortType | _: IntegerType | _: LongType |
         _: FloatType | _: DoubleType | _: DecimalType => true
    case _ => false
  }

  override def toString: String = {
    s"RangeRule(column=$column, range=[$min, $max], severity=$severity)"
  }
}

object RangeRule {
  /**
   * Create a RangeRule with default severity (Error).
   */
  def apply(column: String, min: Double, max: Double): RangeRule = {
    new RangeRule(column, min, max, Severity.Error, s"RangeRule($column)")
  }

  /**
   * Create a RangeRule with custom severity.
   */
  def apply(column: String, min: Double, max: Double, severity: Severity): RangeRule = {
    new RangeRule(column, min, max, severity, s"RangeRule($column)")
  }

  /**
   * Create a RangeRule with custom name.
   */
  def apply(column: String, min: Double, max: Double, severity: Severity, name: String): RangeRule = {
    new RangeRule(column, min, max, severity, name)
  }

  /**
   * Create a positive values rule (> 0).
   */
  def positive(column: String, severity: Severity = Severity.Error): RangeRule = {
    new RangeRule(column, 0.01, Double.MaxValue, severity, s"PositiveRule($column)")
  }

  /**
   * Create a non-negative values rule (>= 0).
   */
  def nonNegative(column: String, severity: Severity = Severity.Error): RangeRule = {
    new RangeRule(column, 0.0, Double.MaxValue, severity, s"NonNegativeRule($column)")
  }

  /**
   * Create a percentage rule (0 to 100).
   */
  def percentage(column: String, severity: Severity = Severity.Error): RangeRule = {
    new RangeRule(column, 0.0, 100.0, severity, s"PercentageRule($column)")
  }
}
