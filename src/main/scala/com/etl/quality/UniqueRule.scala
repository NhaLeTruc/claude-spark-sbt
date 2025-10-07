package com.etl.quality

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Data quality rule that validates uniqueness constraints on columns.
 *
 * This rule checks that specified column combinations have no duplicate values.
 * Common use cases:
 * - Primary key constraints (single or composite)
 * - Unique identifiers (transaction IDs, order numbers)
 * - Natural key constraints (email addresses, SSN)
 * - Deduplication validation
 *
 * Example business rules:
 * - User email must be unique across all users
 * - Transaction ID must be unique
 * - Combination of (user_id, product_id) must be unique in user_purchases
 * - Order number must be unique per customer
 *
 * @param columns List of column names that form the uniqueness constraint
 * @param severity Severity level (Error, Warning, Info)
 * @param ruleName Optional custom name (default: "UniqueRule")
 */
class UniqueRule(
  val columns: Seq[String],
  val severity: Severity = Severity.Error,
  override val name: String = "UniqueRule"
) extends DataQualityRule {

  require(columns.nonEmpty, "UniqueRule requires at least one column")

  /**
   * Validate that the specified column combination has no duplicates.
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

      // Count distinct values
      val distinctCount = df.select(columns.map(col): _*).distinct().count()

      val executionTimeMs = System.currentTimeMillis() - startTime

      if (distinctCount == totalRecords) {
        logger.info(
          s"$name: PASSED - All $totalRecords records have unique values for columns: ${columns.mkString(", ")}"
        )
        success(totalRecords, executionTimeMs)
      } else {
        val duplicateCount = totalRecords - distinctCount
        logger.warn(
          s"$name: FAILED - Found $duplicateCount duplicate records " +
          s"(${distinctCount} unique out of $totalRecords total) for columns: ${columns.mkString(", ")}"
        )

        // Find duplicate rows
        val duplicates = df
          .groupBy(columns.map(col): _*)
          .agg(count("*").as("duplicate_count"))
          .filter(col("duplicate_count") > 1)
          .orderBy(col("duplicate_count").desc)

        val duplicateKeys = duplicates.count()
        logger.info(s"$name: Found $duplicateKeys unique keys with duplicates")

        // Get sample duplicate records
        val sampleDuplicateKeys = duplicates.limit(5).collect()

        // For each duplicate key, get sample records
        val sampleFailures = if (sampleDuplicateKeys.nonEmpty) {
          val firstKey = sampleDuplicateKeys.head
          val keyCondition = columns.map { colName =>
            col(colName) === lit(firstKey.getAs[Any](colName))
          }.reduce(_ and _)

          df.filter(keyCondition).limit(10).collect().toSeq
        } else {
          Seq.empty
        }

        // Log top duplicate keys
        sampleDuplicateKeys.foreach { row =>
          val keyValues = columns.map(c => s"$c=${row.getAs[Any](c)}").mkString(", ")
          val count = row.getAs[Long]("duplicate_count")
          logger.info(s"$name: Duplicate key [$keyValues] appears $count times")
        }

        failure(totalRecords, duplicateCount, sampleFailures, executionTimeMs)
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
    s"UniqueRule(columns=[${columns.mkString(", ")}], severity=$severity)"
  }
}

object UniqueRule {
  /**
   * Create a UniqueRule for a single column.
   */
  def apply(column: String, severity: Severity = Severity.Error): UniqueRule = {
    new UniqueRule(Seq(column), severity, s"UniqueRule($column)")
  }

  /**
   * Create a UniqueRule for multiple columns (composite key).
   */
  def apply(columns: Seq[String], severity: Severity): UniqueRule = {
    new UniqueRule(columns, severity, s"UniqueRule(${columns.mkString(",")})")
  }

  /**
   * Create a UniqueRule with custom name.
   */
  def apply(columns: Seq[String], severity: Severity, name: String): UniqueRule = {
    new UniqueRule(columns, severity, name)
  }

  /**
   * Create a primary key rule (alias for UniqueRule with Error severity).
   */
  def primaryKey(columns: Seq[String]): UniqueRule = {
    new UniqueRule(columns, Severity.Error, s"PrimaryKeyRule(${columns.mkString(",")})")
  }

  /**
   * Create a primary key rule for a single column.
   */
  def primaryKey(column: String): UniqueRule = {
    new UniqueRule(Seq(column), Severity.Error, s"PrimaryKeyRule($column)")
  }
}
