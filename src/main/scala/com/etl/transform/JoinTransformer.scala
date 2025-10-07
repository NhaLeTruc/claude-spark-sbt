package com.etl.transform

import com.etl.config.TransformConfig
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

/**
 * Join transformer for joining two DataFrames.
 *
 * Parameters:
 * - joinType (required): Type of join - inner, left, right, outer, left_semi, left_anti
 * - joinColumns (required): JSON array of column names to join on, e.g., ["user_id", "date"]
 *
 * The second DataFrame (right side) is provided via constructor.
 *
 * Example:
 * {
 *   "joinType": "inner",
 *   "joinColumns": "[\"user_id\"]"
 * }
 */
class JoinTransformer(rightDf: DataFrame) extends Transformer {
  private val logger = LoggerFactory.getLogger(getClass)

  override def transform(df: DataFrame, config: TransformConfig): DataFrame = {
    logger.info(s"Applying join transformation with config: ${config.parameters}")

    // Validate input DataFrames
    validateInputDataFrames(df, rightDf)

    // Parse join type
    val joinType = config.parameters.getOrElse(
      "joinType",
      throw new IllegalArgumentException("joinType parameter is required for join")
    ).toLowerCase

    // Validate join type
    val validJoinTypes = Set("inner", "left", "right", "outer", "left_semi", "left_anti", "cross")
    if (!validJoinTypes.contains(joinType)) {
      throw new IllegalArgumentException(
        s"Invalid joinType: $joinType. " +
          s"Valid types: ${validJoinTypes.mkString(", ")}"
      )
    }

    logger.info(s"Join type: $joinType")

    // Parse join columns
    val joinColumnsJson = config.parameters.getOrElse(
      "joinColumns",
      throw new IllegalArgumentException("joinColumns parameter is required for join")
    )
    val joinColumns = Json.parse(joinColumnsJson).as[Seq[String]]

    if (joinColumns.isEmpty && joinType != "cross") {
      throw new IllegalArgumentException("joinColumns must contain at least one column for non-cross joins")
    }

    if (joinColumns.nonEmpty && joinType == "cross") {
      logger.warn(s"Cross join does not use join columns, but ${joinColumns.size} column(s) provided")
    }

    logger.info(s"Joining on columns: ${joinColumns.mkString(", ")}")

    // Validate columns exist in both DataFrames (skip for cross join)
    if (joinType != "cross") {
      validateJoinColumns(df, rightDf, joinColumns)
    }

    // Perform join
    val result = if (joinType == "cross" || joinColumns.isEmpty) {
      df.join(rightDf, joinType = joinType)
    } else {
      // Build join condition
      val joinCondition = joinColumns.map { colName =>
        df(colName) === rightDf(colName)
      }.reduce(_ && _)
      df.join(rightDf, joinCondition, joinType)
    }

    logger.info(
      s"Join complete. " +
        s"Left rows: ${df.count()}, " +
        s"Right rows: ${rightDf.count()}, " +
        s"Result rows: ${result.count()}, " +
        s"Result columns: ${result.schema.fieldNames.mkString(", ")}"
    )

    result
  }

  /**
   * Validate that input DataFrames are not empty and have valid schemas.
   */
  private def validateInputDataFrames(leftDf: DataFrame, rightDf: DataFrame): Unit = {
    if (leftDf.schema.isEmpty) {
      throw new IllegalArgumentException("Left DataFrame schema is empty")
    }
    if (rightDf.schema.isEmpty) {
      throw new IllegalArgumentException("Right DataFrame schema is empty")
    }
  }

  /**
   * Validate that join columns exist in both DataFrames.
   */
  private def validateJoinColumns(leftDf: DataFrame, rightDf: DataFrame, joinColumns: Seq[String]): Unit = {
    val leftColumns = leftDf.schema.fieldNames.toSet
    val rightColumns = rightDf.schema.fieldNames.toSet

    val missingFromLeft = joinColumns.filterNot(leftColumns.contains)
    val missingFromRight = joinColumns.filterNot(rightColumns.contains)

    if (missingFromLeft.nonEmpty) {
      throw new IllegalArgumentException(
        s"Join columns not found in left DataFrame: ${missingFromLeft.mkString(", ")}. " +
          s"Available columns: ${leftColumns.mkString(", ")}"
      )
    }

    if (missingFromRight.nonEmpty) {
      throw new IllegalArgumentException(
        s"Join columns not found in right DataFrame: ${missingFromRight.mkString(", ")}. " +
          s"Available columns: ${rightColumns.mkString(", ")}"
      )
    }
  }
}
