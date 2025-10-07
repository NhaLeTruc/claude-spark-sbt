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

    if (joinColumns.isEmpty) {
      throw new IllegalArgumentException("joinColumns must contain at least one column")
    }

    logger.info(s"Joining on columns: ${joinColumns.mkString(", ")}")

    // Validate columns exist in both DataFrames
    val leftColumns = df.schema.fieldNames.toSet
    val rightColumns = rightDf.schema.fieldNames.toSet

    joinColumns.foreach { col =>
      if (!leftColumns.contains(col)) {
        throw new IllegalArgumentException(s"Join column '$col' not found in left DataFrame")
      }
      if (!rightColumns.contains(col)) {
        throw new IllegalArgumentException(s"Join column '$col' not found in right DataFrame")
      }
    }

    // Build join condition
    val joinCondition = joinColumns.map { colName =>
      df(colName) === rightDf(colName)
    }.reduce(_ && _)

    // Perform join
    val result = df.join(rightDf, joinCondition, joinType)

    logger.info(
      s"Join complete. " +
        s"Left rows: ${df.count()}, " +
        s"Right rows: ${rightDf.count()}, " +
        s"Result rows: ${result.count()}, " +
        s"Result columns: ${result.schema.fieldNames.mkString(", ")}"
    )

    result
  }
}
