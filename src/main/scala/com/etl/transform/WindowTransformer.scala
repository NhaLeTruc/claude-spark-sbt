package com.etl.transform

import com.etl.config.TransformConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

/**
 * Window transformer for window functions (ranking, aggregation, lag/lead).
 *
 * Parameters:
 * - partitionBy (required): JSON array of columns to partition by, e.g., ["user_id"]
 * - orderBy (required): JSON array of columns to order by (supports DESC), e.g., ["timestamp", "score DESC"]
 * - windowFunction (required): Window function to apply
 *   Ranking: row_number, rank, dense_rank
 *   Aggregations: sum, avg, min, max, count
 *   Offset: lag, lead
 * - aggregateColumn (optional): Column to aggregate (required for sum/avg/min/max/count/lag/lead)
 * - offset (optional): Offset for lag/lead (default: 1)
 * - outputColumn (required): Name of the output column
 *
 * Example:
 * {
 *   "partitionBy": "[\"user_id\"]",
 *   "orderBy": "[\"timestamp\"]",
 *   "windowFunction": "row_number",
 *   "outputColumn": "event_sequence"
 * }
 */
class WindowTransformer extends Transformer {
  private val logger = LoggerFactory.getLogger(getClass)

  override def transform(df: DataFrame, config: TransformConfig): DataFrame = {
    logger.info(s"Applying window transformation with config: ${config.parameters}")

    // Parse partitionBy columns
    val partitionByJson = config.parameters.getOrElse(
      "partitionBy",
      throw new IllegalArgumentException("partitionBy parameter is required for window function")
    )
    val partitionByCols = Json.parse(partitionByJson).as[Seq[String]]

    if (partitionByCols.isEmpty) {
      throw new IllegalArgumentException("partitionBy must contain at least one column")
    }

    // Parse orderBy columns
    val orderByJson = config.parameters.getOrElse(
      "orderBy",
      throw new IllegalArgumentException("orderBy parameter is required for window function")
    )
    val orderBySpecs = Json.parse(orderByJson).as[Seq[String]]

    if (orderBySpecs.isEmpty) {
      throw new IllegalArgumentException("orderBy must contain at least one column")
    }

    // Parse window function
    val windowFunction = config.parameters.getOrElse(
      "windowFunction",
      throw new IllegalArgumentException("windowFunction parameter is required")
    ).toLowerCase

    // Parse output column
    val outputColumn = config.parameters.getOrElse(
      "outputColumn",
      throw new IllegalArgumentException("outputColumn parameter is required")
    )

    logger.info(
      s"Window function: $windowFunction, " +
        s"partitionBy: ${partitionByCols.mkString(", ")}, " +
        s"orderBy: ${orderBySpecs.mkString(", ")}, " +
        s"outputColumn: $outputColumn"
    )

    // Build window specification
    var windowSpec = Window.partitionBy(partitionByCols.map(col): _*)

    // Add order by with ASC/DESC support
    val orderByCols = orderBySpecs.map { spec =>
      val parts = spec.trim.split("\\s+")
      val colName = parts(0)
      val direction = if (parts.length > 1 && parts(1).equalsIgnoreCase("DESC")) "desc" else "asc"

      if (direction == "desc") {
        col(colName).desc
      } else {
        col(colName).asc
      }
    }

    windowSpec = windowSpec.orderBy(orderByCols: _*)

    // Apply window function
    val windowExpr = windowFunction match {
      // Ranking functions
      case "row_number" =>
        row_number().over(windowSpec)

      case "rank" =>
        rank().over(windowSpec)

      case "dense_rank" =>
        dense_rank().over(windowSpec)

      // Aggregate functions
      case "sum" =>
        val aggCol = config.parameters.getOrElse(
          "aggregateColumn",
          throw new IllegalArgumentException("aggregateColumn is required for sum function")
        )
        sum(col(aggCol)).over(windowSpec)

      case "avg" =>
        val aggCol = config.parameters.getOrElse(
          "aggregateColumn",
          throw new IllegalArgumentException("aggregateColumn is required for avg function")
        )
        avg(col(aggCol)).over(windowSpec)

      case "min" =>
        val aggCol = config.parameters.getOrElse(
          "aggregateColumn",
          throw new IllegalArgumentException("aggregateColumn is required for min function")
        )
        min(col(aggCol)).over(windowSpec)

      case "max" =>
        val aggCol = config.parameters.getOrElse(
          "aggregateColumn",
          throw new IllegalArgumentException("aggregateColumn is required for max function")
        )
        max(col(aggCol)).over(windowSpec)

      case "count" =>
        val aggCol = config.parameters.getOrElse(
          "aggregateColumn",
          throw new IllegalArgumentException("aggregateColumn is required for count function")
        )
        count(col(aggCol)).over(windowSpec)

      // Offset functions
      case "lag" =>
        val aggCol = config.parameters.getOrElse(
          "aggregateColumn",
          throw new IllegalArgumentException("aggregateColumn is required for lag function")
        )
        val offset = config.parameters.get("offset").map(_.toInt).getOrElse(1)
        lag(col(aggCol), offset).over(windowSpec)

      case "lead" =>
        val aggCol = config.parameters.getOrElse(
          "aggregateColumn",
          throw new IllegalArgumentException("aggregateColumn is required for lead function")
        )
        val offset = config.parameters.get("offset").map(_.toInt).getOrElse(1)
        lead(col(aggCol), offset).over(windowSpec)

      case other =>
        throw new IllegalArgumentException(
          s"Unsupported window function: $other. " +
            "Supported: row_number, rank, dense_rank, sum, avg, min, max, count, lag, lead"
        )
    }

    // Add window column to DataFrame
    val result = df.withColumn(outputColumn, windowExpr)

    logger.info(
      s"Window transformation complete. " +
        s"Added column: $outputColumn, " +
        s"Total columns: ${result.schema.fieldNames.mkString(", ")}"
    )

    result
  }
}
