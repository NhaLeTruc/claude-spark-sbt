package com.etl.transform

import com.etl.config.TransformConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

/**
 * Aggregation transformer for groupBy and aggregate operations.
 *
 * Parameters:
 * - groupBy (required): JSON array of column names to group by, e.g., ["user_id", "date"]
 * - aggregations (required): JSON object mapping column names to aggregation functions
 *   Format: {"column1": "func1,func2", "column2": "func3"}
 *   Supported functions: sum, avg, min, max, count, first, last
 *
 * Example:
 * {
 *   "groupBy": "[\"user_id\"]",
 *   "aggregations": "{\"amount\": \"sum,avg\", \"transaction_id\": \"count\"}"
 * }
 */
class AggregationTransformer extends Transformer {
  private val logger = LoggerFactory.getLogger(getClass)

  override def transform(df: DataFrame, config: TransformConfig): DataFrame = {
    logger.info(s"Applying aggregation transformation with config: ${config.parameters}")

    // Parse groupBy columns
    val groupByJson = config.parameters.getOrElse(
      "groupBy",
      throw new IllegalArgumentException("groupBy parameter is required for aggregation")
    )
    val groupByCols = Json.parse(groupByJson).as[Seq[String]]

    if (groupByCols.isEmpty) {
      throw new IllegalArgumentException("groupBy must contain at least one column")
    }

    logger.info(s"Grouping by columns: ${groupByCols.mkString(", ")}")

    // Parse aggregations
    val aggregationsJson = config.parameters.getOrElse(
      "aggregations",
      throw new IllegalArgumentException("aggregations parameter is required for aggregation")
    )
    val aggregationsMap = Json.parse(aggregationsJson).as[Map[String, String]]

    if (aggregationsMap.isEmpty) {
      throw new IllegalArgumentException("aggregations must contain at least one column aggregation")
    }

    logger.info(s"Applying aggregations: $aggregationsMap")

    // Build aggregation expressions
    val aggExprs = aggregationsMap.flatMap { case (column, functions) =>
      functions.split(",").map(_.trim.toLowerCase).map {
        case "sum" => sum(col(column)).as(s"sum($column)")
        case "avg" => avg(col(column)).as(s"avg($column)")
        case "min" => min(col(column)).as(s"min($column)")
        case "max" => max(col(column)).as(s"max($column)")
        case "count" => count(col(column)).as(s"count($column)")
        case "first" => first(col(column)).as(s"first($column)")
        case "last" => last(col(column)).as(s"last($column)")
        case other =>
          throw new IllegalArgumentException(
            s"Unsupported aggregation function: $other. " +
              "Supported: sum, avg, min, max, count, first, last"
          )
      }
    }.toSeq

    // Apply groupBy and aggregations
    val result = df.groupBy(groupByCols.map(col): _*)
      .agg(aggExprs.head, aggExprs.tail: _*)

    logger.info(
      s"Aggregation complete. " +
        s"Input rows: ${df.count()}, " +
        s"Output groups: ${result.count()}, " +
        s"Output columns: ${result.schema.fieldNames.mkString(", ")}"
    )

    result
  }
}
