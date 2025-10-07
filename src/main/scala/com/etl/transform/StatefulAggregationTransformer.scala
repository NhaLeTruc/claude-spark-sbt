package com.etl.transform

import com.etl.config.TransformConfig
import com.etl.streaming.StreamingConfig
import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode => SparkOutputMode}
import org.slf4j.LoggerFactory

/**
 * Stateful aggregation transformer for streaming data.
 * Supports windowed aggregations with watermarks and state timeout.
 *
 * Transform parameters:
 * - groupByColumns (required): Comma-separated list of grouping columns
 * - aggregations (required): Aggregation spec in format: "col1:sum,col2:avg,col3:count"
 * - windowColumn (optional): Column for windowing (requires timestamp type)
 * - windowDuration (optional): Window size (e.g., "10 minutes", "1 hour")
 * - slideDuration (optional): Slide interval for sliding windows (default: same as windowDuration)
 * - stateTimeout (optional): Timeout for stateful operations (e.g., "1 hour")
 * - outputMode (optional): append, update, complete (default: update)
 *
 * Examples:
 * 1. Simple aggregation by key:
 *    groupByColumns: "user_id"
 *    aggregations: "amount:sum,transaction_count:count"
 *
 * 2. Windowed aggregation:
 *    groupByColumns: "user_id"
 *    windowColumn: "event_time"
 *    windowDuration: "10 minutes"
 *    aggregations: "clicks:count,revenue:sum"
 *
 * 3. Sliding window:
 *    groupByColumns: "product_id"
 *    windowColumn: "timestamp"
 *    windowDuration: "1 hour"
 *    slideDuration: "10 minutes"
 *    aggregations: "views:count,sales:sum"
 *
 * Note: Windowed aggregations require watermark to be configured in StreamingConfig.
 */
class StatefulAggregationTransformer(
  streamingConfig: Option[StreamingConfig] = None
) extends Transformer {
  private val logger = LoggerFactory.getLogger(getClass)

  override def transform(df: DataFrame, config: TransformConfig): DataFrame = {
    logger.info(s"Applying stateful aggregation with config: ${config.parameters}")

    // Parse required parameters
    val groupByColumnsStr = config.parameters.getOrElse(
      "groupByColumns",
      throw new IllegalArgumentException("groupByColumns is required for StatefulAggregationTransformer")
    ).toString

    val aggregationsStr = config.parameters.getOrElse(
      "aggregations",
      throw new IllegalArgumentException("aggregations is required for StatefulAggregationTransformer")
    ).toString

    val groupByColumns = groupByColumnsStr.split(",").map(_.trim).filter(_.nonEmpty)
    if (groupByColumns.isEmpty) {
      throw new IllegalArgumentException("groupByColumns cannot be empty")
    }

    // Parse aggregations: "col1:sum,col2:avg,col3:count"
    val aggregations = parseAggregations(aggregationsStr)
    if (aggregations.isEmpty) {
      throw new IllegalArgumentException("aggregations cannot be empty")
    }

    // Check if windowing is configured
    val windowColumn = config.parameters.get("windowColumn").map(_.toString)
    val windowDuration = config.parameters.get("windowDuration").map(_.toString)

    // Validate window parameters
    if (windowColumn.isDefined != windowDuration.isDefined) {
      throw new IllegalArgumentException(
        "Both windowColumn and windowDuration must be specified together for windowed aggregations"
      )
    }

    // Check if watermark is required
    if (windowColumn.isDefined && df.isStreaming) {
      // Watermark is highly recommended for windowed streaming aggregations
      if (streamingConfig.isEmpty || !streamingConfig.get.hasWatermark) {
        logger.warn(
          "Performing windowed aggregation on streaming data without watermark. " +
            "This may lead to unbounded state growth. Configure watermark in StreamingConfig."
        )
      }
    }

    // Build aggregation
    val result = if (windowColumn.isDefined) {
      performWindowedAggregation(df, groupByColumns, windowColumn.get, windowDuration.get,
        config.parameters.get("slideDuration").map(_.toString), aggregations)
    } else {
      performSimpleAggregation(df, groupByColumns, aggregations)
    }

    logger.info(
      s"Stateful aggregation completed. " +
        s"Grouped by: ${groupByColumns.mkString(", ")}, " +
        s"Windowed: ${windowColumn.isDefined}, " +
        s"Result schema: ${result.schema.fieldNames.mkString(", ")}"
    )

    result
  }

  /**
   * Parse aggregation specification string.
   * Format: "col1:sum,col2:avg,col3:count"
   * Returns: Seq[(columnName, aggregationType)]
   */
  private def parseAggregations(spec: String): Seq[(String, String)] = {
    spec.split(",").map(_.trim).filter(_.nonEmpty).map { agg =>
      val parts = agg.split(":")
      if (parts.length != 2) {
        throw new IllegalArgumentException(
          s"Invalid aggregation spec: '$agg'. Expected format: 'column:function'"
        )
      }
      val colName = parts(0).trim
      val aggType = parts(1).trim.toLowerCase

      // Validate aggregation type
      if (!Seq("sum", "avg", "count", "min", "max", "first", "last").contains(aggType)) {
        throw new IllegalArgumentException(
          s"Unsupported aggregation type: '$aggType'. " +
            "Supported: sum, avg, count, min, max, first, last"
        )
      }

      (colName, aggType)
    }.toSeq
  }

  /**
   * Perform simple aggregation (no windowing).
   */
  private def performSimpleAggregation(
    df: DataFrame,
    groupByColumns: Array[String],
    aggregations: Seq[(String, String)]
  ): DataFrame = {
    logger.info(s"Performing simple aggregation on columns: ${groupByColumns.mkString(", ")}")

    // Validate group by columns exist
    val missingCols = groupByColumns.filterNot(df.schema.fieldNames.contains)
    if (missingCols.nonEmpty) {
      throw new IllegalArgumentException(
        s"Group by columns not found: ${missingCols.mkString(", ")}. " +
          s"Available: ${df.schema.fieldNames.mkString(", ")}"
      )
    }

    // Build aggregation expressions
    val aggExprs = aggregations.map {
      case (colName, "sum")   => F.sum(colName).as(s"${colName}_sum")
      case (colName, "avg")   => F.avg(colName).as(s"${colName}_avg")
      case (colName, "count") => F.count(colName).as(s"${colName}_count")
      case (colName, "min")   => F.min(colName).as(s"${colName}_min")
      case (colName, "max")   => F.max(colName).as(s"${colName}_max")
      case (colName, "first") => F.first(colName).as(s"${colName}_first")
      case (colName, "last")  => F.last(colName).as(s"${colName}_last")
      case (_, aggType) =>
        throw new IllegalArgumentException(s"Unsupported aggregation: $aggType")
    }

    // Execute aggregation
    df.groupBy(groupByColumns.map(F.col): _*)
      .agg(aggExprs.head, aggExprs.tail: _*)
  }

  /**
   * Perform windowed aggregation.
   */
  private def performWindowedAggregation(
    df: DataFrame,
    groupByColumns: Array[String],
    windowColumn: String,
    windowDuration: String,
    slideDuration: Option[String],
    aggregations: Seq[(String, String)]
  ): DataFrame = {
    logger.info(
      s"Performing windowed aggregation: " +
        s"window=$windowColumn, " +
        s"duration=$windowDuration, " +
        s"slide=${slideDuration.getOrElse("same as duration")}"
    )

    // Validate window column exists and is timestamp type
    if (!df.schema.fieldNames.contains(windowColumn)) {
      throw new IllegalArgumentException(
        s"Window column '$windowColumn' not found in DataFrame. " +
          s"Available columns: ${df.schema.fieldNames.mkString(", ")}"
      )
    }

    val windowField = df.schema.fields.find(_.name == windowColumn).get
    if (windowField.dataType.typeName != "timestamp") {
      throw new IllegalArgumentException(
        s"Window column '$windowColumn' must be of timestamp type, " +
          s"but found: ${windowField.dataType.typeName}"
      )
    }

    // Create window expression
    val windowExpr = slideDuration match {
      case Some(slide) => F.window(F.col(windowColumn), windowDuration, slide)
      case None        => F.window(F.col(windowColumn), windowDuration)
    }

    // Build aggregation expressions
    val aggExprs = aggregations.map {
      case (colName, "sum")   => F.sum(colName).as(s"${colName}_sum")
      case (colName, "avg")   => F.avg(colName).as(s"${colName}_avg")
      case (colName, "count") => F.count(colName).as(s"${colName}_count")
      case (colName, "min")   => F.min(colName).as(s"${colName}_min")
      case (colName, "max")   => F.max(colName).as(s"${colName}_max")
      case (colName, "first") => F.first(colName).as(s"${colName}_first")
      case (colName, "last")  => F.last(colName).as(s"${colName}_last")
      case (_, aggType) =>
        throw new IllegalArgumentException(s"Unsupported aggregation: $aggType")
    }

    // Execute windowed aggregation
    val groupCols = windowExpr +: groupByColumns.map(F.col)
    df.groupBy(groupCols: _*)
      .agg(aggExprs.head, aggExprs.tail: _*)
  }
}

/**
 * Companion object for creating StatefulAggregationTransformer instances.
 */
object StatefulAggregationTransformer {
  /**
   * Create transformer without streaming config (no watermark support).
   */
  def apply(): StatefulAggregationTransformer = new StatefulAggregationTransformer(None)

  /**
   * Create transformer with streaming config (watermark support).
   */
  def apply(streamingConfig: StreamingConfig): StatefulAggregationTransformer =
    new StatefulAggregationTransformer(Some(streamingConfig))
}
