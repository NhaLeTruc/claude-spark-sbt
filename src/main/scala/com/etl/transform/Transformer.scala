package com.etl.transform

import com.etl.config.TransformConfig
import org.apache.spark.sql.DataFrame

/**
 * Transformer interface (Strategy pattern + Functional programming).
 * All data transformations must implement this trait as pure functions
 * that take a DataFrame and configuration, returning a transformed DataFrame.
 *
 * Transformations should:
 * - Be pure functions (no side effects, deterministic)
 * - Not mutate input DataFrame (Spark DataFrames are immutable)
 * - Extract parameters from TransformConfig.parameters map
 * - Be composable via chaining: df.transform(t1).transform(t2)
 * - Throw exceptions on invalid configuration
 *
 * Example implementations:
 * - AggregationTransform: Group by columns and apply aggregations
 * - JoinTransform: Join with another dataset
 * - WindowingTransform: Apply time-based or count-based windows
 *
 * Functional composition example:
 * {{{
 * val result = df
 *   .transform(AggregationTransform.apply(aggConfig))
 *   .transform(FilterTransform.apply(filterConfig))
 * }}}
 */
trait Transformer {

  /**
   * Transform DataFrame according to configuration.
   *
   * @param df Input DataFrame
   * @param config Transform configuration (type and parameters)
   * @return Transformed DataFrame
   * @throws IllegalArgumentException if config is invalid
   */
  def transform(df: DataFrame, config: TransformConfig): DataFrame
}
