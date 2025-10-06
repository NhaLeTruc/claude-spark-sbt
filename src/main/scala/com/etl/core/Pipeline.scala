package com.etl.core

import com.etl.model.PipelineResult

/**
 * Pipeline execution interface (Strategy pattern).
 * All pipeline implementations must provide a run method that executes
 * the complete ETL workflow (Extract → Transform → Load) and returns
 * execution results with metrics.
 *
 * Implementations should:
 * - Execute extract, transform, load stages in sequence
 * - Validate data schemas between stages
 * - Handle errors and populate metrics
 * - Log execution progress
 *
 * Example implementation:
 * {{{
 * class ETLPipeline(
 *   extractor: Extractor,
 *   transformers: Seq[Transformer],
 *   loader: Loader
 * ) extends Pipeline {
 *   def run(context: ExecutionContext): PipelineResult = {
 *     // Extract → Validate → Transform → Validate → Load
 *   }
 * }
 * }}}
 */
trait Pipeline {

  /**
   * Execute the pipeline with given execution context.
   *
   * @param context Execution context containing SparkSession, metrics, config
   * @return PipelineResult with execution metrics and success/failure status
   */
  def run(context: ExecutionContext): PipelineResult
}
