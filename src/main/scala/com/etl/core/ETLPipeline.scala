package com.etl.core

import com.etl.extract.Extractor
import com.etl.load.Loader
import com.etl.model.{PipelineFailure, PipelineResult, PipelineSuccess}
import com.etl.transform.Transformer
import com.etl.util.Logging
import org.slf4j.LoggerFactory

/**
 * Concrete ETL pipeline implementation that orchestrates extract, transform, and load stages.
 * Implements the Pipeline trait using Strategy pattern for pluggable components.
 *
 * Pipeline flow:
 * 1. Extract data using Extractor
 * 2. Apply transformations in sequence (optional)
 * 3. Load data using Loader
 * 4. Track metrics throughout execution
 *
 * @param extractor Extractor strategy for data extraction
 * @param transformers Sequence of Transformer strategies (applied in order)
 * @param loader Loader strategy for data loading
 */
case class ETLPipeline(
  extractor: Extractor,
  transformers: Seq[Transformer],
  loader: Loader
) extends Pipeline with Logging {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Run the ETL pipeline.
   *
   * @param context Execution context with SparkSession, config, and metrics
   * @return PipelineResult with success or failure details
   */
  override def run(context: ExecutionContext): PipelineResult = {
    withMDC(context.getMDCContext) {
      logger.info(
        s"Starting ETL pipeline execution. " +
          s"Transformers: ${transformers.size}"
      )

      try {
        // Stage 1: Extract
        val extractedDf = withMDC(context.getMDCContext ++ Map("stage" -> "extract")) {
          logger.info("Stage 1: Extracting data")
          val df = extractor.extract(context.config.extract)(context.spark)
          val recordCount = df.count()

          logger.info(
            s"Extraction complete. " +
              s"Records extracted: $recordCount, " +
              s"Schema: ${df.schema.fieldNames.mkString(", ")}"
          )

          // Update metrics
          val updatedMetrics = context.metrics.copy(recordsExtracted = recordCount)
          context.updateMetrics(updatedMetrics)

          df
        }

        // Stage 2: Transform (chain all transformers)
        val transformedDf = if (transformers.isEmpty) {
          logger.info("Stage 2: No transformers configured, skipping transformation")
          extractedDf
        } else {
          withMDC(context.getMDCContext ++ Map("stage" -> "transform")) {
            logger.info(s"Stage 2: Applying ${transformers.size} transformer(s)")

            val finalDf = transformers.zipWithIndex.foldLeft(extractedDf) {
              case (df, (transformer, index)) =>
                logger.info(s"Applying transformer ${index + 1}/${transformers.size}")
                val transformConfig = if (index < context.config.transforms.size) {
                  context.config.transforms(index)
                } else {
                  // Fallback config if transform configs don't match transformer count
                  context.config.transforms.headOption.getOrElse(
                    throw new IllegalStateException(
                      s"No transform config found for transformer at index $index"
                    )
                  )
                }
                transformer.transform(df, transformConfig)
            }

            val recordCount = finalDf.count()
            logger.info(
              s"Transformation complete. " +
                s"Records after transform: $recordCount, " +
                s"Schema: ${finalDf.schema.fieldNames.mkString(", ")}"
            )

            // Update metrics
            val updatedMetrics = context.metrics.copy(recordsTransformed = recordCount)
            context.updateMetrics(updatedMetrics)

            finalDf
          }
        }

        // Update transformed records if no transformers
        if (transformers.isEmpty) {
          val updatedMetrics = context.metrics.copy(recordsTransformed = context.metrics.recordsExtracted)
          context.updateMetrics(updatedMetrics)
        }

        // Stage 3: Load
        val loadResult = withMDC(context.getMDCContext ++ Map("stage" -> "load")) {
          logger.info(s"Stage 3: Loading data with mode: ${context.config.load.writeMode}")

          val result = loader.load(
            transformedDf,
            context.config.load,
            context.config.load.writeMode
          )

          logger.info(
            s"Load complete. " +
              s"Records loaded: ${result.recordsLoaded}, " +
              s"Records failed: ${result.recordsFailed}"
          )

          result
        }

        // Update final metrics
        val finalMetrics = context.metrics.copy(
          recordsLoaded = loadResult.recordsLoaded,
          recordsFailed = loadResult.recordsFailed,
          errors = loadResult.errors
        ).complete()

        context.updateMetrics(finalMetrics)

        // Return result
        if (loadResult.isSuccess) {
          logger.info(
            s"ETL pipeline completed successfully. " +
              s"Duration: ${finalMetrics.duration}ms, " +
              s"Success rate: ${finalMetrics.successRate}%"
          )
          PipelineSuccess(finalMetrics)
        } else {
          val error = new RuntimeException(
            s"Load stage failed: ${loadResult.errors.mkString(", ")}"
          )
          logger.error(s"ETL pipeline failed at load stage", Some(error))
          PipelineFailure(finalMetrics, error)
        }

      } catch {
        case e: Exception =>
          logger.error(s"ETL pipeline failed with exception: ${e.getMessage}", Some(e))

          val failedMetrics = context.metrics
            .addError(s"Pipeline failed: ${e.getMessage}")
            .complete()

          context.updateMetrics(failedMetrics)

          PipelineFailure(failedMetrics, e)
      }
    }
  }
}

object ETLPipeline {
  /**
   * Create an ETL pipeline.
   *
   * @param extractor Extractor strategy
   * @param transformers Transformer strategies (in order)
   * @param loader Loader strategy
   * @return ETLPipeline instance
   */
  def apply(
    extractor: Extractor,
    transformers: Seq[Transformer],
    loader: Loader
  ): ETLPipeline = new ETLPipeline(extractor, transformers, loader)

  /**
   * Create an ETL pipeline without transformers.
   *
   * @param extractor Extractor strategy
   * @param loader Loader strategy
   * @return ETLPipeline instance
   */
  def apply(
    extractor: Extractor,
    loader: Loader
  ): ETLPipeline = new ETLPipeline(extractor, Seq.empty, loader)
}
