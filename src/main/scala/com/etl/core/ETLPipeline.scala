package com.etl.core

import com.etl.extract.Extractor
import com.etl.load.Loader
import com.etl.model.{PipelineFailure, PipelineResult, PipelineSuccess}
import com.etl.monitoring.ETLMetrics
import com.etl.quality.{DataQualityRuleFactory, DataQualityValidator, OnFailureAction}
import com.etl.transform.Transformer
import com.etl.util.Logging
import org.apache.spark.sql.DataFrame
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

      val pipelineLabels = Map("pipeline_id" -> context.config.pipelineId)
      val pipelineMode = if (context.config.mode.toString == "Streaming") "streaming" else "batch"

      // Track active pipelines
      ETLMetrics.activePipelines.inc(labels = Map("mode" -> pipelineMode))

      val startTime = System.nanoTime()

      try {
        // Stage 1: Extract
        val extractedDf = withMDC(context.getMDCContext ++ Map("stage" -> "extract")) {
          logger.info("Stage 1: Extracting data")

          val extractStart = System.nanoTime()
          val df = extractor.extractWithVault(context.config.extract, context.vault)(context.spark)
          val extractDuration = (System.nanoTime() - extractStart) / 1e9

          val recordCount = df.count()

          logger.info(
            s"Extraction complete. " +
              s"Records extracted: $recordCount, " +
              s"Schema: ${df.schema.fieldNames.mkString(", ")}"
          )

          // Update metrics
          val updatedMetrics = context.metrics.copy(recordsExtracted = recordCount)
          context.updateMetrics(updatedMetrics)

          // Update Prometheus metrics
          val extractLabels = pipelineLabels ++ Map("source_type" -> context.config.extract.sourceType.toString)
          ETLMetrics.recordsExtractedTotal.inc(recordCount.toDouble, extractLabels)
          ETLMetrics.extractDurationSeconds.observe(extractDuration, extractLabels)
          ETLMetrics.recordBatchSize.observe(recordCount.toDouble, pipelineLabels ++ Map("stage" -> "extract"))

          df
        }

        // Data Quality: Validate after extraction (if configured)
        if (context.config.dataQualityConfig.enabled &&
            context.config.dataQualityConfig.validateAfterExtract) {
          withMDC(context.getMDCContext ++ Map("stage" -> "data-quality-extract")) {
            logger.info("Running data quality validation after extraction")
            runDataQualityValidation(extractedDf, context, "extract")
          }
        }

        // Stage 2: Transform (chain all transformers)
        val transformedDf = if (transformers.isEmpty) {
          logger.info("Stage 2: No transformers configured, skipping transformation")
          extractedDf
        } else {
          withMDC(context.getMDCContext ++ Map("stage" -> "transform")) {
            logger.info(s"Stage 2: Applying ${transformers.size} transformer(s)")

            val transformStart = System.nanoTime()

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

                val txStart = System.nanoTime()
                val result = transformer.transform(df, transformConfig)
                val txDuration = (System.nanoTime() - txStart) / 1e9

                // Track per-transformer metrics
                val transformLabels = pipelineLabels ++ Map("transform_type" -> transformConfig.transformType.toString)
                ETLMetrics.transformDurationSeconds.observe(txDuration, transformLabels)

                result
            }

            val transformDuration = (System.nanoTime() - transformStart) / 1e9
            val recordCount = finalDf.count()

            logger.info(
              s"Transformation complete. " +
                s"Records after transform: $recordCount, " +
                s"Schema: ${finalDf.schema.fieldNames.mkString(", ")}"
            )

            // Update metrics
            val updatedMetrics = context.metrics.copy(recordsTransformed = recordCount)
            context.updateMetrics(updatedMetrics)

            // Update Prometheus metrics
            ETLMetrics.recordsTransformedTotal.inc(recordCount.toDouble, pipelineLabels ++ Map("transform_type" -> "all"))
            ETLMetrics.recordBatchSize.observe(recordCount.toDouble, pipelineLabels ++ Map("stage" -> "transform"))

            finalDf
          }
        }

        // Update transformed records if no transformers
        if (transformers.isEmpty) {
          val updatedMetrics = context.metrics.copy(recordsTransformed = context.metrics.recordsExtracted)
          context.updateMetrics(updatedMetrics)
        }

        // Data Quality: Validate after transformation (if configured)
        if (context.config.dataQualityConfig.enabled &&
            context.config.dataQualityConfig.validateAfterTransform) {
          withMDC(context.getMDCContext ++ Map("stage" -> "data-quality-transform")) {
            logger.info("Running data quality validation after transformation")
            runDataQualityValidation(transformedDf, context, "transform")
          }
        }

        // Stage 3: Load
        val loadResult = withMDC(context.getMDCContext ++ Map("stage" -> "load")) {
          logger.info(s"Stage 3: Loading data with mode: ${context.config.load.writeMode}")

          val loadStart = System.nanoTime()
          val result = loader.loadWithVault(
            transformedDf,
            context.config.load,
            context.config.load.writeMode,
            context.vault
          )
          val loadDuration = (System.nanoTime() - loadStart) / 1e9

          logger.info(
            s"Load complete. " +
              s"Records loaded: ${result.recordsLoaded}, " +
              s"Records failed: ${result.recordsFailed}"
          )

          // Update Prometheus metrics
          val loadLabels = pipelineLabels ++ Map(
            "sink_type" -> context.config.load.sinkType.toString,
            "write_mode" -> context.config.load.writeMode.toString
          )
          ETLMetrics.recordsLoadedTotal.inc(result.recordsLoaded.toDouble, loadLabels)
          ETLMetrics.loadDurationSeconds.observe(loadDuration, loadLabels)

          if (result.recordsFailed > 0) {
            ETLMetrics.recordsFailedTotal.inc(
              result.recordsFailed.toDouble,
              pipelineLabels ++ Map("stage" -> "load", "reason" -> "load_error")
            )
          }

          result
        }

        // Update final metrics
        val finalMetrics = context.metrics.copy(
          recordsLoaded = loadResult.recordsLoaded,
          recordsFailed = loadResult.recordsFailed,
          errors = loadResult.errors
        ).complete()

        context.updateMetrics(finalMetrics)

        // Calculate total pipeline duration
        val pipelineDuration = (System.nanoTime() - startTime) / 1e9

        // Track active pipelines (decrement)
        ETLMetrics.activePipelines.dec(labels = Map("mode" -> pipelineMode))

        // Return result
        if (loadResult.isSuccess) {
          logger.info(
            s"ETL pipeline completed successfully. " +
              s"Duration: ${finalMetrics.duration}ms, " +
              s"Success rate: ${finalMetrics.successRate}%"
          )

          // Update Prometheus metrics for successful execution
          ETLMetrics.pipelineExecutionsTotal.inc(labels = pipelineLabels ++ Map("status" -> "success"))
          ETLMetrics.pipelineDurationSeconds.observe(pipelineDuration, pipelineLabels ++ Map("status" -> "success"))

          PipelineSuccess(finalMetrics)
        } else {
          val error = new RuntimeException(
            s"Load stage failed: ${loadResult.errors.mkString(", ")}"
          )
          logger.error(s"ETL pipeline failed at load stage", Some(error))

          // Update Prometheus metrics for failed execution
          ETLMetrics.pipelineExecutionsTotal.inc(labels = pipelineLabels ++ Map("status" -> "failure"))
          ETLMetrics.pipelineDurationSeconds.observe(pipelineDuration, pipelineLabels ++ Map("status" -> "failure"))

          PipelineFailure(finalMetrics, error)
        }

      } catch {
        case e: Exception =>
          logger.error(s"ETL pipeline failed with exception: ${e.getMessage}", Some(e))

          val failedMetrics = context.metrics
            .addError(s"Pipeline failed: ${e.getMessage}")
            .complete()

          context.updateMetrics(failedMetrics)

          // Calculate pipeline duration even for failures
          val pipelineDuration = (System.nanoTime() - startTime) / 1e9

          // Track active pipelines (decrement)
          ETLMetrics.activePipelines.dec(labels = Map("mode" -> pipelineMode))

          // Update Prometheus metrics for exception
          ETLMetrics.pipelineExecutionsTotal.inc(labels = pipelineLabels ++ Map("status" -> "exception"))
          ETLMetrics.pipelineDurationSeconds.observe(pipelineDuration, pipelineLabels ++ Map("status" -> "exception"))
          ETLMetrics.recordsFailedTotal.inc(
            context.metrics.recordsExtracted.toDouble,
            pipelineLabels ++ Map("stage" -> "pipeline", "reason" -> e.getClass.getSimpleName)
          )

          PipelineFailure(failedMetrics, e)
      }
    }
  }

  /**
   * Run data quality validation on a DataFrame.
   *
   * @param df DataFrame to validate
   * @param context Execution context
   * @param stage Stage name for logging (extract, transform)
   */
  private def runDataQualityValidation(
    df: DataFrame,
    context: ExecutionContext,
    stage: String
  ): Unit = {
    try {
      val config = context.config.dataQualityConfig

      // Create rules from configuration
      val rules = DataQualityRuleFactory.createRules(config.rules)

      if (rules.isEmpty) {
        logger.info(s"No data quality rules configured for $stage stage, skipping validation")
        return
      }

      logger.info(s"Executing ${rules.size} data quality rule(s) for $stage stage")

      // Determine onFailure action
      val onFailure = OnFailureAction.fromString(config.onFailure)

      // Execute validation
      val report = DataQualityValidator.validate(df, rules, onFailure)

      // Log report
      logger.info(s"Data quality validation completed for $stage stage: ${report.summary}")

      // Update Prometheus metrics for data quality checks
      val pipelineLabels = Map("pipeline_id" -> context.config.pipelineId)

      report.results.foreach { result =>
        val checkLabels = pipelineLabels ++ Map(
          "rule_type" -> result.rule.ruleType.toString,
          "severity" -> result.rule.severity.toString,
          "status" -> (if (result.passed) "passed" else "failed")
        )

        ETLMetrics.dataQualityChecksTotal.inc(labels = checkLabels)

        if (!result.passed) {
          val violationLabels = pipelineLabels ++ Map(
            "rule_name" -> result.rule.name,
            "severity" -> result.rule.severity.toString
          )
          ETLMetrics.dataQualityViolationsTotal.inc(labels = violationLabels)
        }
      }

      // Log failed rules
      report.failures.foreach { result =>
        logger.warn(s"Failed rule: ${result.summaryMessage}")
      }

    } catch {
      case ex: Exception =>
        logger.error(s"Data quality validation failed for $stage stage: ${ex.getMessage}", ex)
        // Re-throw to fail the pipeline
        throw ex
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
