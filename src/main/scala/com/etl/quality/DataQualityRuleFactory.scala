package com.etl.quality

import com.etl.config.{DataQualityRuleConfig, DataQualityRuleType}
import org.slf4j.LoggerFactory

/**
 * Factory for creating data quality rules from configuration.
 *
 * Converts DataQualityRuleConfig objects into concrete DataQualityRule instances.
 */
object DataQualityRuleFactory {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Create a data quality rule from configuration.
   *
   * @param config Rule configuration
   * @return Concrete DataQualityRule instance
   * @throws IllegalArgumentException if configuration is invalid
   */
  def createRule(config: DataQualityRuleConfig): DataQualityRule = {
    logger.debug(s"Creating rule: ${config.ruleType} for columns: ${config.columns.mkString(", ")}")

    val severity = Severity.fromString(config.severity)
    val ruleName = config.name.getOrElse(defaultRuleName(config))

    config.ruleType match {
      case DataQualityRuleType.NotNull =>
        new NotNullRule(config.columns, severity, ruleName)

      case DataQualityRuleType.Range =>
        createRangeRule(config, severity, ruleName)

      case DataQualityRuleType.Unique =>
        new UniqueRule(config.columns, severity, ruleName)
    }
  }

  /**
   * Create multiple rules from a list of configurations.
   */
  def createRules(configs: Seq[DataQualityRuleConfig]): Seq[DataQualityRule] = {
    configs.map(createRule)
  }

  /**
   * Create a RangeRule from configuration.
   * Requires 'min' and 'max' parameters.
   */
  private def createRangeRule(
    config: DataQualityRuleConfig,
    severity: Severity,
    ruleName: String
  ): RangeRule = {
    require(
      config.columns.size == 1,
      s"RangeRule requires exactly one column, got: ${config.columns.size}"
    )

    val column = config.columns.head

    // Extract min and max from parameters
    val min = getRequiredDoubleParam(config.parameters, "min", ruleName)
    val max = getRequiredDoubleParam(config.parameters, "max", ruleName)

    new RangeRule(column, min, max, severity, ruleName)
  }

  /**
   * Get a required double parameter from the configuration.
   */
  private def getRequiredDoubleParam(
    parameters: Map[String, Any],
    paramName: String,
    ruleName: String
  ): Double = {
    parameters.get(paramName) match {
      case Some(value: Number) =>
        value.doubleValue()

      case Some(value: String) =>
        try {
          value.toDouble
        } catch {
          case ex: NumberFormatException =>
            throw new IllegalArgumentException(
              s"Rule '$ruleName': Parameter '$paramName' must be a number, got: $value"
            )
        }

      case Some(other) =>
        throw new IllegalArgumentException(
          s"Rule '$ruleName': Parameter '$paramName' must be a number, got: ${other.getClass.getSimpleName}"
        )

      case None =>
        throw new IllegalArgumentException(
          s"Rule '$ruleName': Required parameter '$paramName' is missing"
        )
    }
  }

  /**
   * Generate a default rule name based on configuration.
   */
  private def defaultRuleName(config: DataQualityRuleConfig): String = {
    val columnsPart = if (config.columns.size == 1) {
      config.columns.head
    } else {
      config.columns.mkString(",")
    }

    s"${config.ruleType}($columnsPart)"
  }
}
