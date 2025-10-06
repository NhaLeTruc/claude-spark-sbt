package com.etl.schema

import org.apache.avro.Schema
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * Result of schema validation.
 *
 * @param isValid Whether validation passed
 * @param errors List of validation error messages
 */
case class ValidationResult(isValid: Boolean, errors: Seq[String]) {
  def addError(error: String): ValidationResult =
    ValidationResult(isValid = false, errors = errors :+ error)
}

object ValidationResult {
  def valid: ValidationResult = ValidationResult(isValid = true, errors = Seq.empty)
  def invalid(errors: String*): ValidationResult =
    ValidationResult(isValid = false, errors = errors)
}

/**
 * Validates Spark DataFrames against Avro schemas.
 */
object SchemaValidator {
  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Validate DataFrame schema against registered Avro schema.
   *
   * @param df DataFrame to validate
   * @param schemaName Name of Avro schema in registry
   * @return ValidationResult with errors if validation fails
   */
  def validate(df: DataFrame, schemaName: String): ValidationResult = {
    val avroSchema = SchemaRegistry.getSchema(schemaName)
    validateAgainstAvroSchema(df, avroSchema, schemaName)
  }

  /**
   * Validate DataFrame against Avro schema.
   *
   * @param df DataFrame to validate
   * @param avroSchema Avro schema to validate against
   * @param schemaName Schema name for error messages
   * @return ValidationResult
   */
  private def validateAgainstAvroSchema(
    df: DataFrame,
    avroSchema: Schema,
    schemaName: String
  ): ValidationResult = {
    val sparkSchema = df.schema
    var result = ValidationResult.valid

    // Get Avro fields
    val avroFields = avroSchema.getFields.asScala

    // Check each Avro field exists in Spark schema with correct type
    avroFields.foreach { avroField =>
      val fieldName = avroField.name()
      val sparkField = sparkSchema.fields.find(_.name == fieldName)

      sparkField match {
        case Some(field) =>
          // Field exists, check type compatibility
          val typeError = checkTypeCompatibility(field.dataType, avroField.schema(), fieldName)
          if (typeError.nonEmpty) {
            result = result.addError(typeError.get)
          }

        case None =>
          // Field missing - check if it's nullable in Avro
          if (!isNullable(avroField.schema())) {
            result = result.addError(s"Required field missing: $fieldName")
          }
      }
    }

    if (result.isValid) {
      logger.debug(s"Schema validation passed for $schemaName")
    } else {
      logger.warn(s"Schema validation failed for $schemaName: ${result.errors.mkString("; ")}")
    }

    result
  }

  /**
   * Check if Avro schema allows null values.
   *
   * @param avroSchema Avro schema
   * @return true if schema is nullable (union with null)
   */
  private def isNullable(avroSchema: Schema): Boolean = {
    if (avroSchema.getType == Schema.Type.UNION) {
      avroSchema.getTypes.asScala.exists(_.getType == Schema.Type.NULL)
    } else {
      false
    }
  }

  /**
   * Check type compatibility between Spark type and Avro type.
   *
   * @param sparkType Spark SQL data type
   * @param avroSchema Avro schema
   * @param fieldName Field name for error messages
   * @return Some(error message) if incompatible, None if compatible
   */
  private def checkTypeCompatibility(
    sparkType: DataType,
    avroSchema: Schema,
    fieldName: String
  ): Option[String] = {
    val avroType = if (avroSchema.getType == Schema.Type.UNION) {
      // For union types, get the non-null type
      avroSchema.getTypes.asScala.find(_.getType != Schema.Type.NULL).getOrElse(avroSchema)
    } else {
      avroSchema
    }

    val compatible = (sparkType, avroType.getType) match {
      case (StringType, Schema.Type.STRING)           => true
      case (IntegerType, Schema.Type.INT)             => true
      case (LongType, Schema.Type.LONG)               => true
      case (FloatType, Schema.Type.FLOAT)             => true
      case (DoubleType, Schema.Type.DOUBLE)           => true
      case (BooleanType, Schema.Type.BOOLEAN)         => true
      case (BinaryType, Schema.Type.BYTES)            => true
      case (MapType(StringType, _, _), Schema.Type.MAP) => true
      case (ArrayType(_, _), Schema.Type.ARRAY)       => true
      case (StructType(_), Schema.Type.RECORD)        => true
      case (StringType, Schema.Type.ENUM)             => true // Enum as string
      case _                                          => false
    }

    if (!compatible) {
      Some(s"Type mismatch for field $fieldName: expected ${avroType.getType}, got $sparkType")
    } else {
      None
    }
  }

  /**
   * Validate and log validation result.
   *
   * @param df DataFrame to validate
   * @param schemaName Schema name
   * @return true if valid, false otherwise
   * @throws IllegalStateException if validation fails
   */
  def validateOrThrow(df: DataFrame, schemaName: String): Unit = {
    val result = validate(df, schemaName)
    if (!result.isValid) {
      val errorMsg = s"Schema validation failed for $schemaName: ${result.errors.mkString("; ")}"
      throw new IllegalStateException(errorMsg)
    }
  }
}
