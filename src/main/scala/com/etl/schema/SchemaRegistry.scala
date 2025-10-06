package com.etl.schema

import org.apache.avro.Schema
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * Schema registry for loading and managing Avro schemas.
 * Loads all .avsc files from resources/schemas/ directory.
 */
object SchemaRegistry {
  private val logger = LoggerFactory.getLogger(getClass)
  private val SCHEMAS_PATH = "schemas"

  /**
   * Map of schema name to parsed Avro Schema.
   * Loaded lazily on first access.
   */
  private lazy val schemas: Map[String, Schema] = loadSchemas()

  /**
   * Get Avro schema by name.
   *
   * @param name Schema name (e.g., "user-event", "transaction")
   * @return Avro Schema object
   * @throws IllegalArgumentException if schema not found
   */
  def getSchema(name: String): Schema = {
    schemas.getOrElse(
      name,
      throw new IllegalArgumentException(s"Schema not found: $name. Available: ${listSchemas().mkString(", ")}")
    )
  }

  /**
   * List all available schema names.
   *
   * @return Set of schema names
   */
  def listSchemas(): Set[String] = schemas.keySet

  /**
   * Check if schema exists.
   *
   * @param name Schema name
   * @return true if schema is registered
   */
  def hasSchema(name: String): Boolean = schemas.contains(name)

  /**
   * Load all .avsc files from resources/schemas/ directory.
   *
   * @return Map of schema name to parsed Schema
   */
  private def loadSchemas(): Map[String, Schema] = {
    val classLoader = getClass.getClassLoader

    // List of known schema files
    val schemaFiles = Seq("user-event.avsc", "transaction.avsc", "user-summary.avsc")

    val loadedSchemas = schemaFiles.flatMap { filename =>
      val resourcePath = s"$SCHEMAS_PATH/$filename"
      val schemaName = filename.stripSuffix(".avsc")

      Try {
        val inputStream = classLoader.getResourceAsStream(resourcePath)
        if (inputStream == null) {
          throw new RuntimeException(s"Schema file not found: $resourcePath")
        }

        val schemaJson = Source.fromInputStream(inputStream).mkString
        inputStream.close()

        val parser = new Schema.Parser()
        val schema = parser.parse(schemaJson)

        logger.info(s"Loaded Avro schema: $schemaName (${schema.getName})")
        Some(schemaName -> schema)
      } match {
        case Success(result) => result
        case Failure(exception) =>
          logger.error(s"Failed to load schema $schemaName: ${exception.getMessage}")
          None
      }
    }

    val schemaMap = loadedSchemas.toMap
    logger.info(s"SchemaRegistry initialized with ${schemaMap.size} schemas: ${schemaMap.keys.mkString(", ")}")
    schemaMap
  }

  /**
   * Get schema field names for validation.
   *
   * @param name Schema name
   * @return Set of field names in the schema
   */
  def getFieldNames(name: String): Set[String] = {
    val schema = getSchema(name)
    schema.getFields.asScala.map(_.name()).toSet
  }

  /**
   * Get schema namespace and name as fully qualified name.
   *
   * @param name Schema registry name
   * @return Fully qualified schema name (namespace.name)
   */
  def getFullyQualifiedName(name: String): String = {
    val schema = getSchema(name)
    s"${schema.getNamespace}.${schema.getName}"
  }
}
