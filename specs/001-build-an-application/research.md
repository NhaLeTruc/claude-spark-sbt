# Research: Spark-Based ETL Pipeline Framework

**Feature**: 001-build-an-application
**Date**: 2025-10-06
**Phase**: 0 - Technical Research

## Research Areas

### 1. Spark 3.5.6 + Scala 2.12 Compatibility

**Decision**: Use Apache Spark 3.5.6 with Scala 2.12.18

**Rationale**:
- Spark 3.5.6 officially supports Scala 2.12 and 2.13; 2.12 chosen for broader ecosystem compatibility
- Java 11 is LTS and fully supported by Spark 3.5.x
- Version alignment ensures stable binary compatibility

**Alternatives Considered**:
- Scala 2.13: More modern but narrower library ecosystem support
- Spark 3.4.x: Previous stable, but missing performance improvements in 3.5.6

**Dependencies**:
```scala
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.6",
  "org.apache.spark" %% "spark-sql" % "3.5.6",
  "org.apache.spark" %% "spark-streaming" % "3.5.6",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.6",
  "org.apache.avro" % "avro" % "1.11.3",
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)
```

### 2. Strategy Pattern Implementation in Scala

**Decision**: Use Scala traits for Strategy interfaces, concrete case classes/objects for implementations

**Rationale**:
- Traits provide interface abstraction with optional default implementations
- Case classes enable immutability and structural equality for configurations
- Pattern matching on sealed traits ensures exhaustive handling

**Pattern Design**:
```scala
// Strategy interface
trait Pipeline {
  def run(context: ExecutionContext): PipelineResult
}

trait Extractor {
  def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame
}

trait Transformer {
  def transform(df: DataFrame, config: TransformConfig): DataFrame
}

trait Loader {
  def load(df: DataFrame, config: LoadConfig, mode: WriteMode): LoadResult
}

// Concrete implementations
class KafkaExtractor extends Extractor { ... }
class PostgreSQLLoader extends Loader { ... }
```

**Alternatives Considered**:
- Abstract classes: Less flexible for multiple inheritance scenarios
- Higher-order functions only: Loses type safety and discoverability

### 3. Functional Programming for Transformations

**Decision**: Implement transformations as pure functions within companion objects/modules

**Rationale**:
- Pure functions: `DataFrame => DataFrame` signatures ensure referential transparency
- Composability: Functions can be chained via `.transform()` method
- Testability: Easy to mock inputs/outputs without side effects

**Functional Design**:
```scala
object Transformations {
  // Pure function: aggregation
  def aggregateByKey(
    groupCols: Seq[String],
    aggExprs: Map[String, String]
  ): DataFrame => DataFrame = df =>
    df.groupBy(groupCols.map(col): _*)
      .agg(aggExprs.map { case (col, func) =>
        expr(s"$func($col) as ${col}_$func")
      }.toSeq: _*)

  // Pure function: windowing
  def window(
    timecol: String,
    windowDuration: String,
    slideDuration: String
  ): DataFrame => DataFrame = df =>
    df.groupBy(window(col(timecol), windowDuration, slideDuration))
      .count()
}

// Usage (function composition)
val pipeline =
  aggregateByKey(Seq("user_id"), Map("amount" -> "sum"))
    .andThen(filterInvalid)
```

**Alternatives Considered**:
- OOP transformers with mutable state: Breaks functional paradigm, harder to test
- Spark SQL strings only: Less type-safe, harder to compose

### 4. Avro Format and Schema Management

**Decision**: Use Apache Avro 1.11.3 with JSON schema definitions, schema registry pattern

**Rationale**:
- Avro provides compact binary serialization with schema evolution support
- JSON `.avsc` schemas are human-readable and version-controllable
- Spark has native Avro support via `spark-avro` module
- Schema validation prevents data corruption at pipeline boundaries

**Implementation Approach**:
```scala
// SchemaRegistry pattern
object SchemaRegistry {
  private val schemas: Map[String, Schema] = loadSchemasFromResources()

  def getSchema(name: String): Schema = schemas(name)

  def validate(df: DataFrame, schemaName: String): Boolean = {
    val schema = getSchema(schemaName)
    SchemaValidator.validate(df, schema)
  }
}

// Read/Write with Avro
df.write.format("avro").save(path)
spark.read.format("avro").load(path)
```

**Alternatives Considered**:
- Parquet: Good for storage but less flexible for inter-stage serialization
- JSON: Human-readable but larger size and slower parsing
- Protobuf: Requires code generation, more complex schema evolution

### 5. Configuration Management with JSON

**Decision**: Use Lightbend Config (Typesafe Config) for application settings, custom JSON parser for pipeline configs

**Rationale**:
- Lightbend Config provides HOCON format (JSON superset) with defaults and overrides
- Custom pipeline configs allow dynamic pipeline composition without recompilation
- Separates application config (logging, Spark settings) from pipeline config (sources, transforms)

**Config Structure**:
```json
// Pipeline config: configs/pipeline.json
{
  "pipelineId": "kafka-to-postgres",
  "extract": {
    "type": "kafka",
    "topics": ["user-events"],
    "bootstrap.servers": "${KAFKA_BROKERS}",
    "schema": "user-event"
  },
  "transforms": [
    {
      "type": "aggregation",
      "groupBy": ["user_id"],
      "aggregations": {"amount": "sum"}
    }
  ],
  "load": {
    "type": "postgresql",
    "table": "user_summary",
    "writeMode": "upsert",
    "upsertKeys": ["user_id"]
  },
  "retry": {
    "maxAttempts": 3,
    "delaySeconds": 5
  }
}
```

**Alternatives Considered**:
- YAML: Less JSON-compatible, requires additional parser
- HOCON only: Mixing app and pipeline config reduces clarity
- Scala case classes: Requires recompilation for config changes

### 6. Local Credential Vault

**Decision**: File-based encrypted vault with AES-256, accessed via CredentialVault API

**Rationale**:
- Testable locally without external dependencies (no Vault/AWS Secrets Manager)
- Encrypted at rest with master key from environment variable
- Simple key-value store: `credentialId -> encryptedValue`
- Production can swap implementation for cloud vault

**Vault Design**:
```scala
trait CredentialVault {
  def getCredential(id: String): String
}

class FileBasedVault(vaultPath: String, masterKey: String) extends CredentialVault {
  private val decryptedCache: Map[String, String] = loadAndDecrypt(vaultPath, masterKey)

  def getCredential(id: String): String =
    decryptedCache.getOrElse(id, throw new CredentialNotFoundException(id))
}

// vault.enc (encrypted JSON)
// {
//   "postgres.password": "encrypted_base64_string",
//   "kafka.sasl.password": "encrypted_base64_string"
// }
```

**Alternatives Considered**:
- Plain text: Insecure, fails security review
- External vault service: Adds deployment complexity, not locally testable
- Java Keystore: More complex API, harder to version control encrypted values

### 7. Mocking Strategy for Tests

**Decision**: Use ScalaTest with custom mock implementations, no real external connections

**Rationale**:
- ScalaTest FlatSpec/FunSuite for test structure, Matchers for assertions
- Custom mock implementations of Extractor/Loader traits using in-memory data
- Spark local mode (`master("local[*]")`) for DataFrame operations without cluster
- Deterministic test data in code/resources, no network calls

**Mock Pattern**:
```scala
class MockKafkaExtractor(testData: Seq[Row], schema: StructType) extends Extractor {
  def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(testData.asJava, schema)
  }
}

class ExtractorSpec extends AnyFlatSpec with Matchers {
  "KafkaExtractor" should "extract data matching schema" in {
    implicit val spark = SparkSession.builder().master("local[*]").getOrCreate()
    val extractor = new MockKafkaExtractor(testData, testSchema)
    val df = extractor.extract(mockConfig)
    df.count() shouldBe 100
    df.schema shouldBe testSchema
  }
}
```

**Alternatives Considered**:
- Testcontainers: Requires Docker, slower tests, more complex setup
- Embedded databases: Still requires I/O, harder to control test data
- Mockito/ScalaMock: Overly complex for simple data mocking

### 8. Retry Logic with Exponential Backoff

**Decision**: Implement retry wrapper with fixed 5-second delay (3 attempts) using Scala Try/Either

**Rationale**:
- Simple exponential backoff: 5s, 10s, 20s (or fixed 5s per requirements)
- `Either[Throwable, A]` for error handling without exceptions
- Configurable per pipeline via JSON config
- Logs each retry attempt for observability

**Retry Implementation**:
```scala
object Retry {
  def withRetry[A](
    maxAttempts: Int,
    delaySeconds: Int
  )(operation: => A): Either[Throwable, A] = {
    @tailrec
    def attempt(remainingAttempts: Int, lastError: Option[Throwable]): Either[Throwable, A] = {
      Try(operation) match {
        case Success(result) => Right(result)
        case Failure(error) if remainingAttempts > 0 =>
          logger.warn(s"Attempt failed, retrying in ${delaySeconds}s", error)
          Thread.sleep(delaySeconds * 1000)
          attempt(remainingAttempts - 1, Some(error))
        case Failure(error) => Left(error)
      }
    }
    attempt(maxAttempts, None)
  }
}
```

**Alternatives Considered**:
- Akka retry: Adds heavy dependency for simple use case
- cats-retry library: Good but adds functional library dependency
- Custom Future-based: Overly complex for synchronous pipeline execution

### 9. Structured Logging with JSON

**Decision**: Use SLF4J API with Logback backend, JSON encoder for structured logs

**Rationale**:
- SLF4J is Spark's logging facade, ensures compatibility
- Logback JSON encoder (logstash-logback-encoder) for machine-readable logs
- MDC (Mapped Diagnostic Context) for trace IDs and contextual fields
- Log all metrics: record counts, timings, errors, retries

**Logging Setup**:
```xml
<!-- logback.xml -->
<configuration>
  <appender name="JSON" class="ch.qos.logback.core.ConsoleAppender">
    <encoder class="net.logstash.logback.encoder.LoggingEventCompositeJsonEncoder">
      <providers>
        <timestamp/>
        <logLevel/>
        <message/>
        <mdc/>
        <stackTrace/>
      </providers>
    </encoder>
  </appender>
  <root level="INFO">
    <appender-ref ref="JSON"/>
  </root>
</configuration>
```

```scala
// Usage with MDC
import org.slf4j.{Logger, LoggerFactory, MDC}

val logger = LoggerFactory.getLogger(getClass)
MDC.put("pipelineId", pipeline.id)
MDC.put("traceId", UUID.randomUUID().toString)
logger.info("Pipeline started", Map("recordCount" -> df.count))
```

**Alternatives Considered**:
- Plain text logs: Harder to parse for metrics/alerting
- Log4j2: Similar capabilities but Logback is Spark default
- Custom JSON serialization: Reinventing the wheel

### 10. Performance Optimization Strategies

**Decision**: Leverage Spark's built-in optimizations, tune parallelism and partitioning

**Rationale**:
- Spark Catalyst optimizer handles query optimization automatically
- Partitioning strategy: match source partitions (Kafka partitions, S3 file splits)
- Broadcast joins for small dimension tables
- Caching intermediate DataFrames only when reused multiple times
- Tune `spark.sql.shuffle.partitions` based on data volume

**Optimization Checklist**:
- Avoid `collect()` or `count()` in hot paths (use sampling for validation)
- Use Avro compression (Snappy) for inter-stage data
- Minimize shuffles: prefer narrow transformations (map, filter) over wide (groupBy, join)
- Monitor Spark UI for stage timing and skew
- Performance tests validate 100K/10K rec/s thresholds

**Alternatives Considered**:
- Manual RDD operations: Lower-level but harder to optimize and maintain
- Spark SQL only: Less type-safe than DataFrame API
- Custom partitioning logic: Spark's default is usually sufficient

## Summary

All technical unknowns resolved. Stack: Spark 3.5.6 + Scala 2.12.18 + SBT 1.9.x + ScalaTest 3.2.x + Avro 1.11.3. Design patterns validated: Strategy (traits), Functional (pure functions), Library-first (modular packages). Infrastructure choices: JSON configs, file-based vault, Logback JSON logging, local mocking for tests. Performance strategy relies on Spark optimizations with tuned partitioning. Ready for Phase 1 design.
