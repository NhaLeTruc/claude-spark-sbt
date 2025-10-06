# Spark-Based ETL Pipeline Framework

Production-grade Extract-Transform-Load framework built with Apache Spark 3.5.6, Scala 2.12.18, and Java 11.

## Project Overview

This framework enables data engineers to compose, test, and deploy data pipelines for ETL operations across:
- **Sources/Sinks**: Kafka, PostgreSQL, MySQL, Amazon S3
- **Transformations**: Aggregations, Joins, Windowing (functional programming paradigm)
- **Execution Models**: Batch (Spark) and Streaming (Spark Streaming)
- **Data Format**: Avro serialization with JSON schema validation
- **Fault Tolerance**: Automatic retry logic (3 attempts, 5s delay)

## Design Principles

- **Strategy Pattern**: Extensible interfaces for Pipeline, Extractor, Transformer, Loader
- **SOLID Architecture**: Single responsibility, open/closed, Liskov substitution, interface segregation, dependency inversion
- **Functional Programming**: Pure functions for transformations, immutable data structures
- **Test-First Development**: TDD with unit, integration, contract, and performance tests (85%+ coverage target)
- **Observability**: Structured JSON logging with SLF4J, metrics at every stage

## Prerequisites

- **Java**: 11 (LTS)
- **Scala**: 2.12.18
- **SBT**: 1.9.x
- **Spark**: 3.5.6 (provided at runtime or via spark-submit)

## Project Structure

```
claude-spark-sbt/
├── build.sbt                      # SBT project definition
├── project/
│   ├── build.properties           # SBT version
│   └── plugins.sbt                # SBT plugins (assembly, scalafmt, scalastyle, scoverage)
├── src/
│   ├── main/
│   │   ├── scala/com/etl/
│   │   │   ├── core/              # Pipeline trait, execution context
│   │   │   ├── extract/           # Extractor trait + implementations (Kafka, PostgreSQL, MySQL, S3)
│   │   │   ├── transform/         # Transformer trait + implementations (Aggregation, Join, Windowing)
│   │   │   ├── load/              # Loader trait + implementations (Kafka, PostgreSQL, MySQL, S3)
│   │   │   ├── schema/            # SchemaRegistry, SchemaValidator
│   │   │   ├── config/            # ConfigLoader, CredentialVault, PipelineConfig
│   │   │   ├── model/             # WriteMode, PipelineState, ExecutionMetrics, PipelineResult
│   │   │   └── util/              # Retry, Logging utilities
│   │   └── resources/
│   │       ├── schemas/           # Avro JSON schemas (.avsc)
│   │       ├── configs/           # Pipeline JSON configs
│   │       └── logback.xml        # Logging configuration (JSON output)
│   └── test/
│       └── scala/
│           ├── unit/              # Unit tests for all components
│           ├── integration/       # End-to-end pipeline tests
│           ├── contract/          # Schema validation tests
│           └── performance/       # Throughput and latency tests
├── .scalafmt.conf                 # Scalafmt configuration
├── scalastyle-config.xml          # ScalaStyle linting rules
└── specs/001-build-an-application/
    ├── plan.md                    # Implementation plan
    ├── tasks.md                   # Detailed task breakdown (73 tasks)
    ├── data-model.md              # Entity definitions
    ├── research.md                # Technical decisions
    ├── quickstart.md              # Integration test scenarios
    └── contracts/                 # Avro and JSON schemas
```

## Build & Test

```bash
# Compile
sbt compile

# Run all tests
sbt test

# Run specific test suite
sbt "testOnly com.etl.unit.model.*"

# Code coverage
sbt clean coverage test coverageReport
# Report: target/scala-2.12/scoverage-report/index.html

# Format code
sbt scalafmt

# Check code style
sbt scalastyle

# Create assembly JAR for spark-submit
sbt assembly
# Output: target/scala-2.12/claude-spark-etl-1.0.0.jar
```

## Implementation Status

### ✅ Completed (9 tasks)

**Phase 3.1: Setup**
- [x] T001: SBT project structure (build.sbt, plugins)
- [x] T002: Directory structure (src/main/scala, src/test/scala, resources)
- [x] T003: Scalafmt and ScalaStyle configuration
- [x] T004: Structured logging (logback.xml with JSON encoder)

**Phase 3.3: Core Data Models**
- [x] T009: WriteMode ADT (Append, Overwrite, Upsert)
- [x] T010: PipelineState ADT (Created, Running, Retrying, Success, Failed)
- [x] T011: ExecutionMetrics case class with helper methods
- [x] T012: PipelineResult trait (PipelineSuccess, PipelineFailure)
- [x] T013: Configuration case classes (PipelineConfig, ExtractConfig, TransformConfig, LoadConfig, SourceType, SinkType)

### 🚧 In Progress

The foundation is complete. Next steps:
1. **Phase 3.2**: Contract tests for Avro schemas (T005-T008)
2. **Phase 3.4**: Schema management (SchemaRegistry, SchemaValidator) (T014-T018)
3. **Phase 3.5**: Core traits (Pipeline, Extractor, Transformer, Loader) (T019-T022)
4. **Phase 3.6**: Utilities (Retry, Logging) (T023-T026)
5. **Phase 3.7**: Configuration management (ConfigLoader, CredentialVault) (T027-T030)
6. **Phases 3.8-3.10**: Strategy implementations (Extractors, Transformers, Loaders)
7. **Phases 3.11-3.12**: Core execution (PipelineExecutor, Main)
8. **Phases 3.13-3.15**: Testing and documentation

### 📊 Progress: 9/73 tasks (12.3%)

## Architecture

### Strategy Pattern

All extractors, transformers, and loaders implement common traits:

```scala
trait Extractor {
  def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame
}

trait Transformer {
  def transform(df: DataFrame, config: TransformConfig): DataFrame
}

trait Loader {
  def load(df: DataFrame, config: LoadConfig, mode: WriteMode): LoadResult
}
```

This allows pipelines to be composed dynamically from configuration.

### Functional Transformations

Transformations are pure functions: `DataFrame => DataFrame`

```scala
// Aggregation example
def aggregate(groupBy: Seq[String], aggs: Map[String, String]): DataFrame => DataFrame =
  df => df.groupBy(groupBy.map(col): _*).agg(...)
```

### Pipeline Execution

1. **Extract**: Read from source (Kafka, PostgreSQL, MySQL, S3)
2. **Validate**: Schema conformance check (Avro)
3. **Transform**: Apply functional transformations (chain via `.transform()`)
4. **Validate**: Output schema check
5. **Load**: Write to sink with retry logic
6. **Metrics**: Log telemetry (record counts, timings, errors)

## Performance Targets

- **Batch Processing**:
  - Simple transforms (filter, map): ≥100K records/second
  - Complex transforms (join, aggregate): ≥10K records/second
- **Streaming Processing**:
  - Sustained throughput: ≥50K events/second
  - p95 latency: <5 seconds
- **Resources**:
  - Memory: ≤80% peak utilization
  - CPU: ≤60% average utilization

## Configuration Example

Pipeline config (JSON):

```json
{
  "pipelineId": "kafka-to-postgres",
  "name": "User Event Aggregation",
  "extract": {
    "type": "kafka",
    "topic": "user-events",
    "schemaName": "user-event",
    "connectionParams": {
      "bootstrap.servers": "localhost:9092"
    }
  },
  "transforms": [
    {
      "type": "aggregation",
      "parameters": {
        "groupBy": ["user_id"],
        "aggregations": {"event_id": "count", "amount": "sum"}
      }
    }
  ],
  "load": {
    "type": "postgresql",
    "table": "user_summary",
    "writeMode": "upsert",
    "upsertKeys": ["user_id"],
    "schemaName": "user-summary",
    "credentialId": "postgres.password"
  },
  "retry": {
    "maxAttempts": 3,
    "delaySeconds": 5
  }
}
```

## Usage (When Complete)

```bash
# Local testing
sbt "runMain com.etl.Main --config configs/pipeline.json --mode batch"

# Spark cluster deployment
spark-submit \
  --class com.etl.Main \
  --master spark://cluster:7077 \
  --deploy-mode cluster \
  target/scala-2.12/claude-spark-etl-1.0.0.jar \
  --config configs/pipeline.json \
  --mode batch
```

## Testing Strategy

- **Unit Tests**: Isolated component testing with mocked dependencies (ScalaTest)
- **Integration Tests**: End-to-end pipeline execution with mocked external systems
- **Contract Tests**: Avro schema validation for all data contracts
- **Performance Tests**: Throughput and latency validation against targets

## Development Workflow

1. **Test-First**: Write failing tests before implementation (TDD)
2. **Implement**: Make tests pass
3. **Refactor**: Clean up code while maintaining tests
4. **Format**: Run `sbt scalafmt`
5. **Lint**: Run `sbt scalastyle`
6. **Coverage**: Run `sbt clean coverage test coverageReport`
7. **Commit**: Commit with descriptive message

## Contributing

Follow the task plan in [specs/001-build-an-application/tasks.md](specs/001-build-an-application/tasks.md) for implementation order and dependencies.

## License

Proprietary - Internal use only

## Contact

For questions or issues, refer to the design documents in `specs/001-build-an-application/`.
