# Tasks: Spark-Based ETL Pipeline Framework

**Input**: Design documents from `/home/bob/WORK/claude-spark-sbt/specs/001-build-an-application/`
**Prerequisites**: plan.md, research.md, data-model.md, contracts/, quickstart.md

## Format: `[ID] [P?] Description`
- **[P]**: Can run in parallel (different files, no dependencies)
- Include exact file paths in descriptions

## Path Conventions
- **Single Scala project**: `src/main/scala/`, `src/test/scala/` at repository root
- All paths relative to repository root: `/home/bob/WORK/claude-spark-sbt/`

---

## Phase 3.1: Setup (MUST COMPLETE FIRST)

- [ ] T001 Initialize SBT project structure with build.sbt defining Scala 2.12.18, Java 11, and library dependencies (Spark 3.5.6, Spark Streaming 3.5.6, Avro 1.11.3, ScalaTest 3.2.17)

- [ ] T002 Create project directory structure: src/main/scala/com/etl/{core,extract,transform,load,schema,config,model,util}/, src/main/resources/{schemas,configs}/, src/test/scala/{unit,integration,contract,performance}/

- [ ] T003 [P] Configure Scalafmt (scalafmt.conf) and ScalaStyle (scalastyle-config.xml) for code formatting and linting

- [ ] T004 [P] Configure logging: create src/main/resources/logback.xml with JSON encoder (logstash-logback-encoder) for structured logging

---

## Phase 3.2: Contract Tests (TDD - MUST FAIL BEFORE IMPLEMENTATION)

**CRITICAL: These tests MUST be written and MUST FAIL before ANY implementation**

- [ ] T005 [P] Contract test for user-event.avsc schema validation in src/test/scala/contract/schemas/UserEventSchemaSpec.scala - verify all fields (event_id, user_id, event_type, timestamp, properties, amount) match Avro schema definition

- [ ] T006 [P] Contract test for transaction.avsc schema validation in src/test/scala/contract/schemas/TransactionSchemaSpec.scala - verify all fields (transaction_id, user_id, amount, currency, transaction_time, status, metadata) match Avro schema definition

- [ ] T007 [P] Contract test for user-summary.avsc schema validation in src/test/scala/contract/schemas/UserSummarySchemaSpec.scala - verify aggregation output fields (user_id, total_events, total_amount, last_event_time, event_type_counts, updated_at) match schema

- [ ] T008 [P] Contract test for pipeline-config-schema.json validation in src/test/scala/contract/PipelineConfigSchemaSpec.scala - verify JSON config parsing against schema (pipelineId, extract, transforms, load, retry)

---

## Phase 3.3: Core Data Models (Foundation for all components)

- [ ] T009 [P] Create WriteMode ADT in src/main/scala/com/etl/model/WriteMode.scala with sealed trait and case objects (Append, Overwrite, Upsert)

- [ ] T010 [P] Create PipelineState ADT in src/main/scala/com/etl/model/PipelineState.scala with sealed trait and case objects (Created, Running, Retrying, Success, Failed)

- [ ] T011 [P] Create ExecutionMetrics case class in src/main/scala/com/etl/model/ExecutionMetrics.scala with fields (pipelineId, executionId, startTime, endTime, recordsExtracted, recordsTransformed, recordsLoaded, recordsFailed, retryCount, errors) and helper methods (duration, successRate)

- [ ] T012 [P] Create PipelineResult sealed trait in src/main/scala/com/etl/model/PipelineResult.scala with PipelineSuccess and PipelineFailure case classes

- [ ] T013 [P] Create configuration case classes in src/main/scala/com/etl/config/PipelineConfig.scala (PipelineConfig, RetryConfig, PerformanceConfig, LoggingConfig, ExtractConfig, TransformConfig, LoadConfig) with SourceType and SinkType enums

---

## Phase 3.4: Schema Management (Required for validation)

- [ ] T014 Copy Avro schema files to src/main/resources/schemas/: user-event.avsc, transaction.avsc, user-summary.avsc from contracts directory

- [ ] T015 Unit test for SchemaRegistry in src/test/scala/unit/schema/SchemaRegistrySpec.scala - test loading schemas from resources, getSchema method, schema not found error handling

- [ ] T016 Implement SchemaRegistry object in src/main/scala/com/etl/schema/SchemaRegistry.scala - load all .avsc files from resources/schemas/, provide getSchema(name: String) method returning Avro Schema

- [ ] T017 Unit test for SchemaValidator in src/test/scala/unit/schema/SchemaValidatorSpec.scala - test DataFrame schema validation against Avro schema, field type checking, nullable validation

- [ ] T018 Implement SchemaValidator in src/main/scala/com/etl/schema/SchemaValidator.scala - validate DataFrame against Avro schema, return validation errors with field-level details

---

## Phase 3.5: Core Traits/Interfaces (Strategy Pattern Foundation)

- [ ] T019 Define Pipeline trait in src/main/scala/com/etl/core/Pipeline.scala with run(context: ExecutionContext): PipelineResult method signature

- [ ] T020 Define Extractor trait in src/main/scala/com/etl/extract/Extractor.scala with extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame method signature

- [ ] T021 Define Transformer trait in src/main/scala/com/etl/transform/Transformer.scala with transform(df: DataFrame, config: TransformConfig): DataFrame method signature

- [ ] T022 Define Loader trait in src/main/scala/com/etl/load/Loader.scala with load(df: DataFrame, config: LoadConfig, mode: WriteMode): LoadResult method signature

---

## Phase 3.6: Utility Modules (Shared functionality)

- [ ] T023 Unit test for Retry utility in src/test/scala/unit/util/RetrySpec.scala - test retry logic with 3 attempts, 5 second delays, success after retry, failure after exhausted retries

- [ ] T024 Implement Retry utility in src/main/scala/com/etl/util/Retry.scala - withRetry function using tail recursion, Either[Throwable, A] return type, configurable maxAttempts and delaySeconds

- [ ] T025 [P] Unit test for Logging utility in src/test/scala/unit/util/LoggingSpec.scala - test structured logging with MDC context (pipelineId, traceId), JSON log output format

- [ ] T026 [P] Implement Logging trait in src/main/scala/com/etl/util/Logging.scala - SLF4J wrapper with MDC support, helper methods for structured logging (logInfo, logError with context maps)

---

## Phase 3.7: Configuration Management

- [ ] T027 Unit test for ConfigLoader in src/test/scala/unit/config/ConfigLoaderSpec.scala - test JSON pipeline config parsing, validation against pipeline-config-schema.json, error handling for invalid JSON

- [ ] T028 Implement ConfigLoader in src/main/scala/com/etl/config/ConfigLoader.scala - parse JSON config files using play-json or circe, validate against schema, return PipelineConfig case class

- [ ] T029 Unit test for CredentialVault in src/test/scala/unit/config/CredentialVaultSpec.scala - test file-based vault with AES-256 encryption, getCredential method, credential not found error, decryption with master key from env var

- [ ] T030 Implement CredentialVault trait and FileBasedVault in src/main/scala/com/etl/config/CredentialVault.scala - load encrypted vault.enc file, decrypt using master key from VAULT_MASTER_KEY env var, cache decrypted credentials

---

## Phase 3.8: Extractor Implementations (Strategy Pattern - ONLY AFTER TESTS)

- [ ] T031 [P] Unit test for KafkaExtractor in src/test/scala/unit/extract/KafkaExtractorSpec.scala - mock Kafka source, test extract method with topic/bootstrap.servers config, verify DataFrame schema matches user-event.avsc

- [ ] T032 [P] Unit test for PostgreSQLExtractor in src/test/scala/unit/extract/PostgreSQLExtractorSpec.scala - mock JDBC connection, test extract with SQL query, verify DataFrame output

- [ ] T033 [P] Unit test for MySQLExtractor in src/test/scala/unit/extract/MySQLExtractorSpec.scala - mock JDBC connection, test extract with table/query config, verify credentials from vault

- [ ] T034 [P] Unit test for S3Extractor in src/test/scala/unit/extract/S3ExtractorSpec.scala - mock S3 path, test CSV/Avro format extraction, verify schema validation

- [ ] T035 [P] Implement KafkaExtractor in src/main/scala/com/etl/extract/KafkaExtractor.scala extending Extractor trait - use spark.readStream.format("kafka"), configure bootstrap.servers and topics, deserialize Avro

- [ ] T036 [P] Implement PostgreSQLExtractor in src/main/scala/com/etl/extract/PostgreSQLExtractor.scala extending Extractor trait - use spark.read.jdbc with JDBC URL from config, fetch credentials from vault, execute query

- [ ] T037 [P] Implement MySQLExtractor in src/main/scala/com/etl/extract/MySQLExtractor.scala extending Extractor trait - use spark.read.jdbc with MySQL JDBC driver, handle connection params from config

- [ ] T038 [P] Implement S3Extractor in src/main/scala/com/etl/extract/S3Extractor.scala extending Extractor trait - use spark.read.format("avro"/"csv"), configure S3 path from config, validate schema post-extraction

---

## Phase 3.9: Transformer Implementations (Functional Programming)

- [ ] T039 [P] Unit test for AggregationTransform in src/test/scala/unit/transform/AggregationTransformSpec.scala - test groupBy with aggregate functions (count, sum, max), verify output schema

- [ ] T040 [P] Unit test for JoinTransform in src/test/scala/unit/transform/JoinTransformSpec.scala - test inner/left/right joins, verify join key matching, test with two mock DataFrames

- [ ] T041 [P] Unit test for WindowingTransform in src/test/scala/unit/transform/WindowingTransformSpec.scala - test time-based windowing (10s window, 5s slide), verify window aggregation output

- [ ] T042 [P] Implement AggregationTransform in src/main/scala/com/etl/transform/AggregationTransform.scala extending Transformer trait - pure function: df.groupBy(cols).agg(exprs), extract groupBy and aggregations from TransformConfig.parameters

- [ ] T043 [P] Implement JoinTransform in src/main/scala/com/etl/transform/JoinTransform.scala extending Transformer trait - pure function: df.join(right, keys, joinType), load right dataset from TransformConfig.parameters

- [ ] T044 [P] Implement WindowingTransform in src/main/scala/com/etl/transform/WindowingTransform.scala extending Transformer trait - pure function: df.groupBy(window(timeCol, duration, slide)).count(), extract window params from config

---

## Phase 3.10: Loader Implementations (Strategy Pattern - ONLY AFTER TESTS)

- [ ] T045 [P] Unit test for KafkaLoader in src/test/scala/unit/load/KafkaLoaderSpec.scala - mock Kafka sink, test load with append mode, verify Avro serialization, test topic from config

- [ ] T046 [P] Unit test for PostgreSQLLoader in src/test/scala/unit/load/PostgreSQLLoaderSpec.scala - mock JDBC connection, test append/overwrite/upsert modes, verify upsertKeys for upsert mode

- [ ] T047 [P] Unit test for MySQLLoader in src/test/scala/unit/load/MySQLLoaderSpec.scala - mock JDBC connection, test load with WriteMode, verify table name and credentials

- [ ] T048 [P] Unit test for S3Loader in src/test/scala/unit/load/S3LoaderSpec.scala - mock S3 path, test Avro output with Snappy compression, verify append/overwrite modes

- [ ] T049 [P] Implement KafkaLoader in src/main/scala/com/etl/load/KafkaLoader.scala extending Loader trait - use df.write.format("kafka"), serialize to Avro, configure topic and bootstrap.servers

- [ ] T050 [P] Implement PostgreSQLLoader in src/main/scala/com/etl/load/PostgreSQLLoader.scala extending Loader trait - use df.write.jdbc for append/overwrite, implement upsert with temp table + merge SQL, fetch credentials from vault

- [ ] T051 [P] Implement MySQLLoader in src/main/scala/com/etl/load/MySQLLoader.scala extending Loader trait - use df.write.jdbc with MySQL JDBC driver, handle WriteMode (append: SaveMode.Append, overwrite: SaveMode.Overwrite, upsert: custom logic)

- [ ] T052 [P] Implement S3Loader in src/main/scala/com/etl/load/S3Loader.scala extending Loader trait - use df.write.format("avro").option("compression", "snappy"), configure S3 path, handle append (partition append) vs overwrite modes

---

## Phase 3.11: Core Pipeline Execution

- [ ] T053 Unit test for ExecutionContext in src/test/scala/unit/core/ExecutionContextSpec.scala - test SparkSession initialization, test metrics tracking, test trace ID generation

- [ ] T054 Implement ExecutionContext case class in src/main/scala/com/etl/core/ExecutionContext.scala - wrap SparkSession, ExecutionMetrics, trace ID, provide helper methods for logging context

- [ ] T055 Unit test for PipelineExecutor in src/test/scala/unit/core/PipelineExecutorSpec.scala - test execute method with mock Pipeline, verify retry logic integration (3 attempts, 5s delay), test success and failure paths with metrics

- [ ] T056 Implement PipelineExecutor in src/main/scala/com/etl/core/PipelineExecutor.scala - execute(pipeline: Pipeline, config: PipelineConfig): PipelineResult method, integrate Retry utility, wrap execution in Try/Either, populate ExecutionMetrics, log all stages

- [ ] T057 Unit test for concrete Pipeline implementation in src/test/scala/unit/core/ETLPipelineSpec.scala - test run method with mock Extractor/Transformer/Loader, verify stage orchestration (extract → transform → load), verify schema validation between stages

- [ ] T058 Implement ETLPipeline case class in src/main/scala/com/etl/core/ETLPipeline.scala implementing Pipeline trait - compose extractor, transformers (Seq), loader, implement run method: extract → validate → transform (chain) → validate → load → validate, return PipelineResult with metrics

---

## Phase 3.12: Main Entry Point

- [ ] T059 Implement Main object in src/main/scala/com/etl/Main.scala - parse command-line args (--config, --mode batch|streaming), load PipelineConfig from JSON, initialize SparkSession, build Pipeline from config (factory pattern to map config types to implementations), execute via PipelineExecutor, log final result, exit with status code

- [ ] T060 Create example pipeline configs in src/main/resources/configs/: example-batch-pipeline.json (S3 → Aggregation → PostgreSQL), example-streaming-pipeline.json (Kafka → Windowing → S3) matching quickstart scenarios

---

## Phase 3.13: Integration Tests (End-to-End Scenarios)

- [ ] T061 [P] Integration test for Scenario 1 (Batch Pipeline) in src/test/scala/integration/pipelines/BatchPipelineIntegrationSpec.scala - mock S3 source with CSV test data, create pipeline with AggregationTransform, mock PostgreSQL sink, verify end-to-end execution, validate record counts match (extract = transform = load), verify upsert mode with upsertKeys

- [ ] T062 [P] Integration test for Scenario 2 (Streaming Pipeline) in src/test/scala/integration/pipelines/StreamingPipelineIntegrationSpec.scala - mock Kafka stream source, create pipeline with WindowingTransform (10s window, 5s slide), mock S3 sink with Avro output, verify micro-batch processing, validate p95 latency <5s

- [ ] T063 [P] Integration test for Scenario 3 (Join Pipeline) in src/test/scala/integration/pipelines/JoinPipelineIntegrationSpec.scala - mock PostgreSQL and MySQL sources, create pipeline with JoinTransform (inner join on user_id), mock Kafka sink, verify join logic and enriched output

- [ ] T064 [P] Integration test for Scenario 4 (Retry Logic) in src/test/scala/integration/pipelines/RetryIntegrationSpec.scala - create pipeline with intentionally failing loader (invalid credentials), execute and capture logs, verify exactly 3 retry attempts with 5-second delays between attempts (check timestamps), verify final PipelineFailure result

- [ ] T065 [P] Integration test for Scenario 5 (Schema Validation) in src/test/scala/integration/pipelines/SchemaValidationIntegrationSpec.scala - mock extractor returning DataFrame with missing required field (violates schema), execute pipeline, verify validation fails immediately after extract stage, verify pipeline halts without executing transform/load, verify detailed error message identifies missing field

---

## Phase 3.14: Performance Tests

- [ ] T066 [P] Performance test for batch throughput in src/test/scala/performance/throughput/BatchThroughputSpec.scala - generate 1M test records, create simple pipeline (filter/map transform), measure throughput, assert ≥100K records/sec for simple transforms, measure for complex pipeline (join/aggregate), assert ≥10K records/sec

- [ ] T067 [P] Performance test for streaming latency in src/test/scala/performance/throughput/StreamingLatencySpec.scala - generate event stream at 60K events/sec for 60 seconds, create streaming pipeline with windowing, measure p95 latency across micro-batches, assert p95 <5 seconds, verify sustained throughput ≥50K events/sec

- [ ] T068 [P] Performance test for resource usage in src/test/scala/performance/throughput/ResourceUsageSpec.scala - execute pipeline with production-scale data, monitor JVM memory usage (assert ≤80% peak), monitor CPU usage (assert average ≤60%), verify no memory leaks across multiple pipeline executions

---

## Phase 3.15: Documentation & Polish

- [ ] T069 [P] Create comprehensive README.md at repository root with sections: Project Overview, Prerequisites (Java 11, Scala 2.12, SBT), Build Instructions (sbt compile/test/assembly), Configuration Guide (JSON config schema, vault setup), Usage Examples (spark-submit commands), Architecture Overview (Strategy pattern, functional transforms), Testing Guide (run tests, coverage), Performance Benchmarks

- [ ] T070 [P] Add inline Scaladoc comments to all public traits and classes: Pipeline, Extractor, Transformer, Loader, all implementations, ConfigLoader, SchemaRegistry, Main - document parameters, return types, examples, note Strategy pattern usage

- [ ] T071 Run sbt scalafmt to format all Scala source files according to scalafmt.conf

- [ ] T072 Run sbt scalastyle to check all code against ScalaStyle rules, fix any violations

- [ ] T073 Run sbt coverage test coverageReport to generate code coverage report, verify ≥85% coverage across all modules (unit + integration tests), identify and add tests for any uncovered critical paths

---

## Dependencies

**Critical Path**:
1. Setup (T001-T004) blocks everything
2. Contract tests (T005-T008) and Data models (T009-T013) can run after setup
3. Schema management (T014-T018) requires data models
4. Core traits (T019-T022) require data models
5. Utilities (T023-T026) can run parallel with traits
6. Config (T027-T030) requires data models and utilities
7. Extractors (T031-T038) require traits, schema, config
8. Transformers (T039-T044) require traits
9. Loaders (T045-T052) require traits, schema, config
10. Core execution (T053-T058) requires all extractors/transformers/loaders
11. Main (T059-T060) requires core execution
12. Integration tests (T061-T065) require Main
13. Performance tests (T066-T068) require Main
14. Documentation (T069-T073) can start after integration tests pass

**Parallel Execution Groups**:
- Contract tests T005-T008 [P]
- Data models T009-T013 [P]
- Schema tests/impl T015-T018 (after T014)
- Extractor tests T031-T034 [P]
- Extractor implementations T035-T038 [P]
- Transformer tests T039-T041 [P]
- Transformer implementations T042-T044 [P]
- Loader tests T045-T048 [P]
- Loader implementations T049-T052 [P]
- Integration tests T061-T065 [P]
- Performance tests T066-T068 [P]
- Documentation T069-T073 [P]

---

## Parallel Execution Examples

### Example 1: Launch Contract Tests Together
```scala
// All contract tests are independent (different schema files)
// Run in parallel:
sbt "testOnly contract.schemas.UserEventSchemaSpec"
sbt "testOnly contract.schemas.TransactionSchemaSpec"
sbt "testOnly contract.schemas.UserSummarySchemaSpec"
sbt "testOnly contract.PipelineConfigSchemaSpec"
```

### Example 2: Launch Extractor Implementations Together
```scala
// All extractor implementations in separate files
// After unit tests pass, implement in parallel:
// T035: src/main/scala/com/etl/extract/KafkaExtractor.scala
// T036: src/main/scala/com/etl/extract/PostgreSQLExtractor.scala
// T037: src/main/scala/com/etl/extract/MySQLExtractor.scala
// T038: src/main/scala/com/etl/extract/S3Extractor.scala
```

### Example 3: Launch Integration Tests Together
```scala
// All integration tests are independent scenarios
// Run in parallel:
sbt "testOnly integration.pipelines.BatchPipelineIntegrationSpec"
sbt "testOnly integration.pipelines.StreamingPipelineIntegrationSpec"
sbt "testOnly integration.pipelines.JoinPipelineIntegrationSpec"
sbt "testOnly integration.pipelines.RetryIntegrationSpec"
sbt "testOnly integration.pipelines.SchemaValidationIntegrationSpec"
```

---

## Task Execution Notes

- **TDD Discipline**: Every test task must show failing tests before corresponding implementation task
- **Commit Strategy**: Commit after each task completion with descriptive message
- **Test Coverage**: Run `sbt coverage test coverageReport` regularly to track progress toward 85% target
- **Code Quality**: Run `sbt scalafmt` and `sbt scalastyle` before committing
- **Integration Validation**: After core implementation (T058), run integration tests to verify end-to-end functionality
- **Performance Baseline**: Establish performance baselines early (T066-T068) to catch regressions
- **Spark Local Mode**: Use `.master("local[*]")` for all tests to avoid requiring Spark cluster

---

## Estimated Task Distribution

- **Setup**: 4 tasks (T001-T004)
- **Contract Tests**: 4 tasks (T005-T008)
- **Core Models**: 5 tasks (T009-T013)
- **Schema Management**: 5 tasks (T014-T018)
- **Core Traits**: 4 tasks (T019-T022)
- **Utilities**: 4 tasks (T023-T026)
- **Configuration**: 4 tasks (T027-T030)
- **Extractors**: 8 tasks (T031-T038) - 4 tests + 4 implementations
- **Transformers**: 6 tasks (T039-T044) - 3 tests + 3 implementations
- **Loaders**: 8 tasks (T045-T052) - 4 tests + 4 implementations
- **Core Execution**: 6 tasks (T053-T058)
- **Main Entry**: 2 tasks (T059-T060)
- **Integration Tests**: 5 tasks (T061-T065)
- **Performance Tests**: 3 tasks (T066-T068)
- **Documentation**: 5 tasks (T069-T073)

**Total**: 73 tasks

**Estimated Timeline**:
- Solo developer: 15-20 working days (TDD discipline + comprehensive testing)
- Team of 3: 7-10 working days (leveraging parallelization)
- Assumes 6-8 productive hours/day, proper test-first workflow

---

## Success Criteria

- [ ] All 73 tasks completed
- [ ] All tests pass (unit, integration, contract, performance)
- [ ] Code coverage ≥85%
- [ ] All 5 quickstart scenarios execute successfully
- [ ] Performance benchmarks met (100K/10K batch, 50K streaming, 5s p95 latency)
- [ ] No ScalaStyle violations
- [ ] All code formatted with Scalafmt
- [ ] spark-submit deployment verified
- [ ] README complete with usage examples

When all criteria met, framework is production-ready and ready for deployment.
