
# Implementation Plan: Spark-Based ETL Pipeline Framework

**Branch**: `001-build-an-application` | **Date**: 2025-10-06 | **Spec**: [spec.md](spec.md)
**Input**: Feature specification from `/home/bob/WORK/claude-spark-sbt/specs/001-build-an-application/spec.md`

## Execution Flow (/plan command scope)
```
1. Load feature spec from Input path
   → If not found: ERROR "No feature spec at {path}"
2. Fill Technical Context (scan for NEEDS CLARIFICATION)
   → Detect Project Type from file system structure or context (web=frontend+backend, mobile=app+api)
   → Set Structure Decision based on project type
3. Fill the Constitution Check section based on the content of the constitution document.
4. Evaluate Constitution Check section below
   → If violations exist: Document in Complexity Tracking
   → If no justification possible: ERROR "Simplify approach first"
   → Update Progress Tracking: Initial Constitution Check
5. Execute Phase 0 → research.md
   → If NEEDS CLARIFICATION remain: ERROR "Resolve unknowns"
6. Execute Phase 1 → contracts, data-model.md, quickstart.md, agent-specific template file (e.g., `CLAUDE.md` for Claude Code, `.github/copilot-instructions.md` for GitHub Copilot, `GEMINI.md` for Gemini CLI, `QWEN.md` for Qwen Code, or `AGENTS.md` for all other agents).
7. Re-evaluate Constitution Check section
   → If new violations: Refactor design, return to Phase 1
   → Update Progress Tracking: Post-Design Constitution Check
8. Plan Phase 2 → Describe task generation approach (DO NOT create tasks.md)
9. STOP - Ready for /tasks command
```

**IMPORTANT**: The /plan command STOPS at step 7. Phases 2-4 are executed by other commands:
- Phase 2: /tasks command creates tasks.md
- Phase 3-4: Implementation execution (manual or via tools)

## Summary
Building a production-grade Spark-based ETL framework that enables data engineers to compose, test, and deploy data pipelines for Extract-Transform-Load operations across Kafka, PostgreSQL, MySQL, and Amazon S3. The framework implements Strategy pattern for extensibility, functional programming for transformations (aggregations, joins, windowing), and comprehensive testing (unit, integration, contract, performance). Pipelines process data in Avro format with JSON schema validation, support both batch (Spark) and streaming (Spark Streaming) execution models, include automatic retry logic (3 attempts, 5s delay), and meet performance targets (100K rec/s simple batch, 10K rec/s complex batch, 5s p95 streaming latency). All components follow SOLID principles, achieve 85%+ test coverage, and deploy via spark-submit.

## Technical Context
**Language/Version**: Scala 2.12.18 + Java 11
**Primary Dependencies**: Apache Spark 3.5.6, Spark Streaming 3.5.6, Apache Avro 1.11.x (for schema/serialization), minimal additional libraries
**Build Tool**: SBT (Simple Build Tool) 1.9.x
**Storage**: PostgreSQL (JDBC), MySQL (JDBC), Amazon S3 (spark-hadoop-cloud), Kafka (spark-sql-kafka)
**Testing**: ScalaTest 3.2.x for unit/integration tests, mocked external systems (no real connections)
**Target Platform**: Spark cluster (on-premises or cloud), deployable via spark-submit, JVM-based execution
**Project Type**: Single Scala/Spark project with library modules
**Design Patterns**: Strategy pattern (Pipeline, Extract, Transform, Load interfaces), functional programming for data operations
**Performance Goals**: Batch 100K rec/s (simple), 10K rec/s (complex); Streaming 50K events/s, p95 < 5s
**Constraints**: Memory ≤80% peak, CPU avg ≤60%; Avro inter-stage format; JSON configs/schemas; local vault for credentials
**Scale/Scope**: 4 sources/sinks (Kafka, PostgreSQL, MySQL, S3), 3 transform types (aggregate, join, window), 3 write modes (append, overwrite, upsert), retry logic (3x, 5s delay)

## Constitution Check
*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### I. SOLID Architecture ✓
- **Single Responsibility**: Each component (Pipeline, Extractor, Transformer, Loader) has one purpose
- **Open/Closed**: Strategy pattern enables extension without modification (new sources/sinks via interface implementation)
- **Liskov Substitution**: All Extract/Transform/Load implementations are substitutable via their interfaces
- **Interface Segregation**: Separate interfaces for Pipeline, Extract, Transform, Load operations
- **Dependency Inversion**: Core pipeline depends on abstractions (traits), not concrete implementations

### II. Test-First Development ✓
- TDD approach planned: contract tests → unit tests → implementation
- All tests must fail before implementation (red-green-refactor)
- Commit sequence will show tests-first discipline

### III. Comprehensive Test Coverage ✓
- **Unit Tests**: All transformations, extractors, loaders with mocked dependencies (ScalaTest)
- **Integration Tests**: End-to-end pipeline execution with mocked Kafka/PostgreSQL/MySQL/S3
- **Contract Tests**: Avro schema validation for all data contracts
- **Performance Tests**: Throughput/latency validation against constitutional standards
- Target: 85% code coverage minimum

### IV. Clean Code Standards ✓
- Scala idioms: case classes for immutability, pattern matching, for-comprehensions
- Function size: max 20 lines, max 3 parameters
- Naming: intention-revealing (KafkaExtractor, AggregationTransform, PostgreSQLLoader)
- No duplication: functional composition for reusable data operations
- Scalafmt for formatting, ScalaStyle for linting

### V. Library-First Architecture ✓
- Core modules as standalone Scala libraries: etl-core, etl-extract, etl-transform, etl-load, etl-schemas, etl-config
- Each library independently testable without Spark cluster
- Main entry point wraps libraries for spark-submit deployment

### VI. Observability as Code ✓
- Structured logging via SLF4J with JSON formatter (Logback/Log4j2)
- Metrics logged at each stage: record counts, processing time, error rates
- Trace IDs propagated through pipeline stages
- Data quality metrics logged (validation failures, schema mismatches)

### VII. Idempotency and Fault Tolerance ✓
- Retry logic: 3 attempts with 5-second exponential backoff
- Idempotent writes: upsert mode with deterministic record IDs
- Checkpointing for Spark Streaming (incremental processing)
- Dead letter queue via log output for failed records

### Data Quality Standards ✓
- **Extract Validation**: Avro schema conformance check post-extraction
- **Transform Validation**: Business rule enforcement in transform functions (configurable validators)
- **Load Validation**: Row count reconciliation logged pre/post load
- Schema stored separately in JSON format, loaded at pipeline initialization

### Performance Standards ✓
- Targets specified match constitution: 100K/10K rec/s batch, 50K events/s streaming, 5s p95 latency
- Horizontal scalability: Spark's partitioned processing, configurable parallelism
- Performance tests validate throughput under load

**Initial Assessment**: PASS - No constitutional violations identified. Design aligns with all seven core principles, data quality standards, and performance requirements.

## Project Structure

### Documentation (this feature)
```
specs/[###-feature]/
├── plan.md              # This file (/plan command output)
├── research.md          # Phase 0 output (/plan command)
├── data-model.md        # Phase 1 output (/plan command)
├── quickstart.md        # Phase 1 output (/plan command)
├── contracts/           # Phase 1 output (/plan command)
└── tasks.md             # Phase 2 output (/tasks command - NOT created by /plan)
```

### Source Code (repository root)
```
project/                        # SBT build configuration
├── build.properties            # SBT version
└── plugins.sbt                 # SBT plugins

src/main/scala/
├── com/etl/
│   ├── core/
│   │   ├── Pipeline.scala           # Pipeline trait with run() method (Strategy)
│   │   ├── PipelineExecutor.scala   # Pipeline execution with retry logic
│   │   └── ExecutionContext.scala   # Shared context (SparkSession, metrics)
│   ├── extract/
│   │   ├── Extractor.scala          # Extract trait (Strategy interface)
│   │   ├── KafkaExtractor.scala     # Kafka implementation
│   │   ├── PostgreSQLExtractor.scala
│   │   ├── MySQLExtractor.scala
│   │   └── S3Extractor.scala
│   ├── transform/
│   │   ├── Transformer.scala        # Transform trait
│   │   ├── AggregationTransform.scala
│   │   ├── JoinTransform.scala
│   │   └── WindowingTransform.scala
│   ├── load/
│   │   ├── Loader.scala             # Load trait with WriteMode
│   │   ├── KafkaLoader.scala
│   │   ├── PostgreSQLLoader.scala
│   │   ├── MySQLLoader.scala
│   │   └── S3Loader.scala
│   ├── schema/
│   │   ├── SchemaRegistry.scala     # Avro schema management
│   │   └── SchemaValidator.scala    # Schema validation logic
│   ├── config/
│   │   ├── PipelineConfig.scala     # Config case classes
│   │   ├── ConfigLoader.scala       # JSON config parser
│   │   └── CredentialVault.scala    # Local vault implementation
│   ├── model/
│   │   ├── WriteMode.scala          # ADT: Append, Overwrite, Upsert
│   │   ├── PipelineResult.scala     # Success/Failure with metrics
│   │   └── ExecutionMetrics.scala   # Telemetry data
│   └── util/
│       ├── Logging.scala            # Structured logging utilities
│       └── Retry.scala              # Retry logic helper
└── Main.scala                       # Entry point for spark-submit

src/main/resources/
├── schemas/                    # JSON Avro schemas
│   ├── user-event.avsc
│   └── transaction.avsc
├── configs/                    # JSON pipeline configs
│   └── example-pipeline.json
├── logback.xml                 # Logging configuration
└── application.conf            # App settings

src/test/scala/
├── unit/
│   ├── extract/
│   ├── transform/
│   ├── load/
│   └── core/
├── integration/
│   └── pipelines/
├── contract/
│   └── schemas/
└── performance/
    └── throughput/

build.sbt                       # SBT project definition
README.md
```

**Structure Decision**: Single Scala project with modular package structure. Core abstractions (Pipeline, Extractor, Transformer, Loader traits) in `core/`, concrete implementations in domain packages. Functional transformations are pure functions within transform objects. Schemas and configs externalized to `resources/`. Test structure mirrors source with four levels (unit, integration, contract, performance). This design satisfies Strategy pattern (interfaces), functional paradigm (transformations), library-first (modular packages), and spark-submit compatibility (Main entry point).

## Phase 0: Outline & Research
1. **Extract unknowns from Technical Context** above:
   - For each NEEDS CLARIFICATION → research task
   - For each dependency → best practices task
   - For each integration → patterns task

2. **Generate and dispatch research agents**:
   ```
   For each unknown in Technical Context:
     Task: "Research {unknown} for {feature context}"
   For each technology choice:
     Task: "Find best practices for {tech} in {domain}"
   ```

3. **Consolidate findings** in `research.md` using format:
   - Decision: [what was chosen]
   - Rationale: [why chosen]
   - Alternatives considered: [what else evaluated]

**Output**: research.md with all NEEDS CLARIFICATION resolved

## Phase 1: Design & Contracts
*Prerequisites: research.md complete*

1. **Extract entities from feature spec** → `data-model.md`:
   - Entity name, fields, relationships
   - Validation rules from requirements
   - State transitions if applicable

2. **Generate API contracts** from functional requirements:
   - For each user action → endpoint
   - Use standard REST/GraphQL patterns
   - Output OpenAPI/GraphQL schema to `/contracts/`

3. **Generate contract tests** from contracts:
   - One test file per endpoint
   - Assert request/response schemas
   - Tests must fail (no implementation yet)

4. **Extract test scenarios** from user stories:
   - Each story → integration test scenario
   - Quickstart test = story validation steps

5. **Update agent file incrementally** (O(1) operation):
   - Run `.specify/scripts/bash/update-agent-context.sh claude`
     **IMPORTANT**: Execute it exactly as specified above. Do not add or remove any arguments.
   - If exists: Add only NEW tech from current plan
   - Preserve manual additions between markers
   - Update recent changes (keep last 3)
   - Keep under 150 lines for token efficiency
   - Output to repository root

**Output**: data-model.md, /contracts/*, failing tests, quickstart.md, agent-specific file

## Phase 2: Task Planning Approach
*This section describes what the /tasks command will do - DO NOT execute during /plan*

**Task Generation Strategy**:
- Load `.specify/templates/tasks-template.md` as base
- Generate tasks from Phase 1 artifacts (data-model.md, contracts/, quickstart.md)
- Schema validation tasks from 3 Avro contracts (user-event, transaction, user-summary) → contract tests [P]
- Core trait/interface tasks (Pipeline, Extractor, Transformer, Loader) → unit tests + implementation
- Extractor implementations (Kafka, PostgreSQL, MySQL, S3) → unit tests [P] + implementation [P]
- Transformer implementations (Aggregation, Join, Windowing) → unit tests [P] + implementation [P]
- Loader implementations (Kafka, PostgreSQL, MySQL, S3) → unit tests [P] + implementation [P]
- Configuration module (PipelineConfig, ConfigLoader, CredentialVault) → unit tests + implementation
- Integration tests from quickstart scenarios (5 scenarios) → integration test tasks
- Performance tests for throughput/latency validation

**Ordering Strategy**:
- TDD strict order: Contract tests → Unit tests → Implementation → Integration tests → Performance tests
- Dependency order:
  1. Build setup (SBT, dependencies)
  2. Data models (case classes, ADTs)
  3. Schema registry and validators
  4. Core traits/interfaces
  5. Concrete implementations (extractors, transformers, loaders)
  6. Configuration and vault
  7. Pipeline executor with retry logic
  8. Main entry point
  9. Integration tests
  10. Performance tests
- Mark [P] for parallel execution (independent modules/files)
- Same-file edits cannot be parallel

**Task Categorization**:
- **Setup** (~3 tasks): SBT project, dependencies, directory structure
- **Contract Tests** (~3 tasks): Avro schema validation tests [P]
- **Core Models** (~5 tasks): Case classes, ADTs, traits
- **Unit Tests** (~15 tasks): One per component/implementation
- **Implementation** (~18 tasks): Extractors (4), Transformers (3), Loaders (4), Config (3), Core (2), Utils (2)
- **Integration Tests** (~5 tasks): End-to-end scenarios from quickstart
- **Performance Tests** (~2 tasks): Batch and streaming benchmarks
- **Documentation** (~2 tasks): README, inline docs

**Estimated Output**: 50-55 numbered, ordered tasks in tasks.md (comprehensive due to Strategy pattern requiring many implementations)

**IMPORTANT**: This phase is executed by the /tasks command, NOT by /plan

## Phase 3+: Future Implementation
*These phases are beyond the scope of the /plan command*

**Phase 3**: Task execution (/tasks command creates tasks.md)  
**Phase 4**: Implementation (execute tasks.md following constitutional principles)  
**Phase 5**: Validation (run tests, execute quickstart.md, performance validation)

## Complexity Tracking
*Fill ONLY if Constitution Check has violations that must be justified*

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| [e.g., 4th project] | [current need] | [why 3 projects insufficient] |
| [e.g., Repository pattern] | [specific problem] | [why direct DB access insufficient] |


## Progress Tracking
*This checklist is updated during execution flow*

**Phase Status**:
- [x] Phase 0: Research complete (/plan command)
- [x] Phase 1: Design complete (/plan command)
- [x] Phase 2: Task planning complete (/plan command - describe approach only)
- [ ] Phase 3: Tasks generated (/tasks command)
- [ ] Phase 4: Implementation complete
- [ ] Phase 5: Validation passed

**Gate Status**:
- [x] Initial Constitution Check: PASS
- [x] Post-Design Constitution Check: PASS
- [x] All NEEDS CLARIFICATION resolved (none identified)
- [x] Complexity deviations documented (none - design fully compliant)

**Artifacts Generated**:
- [x] plan.md (this file)
- [x] research.md (Phase 0 - technical decisions)
- [x] data-model.md (Phase 1 - 9 entities)
- [x] contracts/user-event.avsc (Phase 1 - Avro schema)
- [x] contracts/transaction.avsc (Phase 1 - Avro schema)
- [x] contracts/user-summary.avsc (Phase 1 - Avro schema)
- [x] contracts/pipeline-config-schema.json (Phase 1 - JSON schema)
- [x] quickstart.md (Phase 1 - 5 test scenarios)
- [ ] tasks.md (Phase 2 - awaiting /tasks command)
- [x] CLAUDE.md (agent context - updated with tech stack)

---
*Based on Constitution v1.0.0 - See `.specify/memory/constitution.md`*
