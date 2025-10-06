<!--
Sync Impact Report:
- Version change: [Initial] → 1.0.0
- Change type: MINOR (Initial constitution creation)
- Principles defined: 7 core principles
- Sections added: Core Principles, Data Quality Standards, Performance Standards, Governance
- Templates status:
  ✅ plan-template.md - Constitution Check section references this document
  ✅ spec-template.md - Requirement completeness aligns with principles
  ✅ tasks-template.md - TDD and testing discipline enforced
- Follow-up TODOs: None - all placeholders resolved
- Rationale: Initial constitution establishing ETL-specific governance for clean code,
  SOLID principles, comprehensive testing, data quality, and big data performance
-->

# claude-spark-sbt Constitution

## Core Principles

### I. SOLID Architecture (NON-NEGOTIABLE)
Every component MUST adhere to SOLID principles:
- **Single Responsibility**: Each class/module has one reason to change
- **Open/Closed**: Extend behavior without modifying existing code
- **Liskov Substitution**: Derived types fully substitutable for base types
- **Interface Segregation**: Clients depend only on methods they use
- **Dependency Inversion**: Depend on abstractions, not concretions

**Rationale**: SOLID principles ensure maintainable, testable, and scalable ETL
pipelines that can evolve with changing data sources and transformation requirements
without introducing regression risks.

**Enforcement**: All PRs MUST document how new components satisfy each SOLID
principle. Violations require architectural justification and remediation plan.

### II. Test-First Development (NON-NEGOTIABLE)
TDD is mandatory for all production code:
- Tests MUST be written before implementation
- Tests MUST fail initially (red phase)
- Implementation makes tests pass (green phase)
- Refactor only after tests pass
- Red-Green-Refactor cycle strictly enforced

**Rationale**: ETL systems process critical business data. Test-first development
prevents data corruption, ensures correctness, and provides regression safety nets
essential for production data pipelines.

**Enforcement**: PRs without failing test commits preceding implementation commits
will be rejected. CI/CD pipeline verifies test-first commit sequence.

### III. Comprehensive Test Coverage
All code MUST include multiple test levels:

**Unit Tests** (REQUIRED):
- Isolated component testing with mocked dependencies
- Coverage threshold: minimum 85% line coverage
- Fast execution: entire unit suite under 30 seconds
- Test file convention: `test_<module_name>.py` in `tests/unit/`

**Integration Tests** (REQUIRED):
- End-to-end pipeline validation with real dependencies
- Data source integration (databases, APIs, files)
- Transformation chain validation
- Load target verification
- Test file convention: `test_<pipeline_name>_integration.py` in `tests/integration/`

**Contract Tests** (REQUIRED for all interfaces):
- Schema validation for all data sources
- API contract verification
- Output format guarantees
- Test file convention: `test_<contract_name>_contract.py` in `tests/contract/`

**Performance Tests** (REQUIRED for ETL pipelines):
- Throughput validation against performance standards (see Section III)
- Memory usage profiling
- Concurrent processing verification
- Test file convention: `test_<pipeline_name>_performance.py` in `tests/performance/`

**Rationale**: ETL failures cascade through downstream systems. Multi-level testing
catches issues at appropriate abstraction levels: units verify logic, integration
tests verify connectivity, contracts prevent breaking changes, performance tests
ensure SLAs are met.

### IV. Clean Code Standards
All code MUST follow clean code practices:
- **Naming**: Intention-revealing names for variables, functions, classes
- **Functions**: Single responsibility, max 20 lines, max 3 parameters
- **Comments**: Code self-documents; comments explain "why" not "what"
- **Error Handling**: Explicit exception types, no silent failures
- **Formatting**: Language-standard formatters (black, prettier, etc.)
- **DRY Principle**: No code duplication; extract reusable components

**Code Review Requirements**:
- All PRs require 2 approvals from team members
- Reviewers MUST verify clean code standards
- Automated linting MUST pass before review

**Rationale**: Clean code reduces cognitive load, prevents bugs, and enables team
velocity. ETL pipelines are often long-lived; readable code reduces maintenance cost.

### V. Library-First Architecture
All functionality MUST be implemented as libraries:
- Each ETL component is a standalone, reusable library
- Libraries MUST be independently testable without system dependencies
- Clear API boundaries with documented contracts
- CLI interfaces wrap library functionality for operational use

**Library Requirements**:
- Self-contained with explicit dependencies
- Comprehensive documentation (docstrings + README)
- Semantic versioning for all libraries
- Published to internal package registry

**Rationale**: Library-first design enables component reuse across pipelines,
simplifies testing (no system setup needed), and allows independent evolution
of ETL building blocks.

### VI. Observability as Code
All components MUST include observability:
- **Structured Logging**: JSON-formatted logs with contextual fields
- **Metrics**: Counters, gauges, histograms for all operations
- **Tracing**: Distributed trace IDs for cross-component flows
- **Health Checks**: Liveness and readiness endpoints

**Required Instrumentation**:
- Record count at each transformation stage
- Processing time per batch
- Error rates and types
- Data quality metrics (see Data Quality Standards)
- Resource utilization (CPU, memory, I/O)

**Rationale**: ETL systems process data asynchronously and at scale. Without
observability, debugging production issues is impossible. Structured telemetry
enables quick root cause analysis and proactive issue detection.

### VII. Idempotency and Fault Tolerance
All ETL operations MUST be idempotent and fault-tolerant:
- **Idempotency**: Re-running a pipeline produces identical results
- **Checkpointing**: Save state to enable resume from failure point
- **Retry Logic**: Exponential backoff with configurable retry limits
- **Dead Letter Queues**: Capture unprocessable records for analysis
- **Transaction Boundaries**: Atomic commits for batch operations

**Implementation Requirements**:
- Unique identifiers for all records (deterministic or external)
- Upsert semantics for load operations
- Incremental processing support (watermarks, cursors)
- Graceful degradation on partial failures

**Rationale**: Big data pipelines run for hours and inevitably encounter transient
failures (network, quota, resource). Idempotent, fault-tolerant design prevents
data loss and enables reliable automation.

## Data Quality Standards

### Validation Gates (MANDATORY)
Every ETL pipeline MUST implement validation at three gates:

**Extract Validation**:
- Schema conformance (all expected fields present with correct types)
- Referential integrity (foreign keys valid if applicable)
- Completeness checks (no unexpected nulls in required fields)
- Freshness verification (source data within expected time window)

**Transform Validation**:
- Business rule enforcement (e.g., amounts non-negative, dates valid)
- Anomaly detection (outliers, suspicious patterns)
- Deduplication verification
- Derived field accuracy

**Load Validation**:
- Row count reconciliation (extract count = load count ± expected filters)
- Sample data spot checks (random rows match source)
- Aggregate consistency (sums, counts match source aggregates)
- Post-load query validation

**Failure Handling**:
- Validation failures MUST halt pipeline execution
- Failed batches logged with failure reason and affected record IDs
- Alerts sent to on-call personnel
- Manual approval required to bypass validation failures

### Data Lineage Tracking
All transformations MUST record lineage metadata:
- Source system and extraction timestamp
- Transformation version and execution ID
- Record-level lineage for critical fields
- Audit trail for regulatory compliance

### Data Quality Metrics
Pipelines MUST emit quality metrics:
- Validation failure rate by rule
- Null rate per field
- Duplicate detection rate
- Schema evolution events

**Rationale**: Poor data quality destroys downstream analytics and business decisions.
Multi-gate validation catches issues early. Lineage enables debugging and compliance.
Metrics provide visibility into quality trends.

## Performance Standards

### Throughput Requirements
ETL pipelines MUST meet minimum throughput standards:

**Batch Processing**:
- Minimum: 100,000 records/second for simple transformations (filter, map)
- Minimum: 10,000 records/second for complex transformations (joins, aggregations)
- Target batch size: 10,000-100,000 records (tuned per pipeline)

**Streaming Processing**:
- Maximum latency: 5 seconds end-to-end (p95)
- Minimum throughput: 50,000 events/second sustained
- Backpressure handling with graceful degradation

**Resource Limits**:
- Memory: Maximum 80% utilization under peak load
- CPU: Average ≤60% utilization, bursts ≤90%
- Network: Efficient use of bandwidth (compression enabled)
- Storage: Local disk only for temporary state, all persistent state remote

### Scalability Requirements
Pipelines MUST scale horizontally:
- Partitioned processing (data split across workers)
- Stateless workers (state externalized to shared storage)
- Auto-scaling based on queue depth or throughput lag
- No hard-coded resource limits (configurable via parameters)

### Performance Testing
All pipelines MUST include performance regression tests:
- Benchmark with representative production data volumes
- Validate throughput meets standards
- Profile memory and CPU usage
- Test scalability (2x data = 2x resources = same latency)

**Rationale**: Big data pipelines fail silently through degraded performance.
Explicit throughput standards ensure SLAs are met. Horizontal scalability prevents
bottlenecks. Performance tests catch regressions before production.

## Governance

### Amendment Process
This constitution supersedes all other development practices. Amendments require:
1. Documented proposal with rationale and impact analysis
2. Team review and approval (75% consensus)
3. Migration plan for existing code (if backward-incompatible)
4. Update to all dependent templates and documentation
5. Version increment following semantic versioning

### Compliance Review
All pull requests MUST verify constitutional compliance:
- Automated checks: test coverage, linting, performance benchmarks
- Code review checklist includes SOLID principles and clean code standards
- Architecture review for new components (library boundaries, observability)
- Data quality validation for new pipelines

### Deviation Justification
Deviations from constitutional principles require:
- Explicit justification in PR description
- Documentation in `Complexity Tracking` section of plan.md
- Time-boxed exception (e.g., "temporary workaround, refactor by Q2 2026")
- Tech debt ticket created and tracked

### Continuous Improvement
Constitution effectiveness reviewed quarterly:
- Metrics: test coverage trends, incident rate, time-to-resolution
- Feedback: developer survey on pain points and bottlenecks
- Evolution: Propose amendments based on learnings

**Version**: 1.0.0 | **Ratified**: 2025-10-06 | **Last Amended**: 2025-10-06
