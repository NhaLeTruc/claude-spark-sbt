# Contributing to claude-spark-sbt

Thank you for your interest in contributing to the claude-spark-sbt ETL framework! This guide will help you get started.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Development Workflow](#development-workflow)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Commit Guidelines](#commit-guidelines)
- [Pull Request Process](#pull-request-process)
- [Project Structure](#project-structure)

## Code of Conduct

This project follows professional engineering standards:

- **Be respectful** - Welcome diverse perspectives
- **Be constructive** - Provide helpful feedback
- **Be collaborative** - Work together towards common goals
- **Be professional** - Maintain high standards of quality

## Getting Started

### Prerequisites

- **Java 11** (LTS version)
- **Scala 2.12.18**
- **SBT 1.9.x**
- **Docker & Docker Compose** (for integration tests)
- **Git**

### Quick Setup

```bash
# Clone repository
git clone <repository-url>
cd claude-spark-sbt

# Install git hooks
./.git-hooks/install.sh

# Copy environment template
cp .env.example .env

# Start local services
docker-compose up -d

# Compile project
sbt compile

# Run tests
sbt test
```

## Development Setup

### 1. Environment Configuration

Create `.env` file from template:

```bash
cp .env.example .env
```

Edit `.env` with your local credentials (this file is git-ignored).

### 2. IDE Setup

**IntelliJ IDEA (Recommended):**
- Install Scala plugin
- Import project as SBT project
- Enable Scalafmt on save: `Settings > Editor > Code Style > Scala > Scalafmt`

**VS Code:**
- Install Metals extension
- Install Scala Syntax extension

### 3. Install Git Hooks

```bash
./.git-hooks/install.sh
```

This installs pre-commit hooks that run:
- Code formatting check
- Style validation
- Compilation
- Tests

### 4. Verify Setup

```bash
# Should compile without errors
sbt compile

# Should pass all tests
sbt test

# Should pass style checks
sbt scalastyle scalafmtCheck
```

## Development Workflow

### Test-Driven Development (TDD)

We follow TDD principles:

1. **Red** - Write failing test
2. **Green** - Make test pass
3. **Refactor** - Clean up code

Example:

```scala
// 1. Write failing test
class MyNewFeatureSpec extends AnyFlatSpec {
  "MyNewFeature" should "do something useful" in {
    val result = MyNewFeature.doSomething()
    result shouldBe "expected"
  }
}

// 2. Implement feature
object MyNewFeature {
  def doSomething(): String = "expected"
}

// 3. Refactor if needed
```

### Branch Strategy

- `main` - Production-ready code
- `develop` - Integration branch
- `feature/xxx` - New features
- `bugfix/xxx` - Bug fixes
- `hotfix/xxx` - Critical production fixes

### Making Changes

```bash
# Create feature branch
git checkout -b feature/my-new-feature

# Make changes
# ... edit files ...

# Run quality checks (done automatically by pre-commit hook)
sbt scalafmtAll scalastyle test

# Commit changes
git add .
git commit -m "feat: Add my new feature"

# Push to remote
git push origin feature/my-new-feature

# Create pull request
```

## Coding Standards

### Code Style

- **Formatting**: Scalafmt with 120-character lines (automatic)
- **Naming**:
  - `camelCase` for methods and variables
  - `PascalCase` for classes and traits
  - `UPPER_CASE` for constants
- **Documentation**: Scaladoc for all public APIs
- **Immutability**: Prefer immutable data structures

### Architecture Patterns

**Strategy Pattern:**
```scala
// Define trait
trait Extractor {
  def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame
}

// Implement strategies
class KafkaExtractor extends Extractor { ... }
class S3Extractor extends Extractor { ... }
```

**Functional Programming:**
```scala
// Prefer pure functions
def transform(df: DataFrame): DataFrame = {
  df.filter($"value" > 0)
    .groupBy($"key")
    .agg(sum($"value"))
}

// Use immutable case classes
case class PipelineConfig(...)
```

**Error Handling:**
```scala
// Use Either for recoverable errors
def loadConfig(path: String): Either[String, PipelineConfig] = { ... }

// Use Try for external operations
Try {
  Source.fromFile(path).mkString
} match {
  case Success(content) => Right(content)
  case Failure(e) => Left(e.getMessage)
}
```

### Code Quality Checklist

- [ ] Follows Scalafmt style (automatic)
- [ ] Passes Scalastyle checks
- [ ] No compiler warnings
- [ ] Public APIs have Scaladoc
- [ ] Tests cover new code (85%+ coverage)
- [ ] No hardcoded credentials or secrets
- [ ] Proper error handling with context
- [ ] Logging at appropriate levels

## Testing Guidelines

### Test Categories

**1. Unit Tests** - Fast, isolated
```scala
class MyClassSpec extends AnyFlatSpec with Matchers {
  "MyClass" should "do something" in {
    val result = MyClass.doSomething()
    result shouldBe "expected"
  }
}
```

**2. Integration Tests** - With Docker services
```scala
class MyIntegrationSpec extends IntegrationTestBase {
  "Pipeline" should "process data end-to-end" in {
    // Uses docker-compose services
    val result = pipeline.run(testData)
    result.isSuccess shouldBe true
  }
}
```

**3. Contract Tests** - Schema validation
```scala
class SchemaContractSpec extends AnyFlatSpec {
  "UserEvent schema" should "match contract" in {
    val schema = loadSchema("user-event.avsc")
    schema.getFields should contain ("event_id")
  }
}
```

### Test Structure

```scala
class FeatureSpec extends AnyFlatSpec with Matchers {
  // Arrange
  val input = ...
  val expected = ...

  // Act
  val result = performAction(input)

  // Assert
  result shouldBe expected
}
```

### Running Tests

```bash
# All tests
sbt test

# Specific test
sbt "testOnly com.etl.MyClassSpec"

# Integration tests only
sbt "testOnly *IntegrationSpec"

# With coverage
sbt clean coverage test coverageReport

# Coverage report location
open target/scala-2.12/scoverage-report/index.html
```

### Test Best Practices

- ✅ **DO**: Test behavior, not implementation
- ✅ **DO**: Use descriptive test names
- ✅ **DO**: Keep tests independent
- ✅ **DO**: Mock external dependencies
- ❌ **DON'T**: Test private methods directly
- ❌ **DON'T**: Share mutable state between tests
- ❌ **DON'T**: Test framework code (trust Spark, etc.)

## Commit Guidelines

### Commit Message Format

```
<type>(<scope>): <subject>

<body>

<footer>
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Code style (formatting, no logic change)
- `refactor`: Code restructuring (no behavior change)
- `perf`: Performance improvement
- `test`: Adding or updating tests
- `chore`: Build process, tooling

**Examples:**

```bash
# Good commits
git commit -m "feat(extractor): Add DeltaLake time travel support"
git commit -m "fix(pipeline): Resolve memory leak in cache cleanup"
git commit -m "docs: Add performance tuning guide"

# Bad commits (avoid these)
git commit -m "fixed bug"
git commit -m "WIP"
git commit -m "minor changes"
```

### Commit Best Practices

- **Atomic commits** - One logical change per commit
- **Descriptive messages** - Explain WHY, not just WHAT
- **Reference issues** - Include ticket/issue numbers
- **Sign commits** - Use GPG signatures for security

## Pull Request Process

### Before Creating PR

1. **Ensure all tests pass**
   ```bash
   sbt test
   ```

2. **Check code quality**
   ```bash
   sbt scalafmtCheckAll scalastyle
   ```

3. **Verify coverage**
   ```bash
   sbt coverage test coverageReport
   # Ensure >= 85% coverage
   ```

4. **Update documentation**
   - Update README if needed
   - Add/update Scaladoc
   - Update CHANGELOG

### Creating Pull Request

1. **Push to feature branch**
   ```bash
   git push origin feature/my-feature
   ```

2. **Create PR** with description:
   ```markdown
   ## Summary
   Brief description of changes

   ## Changes
   - Added feature X
   - Fixed bug Y
   - Improved Z

   ## Testing
   - [ ] Unit tests added/updated
   - [ ] Integration tests pass
   - [ ] Manual testing completed

   ## Checklist
   - [ ] Code compiles without warnings
   - [ ] Tests pass (>= 85% coverage)
   - [ ] Documentation updated
   - [ ] No breaking changes (or documented)
   ```

3. **Request review** from maintainers

### Review Process

**As Author:**
- Respond to feedback promptly
- Make requested changes
- Keep PR scope focused

**As Reviewer:**
- Check code quality and style
- Verify tests are adequate
- Ensure documentation is clear
- Test locally if needed

### Merging

- **Squash and merge** for feature branches
- **Rebase and merge** for clean history
- **Delete branch** after merge

## Project Structure

```
claude-spark-sbt/
├── src/
│   ├── main/
│   │   ├── scala/com/etl/
│   │   │   ├── Main.scala           # Entry point
│   │   │   ├── core/                # Pipeline core
│   │   │   ├── extract/             # Data extractors
│   │   │   ├── transform/           # Data transformers
│   │   │   ├── load/                # Data loaders
│   │   │   ├── config/              # Configuration
│   │   │   ├── model/               # Domain models
│   │   │   ├── util/                # Utilities
│   │   │   ├── schema/              # Schema management
│   │   │   ├── quality/             # Data quality
│   │   │   ├── monitoring/          # Metrics
│   │   │   └── streaming/           # Streaming support
│   │   └── resources/
│   │       ├── schemas/             # Avro schemas
│   │       └── configs/             # Example configs
│   └── test/
│       └── scala/
│           ├── unit/                # Unit tests
│           ├── integration/         # Integration tests
│           └── contract/            # Contract tests
├── .git-hooks/                      # Git hooks
├── .github/workflows/               # CI/CD
├── docker/                          # Docker scripts
├── specs/                           # Design documents
└── project/                         # SBT configuration
```

## Common Tasks

### Adding a New Extractor

1. Create extractor class
   ```scala
   class MyExtractor extends Extractor {
     override def extract(config: ExtractConfig)(implicit spark: SparkSession): DataFrame = {
       // Implementation
     }
   }
   ```

2. Add to factory in `Main.scala`
   ```scala
   case SourceType.MySource => new MyExtractor()
   ```

3. Add tests
   ```scala
   class MyExtractorSpec extends AnyFlatSpec { ... }
   ```

4. Update documentation

### Adding Configuration Options

1. Update config case class
   ```scala
   case class ExtractConfig(
     // ... existing fields
     myNewOption: Option[String] = None
   )
   ```

2. Add JSON format
   ```scala
   implicit val extractConfigFormat: Format[ExtractConfig] = Json.format[ExtractConfig]
   ```

3. Add validation
   ```scala
   def validate(config: PipelineConfig): Seq[String] = {
     // Add validation logic
   }
   ```

## Getting Help

- **Documentation**: Check `docs/` directory
- **Examples**: See `src/main/resources/configs/`
- **API Docs**: Generate with `sbt doc`
- **Issues**: Search existing issues before creating new ones

## License

This project is proprietary - internal use only.

---

**Thank you for contributing to claude-spark-sbt!** 🚀
