# Data Quality Validation Framework

**Version**: 1.0.0
**Date**: 2025-10-07

Comprehensive data quality validation framework for ensuring business logic constraints and data integrity in ETL pipelines.

---

## Overview

The Data Quality Validation Framework provides:

1. **Built-in Rules**: Pre-built validators for common data quality checks
2. **Configurable Validation**: JSON-based rule configuration
3. **Multi-Stage Validation**: Validate after extraction and/or transformation
4. **Flexible Failure Handling**: Abort, continue, or warn on validation failures
5. **Comprehensive Reporting**: Detailed reports with failure samples and statistics

---

## Features

### ✅ Built-in Data Quality Rules

1. **NotNullRule** - Validates that columns contain no null values
2. **RangeRule** - Validates that numeric values fall within specified ranges
3. **UniqueRule** - Validates uniqueness constraints (primary keys, natural keys)

### ✅ Validation Lifecycle

- **After Extraction**: Validate raw data from source
- **After Transformation**: Validate transformed data before loading

### ✅ Failure Handling

- **Abort**: Stop pipeline execution on validation failure (default)
- **Continue**: Log errors and continue pipeline
- **Warn**: Treat failures as warnings only

### ✅ Rich Reporting

- Pass/fail statistics
- Sample failed records for debugging
- Execution time tracking
- Severity levels (Error, Warning, Info)

---

## Quick Start

### 1. Enable Data Quality Validation

Add `dataQualityConfig` to your pipeline configuration:

```json
{
  "pipelineId": "my-pipeline",
  "dataQualityConfig": {
    "enabled": true,
    "validateAfterExtract": false,
    "validateAfterTransform": true,
    "onFailure": "abort",
    "rules": [
      {
        "ruleType": "not_null",
        "columns": ["user_id", "email"],
        "severity": "error"
      },
      {
        "ruleType": "range",
        "columns": ["age"],
        "severity": "error",
        "parameters": {
          "min": 0,
          "max": 150
        }
      },
      {
        "ruleType": "unique",
        "columns": ["email"],
        "severity": "error"
      }
    ]
  }
}
```

### 2. Run Your Pipeline

The validation runs automatically during pipeline execution:

```scala
val result = pipeline.run(context)
// Data quality validation happens automatically
// If validation fails with onFailure=abort, DataQualityException is thrown
```

### 3. Review Results

Check logs for validation results:

```
INFO  - Data quality validation completed: ✓ PASSED (3/3 rules passed in 1234ms)
INFO  - ✓ NotNullRule: PASSED (10000 records)
INFO  - ✓ RangeRule(age): PASSED (10000 records)
INFO  - ✓ UniqueRule(email): PASSED (10000 records)
```

---

## Built-in Rules

### 1. NotNullRule

Validates that specified columns contain no null values.

**Use Cases**:
- Primary key constraints
- Required fields validation
- Foreign key validation
- Mandatory business data

**Configuration**:

```json
{
  "ruleType": "not_null",
  "name": "RequiredFieldsRule",
  "columns": ["user_id", "email", "timestamp"],
  "severity": "error"
}
```

**Example Business Rules**:

```json
// Single column
{
  "ruleType": "not_null",
  "columns": ["user_id"],
  "severity": "error"
}

// Multiple columns
{
  "ruleType": "not_null",
  "columns": ["order_id", "customer_id", "order_date"],
  "severity": "error"
}

// Warning level
{
  "ruleType": "not_null",
  "columns": ["phone_number"],
  "severity": "warning"
}
```

**Validation Logic**:
- Checks if any value in specified columns is NULL
- Returns failure if any NULL found
- Provides count and sample of NULL records

**Output**:
```
✗ RequiredFieldsRule: FAILED (5/10000 records, 0.05%) [Error]
  Sample failures:
    - Row(user_id=null, email=user@example.com, timestamp=2024-10-07)
    - Row(user_id=123, email=null, timestamp=2024-10-07)
```

---

### 2. RangeRule

Validates that numeric values fall within a specified range.

**Use Cases**:
- Amount validation (must be positive)
- Age constraints (0-150)
- Percentage validation (0-100)
- Temperature ranges
- Business value constraints

**Configuration**:

```json
{
  "ruleType": "range",
  "name": "AgeValidationRule",
  "columns": ["age"],
  "severity": "error",
  "parameters": {
    "min": 0,
    "max": 150
  }
}
```

**Example Business Rules**:

```json
// Positive amounts (transactions must be > 0)
{
  "ruleType": "range",
  "name": "PositiveAmountRule",
  "columns": ["amount"],
  "severity": "error",
  "parameters": {
    "min": 0.01,
    "max": 1000000.0
  }
}

// Percentage (discount must be 0-100%)
{
  "ruleType": "range",
  "name": "DiscountPercentageRule",
  "columns": ["discount_percent"],
  "severity": "error",
  "parameters": {
    "min": 0.0,
    "max": 100.0
  }
}

// Age constraint
{
  "ruleType": "range",
  "name": "UserAgeRule",
  "columns": ["age"],
  "severity": "warning",
  "parameters": {
    "min": 18,
    "max": 120
  }
}

// Account balance (warning for negative)
{
  "ruleType": "range",
  "name": "AccountBalanceRule",
  "columns": ["balance"],
  "severity": "warning",
  "parameters": {
    "min": -10000.0,
    "max": 10000000.0
  }
}
```

**Validation Logic**:
- Checks if values are between min and max (inclusive)
- NULL values are considered failures
- Provides statistics: min found, max found, null count
- Supports all numeric types (Int, Long, Float, Double, Decimal)

**Output**:
```
✗ AgeValidationRule: FAILED (3/10000 records, 0.03%) [Error]
  Range violation stats - Min found: -5, Max found: 200, Nulls: 1
  Sample failures:
    - Row(user_id=123, age=-5, ...)
    - Row(user_id=456, age=200, ...)
    - Row(user_id=789, age=null, ...)
```

---

### 3. UniqueRule

Validates uniqueness constraints on single or multiple columns.

**Use Cases**:
- Primary key constraints
- Unique identifiers (email, SSN, transaction ID)
- Composite unique keys
- Deduplication validation
- Natural key constraints

**Configuration**:

```json
{
  "ruleType": "unique",
  "name": "UniqueEmailRule",
  "columns": ["email"],
  "severity": "error"
}
```

**Example Business Rules**:

```json
// Single column uniqueness (email)
{
  "ruleType": "unique",
  "name": "UniqueEmailRule",
  "columns": ["email"],
  "severity": "error"
}

// Primary key
{
  "ruleType": "unique",
  "name": "PrimaryKeyRule",
  "columns": ["user_id"],
  "severity": "error"
}

// Composite unique key (user + product)
{
  "ruleType": "unique",
  "name": "UserProductUniqueRule",
  "columns": ["user_id", "product_id"],
  "severity": "error"
}

// Transaction ID uniqueness
{
  "ruleType": "unique",
  "name": "TransactionIdRule",
  "columns": ["transaction_id"],
  "severity": "error"
}

// Deduplication check (warning level)
{
  "ruleType": "unique",
  "name": "DeduplicationCheckRule",
  "columns": ["order_id", "line_item_id"],
  "severity": "warning"
}
```

**Validation Logic**:
- Counts distinct values vs total records
- Returns failure if duplicates found
- Identifies which keys have duplicates
- Shows duplicate count per key
- Provides sample records for duplicate keys

**Output**:
```
✗ UniqueEmailRule: FAILED - Found 5 duplicate records (9995 unique out of 10000 total)
  Found 3 unique keys with duplicates
  Duplicate key [email=user@example.com] appears 3 times
  Duplicate key [email=admin@example.com] appears 2 times
  Sample failures:
    - Row(user_id=123, email=user@example.com, ...)
    - Row(user_id=456, email=user@example.com, ...)
    - Row(user_id=789, email=user@example.com, ...)
```

---

## Configuration Reference

### DataQualityConfig

Top-level data quality configuration.

```json
{
  "dataQualityConfig": {
    "enabled": true,
    "validateAfterExtract": false,
    "validateAfterTransform": true,
    "onFailure": "abort",
    "rules": [ /* ... */ ]
  }
}
```

**Fields**:

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable/disable data quality validation |
| `validateAfterExtract` | boolean | `false` | Run validation after extraction stage |
| `validateAfterTransform` | boolean | `true` | Run validation after transformation stage |
| `onFailure` | string | `"abort"` | Action on validation failure: `abort`, `continue`, `warn` |
| `rules` | array | `[]` | List of data quality rules to execute |

### DataQualityRuleConfig

Individual rule configuration.

```json
{
  "ruleType": "not_null",
  "name": "MyCustomRule",
  "columns": ["column1", "column2"],
  "severity": "error",
  "parameters": {
    "min": 0,
    "max": 100
  }
}
```

**Fields**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `ruleType` | string | ✅ Yes | Type of rule: `not_null`, `range`, `unique` |
| `name` | string | ❌ No | Custom rule name (auto-generated if not provided) |
| `columns` | array | ✅ Yes | List of columns to validate |
| `severity` | string | ❌ No | Severity level: `error`, `warning`, `info` (default: `error`) |
| `parameters` | object | ❌ No | Rule-specific parameters (required for `range` rule) |

### Rule-Specific Parameters

**RangeRule Parameters**:

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `min` | number | ✅ Yes | Minimum allowed value (inclusive) |
| `max` | number | ✅ Yes | Maximum allowed value (inclusive) |

```json
{
  "ruleType": "range",
  "columns": ["age"],
  "parameters": {
    "min": 0,
    "max": 150
  }
}
```

---

## Severity Levels

### Error (Default)

- **Behavior**: Validation failure blocks pipeline execution (if `onFailure=abort`)
- **Use Case**: Critical business rules that must pass
- **Examples**: Primary key constraints, required fields, valid ranges

```json
{
  "ruleType": "not_null",
  "columns": ["user_id"],
  "severity": "error"
}
```

### Warning

- **Behavior**: Validation failure logged as warning, pipeline continues
- **Use Case**: Important but non-critical data quality issues
- **Examples**: Recommended ranges, soft constraints

```json
{
  "ruleType": "range",
  "columns": ["age"],
  "severity": "warning",
  "parameters": {"min": 13, "max": 100}
}
```

### Info

- **Behavior**: Validation failure logged as info, pipeline continues
- **Use Case**: Data profiling, statistics, informational checks
- **Examples**: Data distribution checks, anomaly detection

```json
{
  "ruleType": "unique",
  "columns": ["email"],
  "severity": "info"
}
```

---

## Failure Handling Strategies

### Abort (Default)

Stop pipeline execution on validation failure.

```json
{
  "dataQualityConfig": {
    "enabled": true,
    "onFailure": "abort",
    "rules": [ /* ... */ ]
  }
}
```

**Behavior**:
- Validation runs and produces report
- If any ERROR-level rule fails → throws `DataQualityException`
- Pipeline halts immediately
- Failed records not loaded

**Use Case**: Production pipelines where data quality is critical

---

### Continue

Log errors and continue pipeline execution.

```json
{
  "dataQualityConfig": {
    "enabled": true,
    "onFailure": "continue",
    "rules": [ /* ... */ ]
  }
}
```

**Behavior**:
- Validation runs and produces report
- All failures logged as errors
- Pipeline continues execution
- Data loaded even if validation fails

**Use Case**: Best-effort pipelines, exploratory analysis

---

### Warn

Treat all failures as warnings.

```json
{
  "dataQualityConfig": {
    "enabled": true,
    "onFailure": "warn",
    "rules": [ /* ... */ ]
  }
}
```

**Behavior**:
- Validation runs and produces report
- All failures logged as warnings
- Pipeline continues execution

**Use Case**: Development, testing, data profiling

---

## Complete Example

### Example 1: E-commerce Order Validation

```json
{
  "pipelineId": "order-processing",
  "name": "Order Processing Pipeline with Data Quality",
  "extract": {
    "sourceType": "postgresql",
    "connectionParams": {
      "host": "orders-db.example.com",
      "database": "orders",
      "user": "etl_user"
    },
    "query": "SELECT * FROM orders WHERE status = 'pending'",
    "schemaName": "order_schema"
  },
  "load": {
    "sinkType": "s3",
    "path": "s3a://warehouse/orders",
    "writeMode": "append",
    "schemaName": "order_schema"
  },
  "dataQualityConfig": {
    "enabled": true,
    "validateAfterExtract": true,
    "validateAfterTransform": false,
    "onFailure": "abort",
    "rules": [
      {
        "ruleType": "not_null",
        "name": "RequiredOrderFields",
        "columns": ["order_id", "customer_id", "order_date", "total_amount"],
        "severity": "error"
      },
      {
        "ruleType": "unique",
        "name": "UniqueOrderId",
        "columns": ["order_id"],
        "severity": "error"
      },
      {
        "ruleType": "range",
        "name": "PositiveOrderAmount",
        "columns": ["total_amount"],
        "severity": "error",
        "parameters": {
          "min": 0.01,
          "max": 1000000.0
        }
      },
      {
        "ruleType": "range",
        "name": "ReasonableQuantity",
        "columns": ["quantity"],
        "severity": "warning",
        "parameters": {
          "min": 1,
          "max": 1000
        }
      }
    ]
  }
}
```

### Example 2: User Data Validation

```json
{
  "pipelineId": "user-import",
  "name": "User Data Import with Validation",
  "extract": {
    "sourceType": "s3",
    "path": "s3a://imports/users/*.csv",
    "connectionParams": {
      "format": "csv",
      "header": "true"
    },
    "schemaName": "user_import_schema"
  },
  "load": {
    "sinkType": "postgresql",
    "table": "users",
    "writeMode": "append",
    "connectionParams": {
      "host": "users-db.example.com",
      "database": "users"
    },
    "schemaName": "user_schema"
  },
  "dataQualityConfig": {
    "enabled": true,
    "validateAfterExtract": true,
    "validateAfterTransform": false,
    "onFailure": "continue",
    "rules": [
      {
        "ruleType": "not_null",
        "name": "RequiredUserFields",
        "columns": ["email", "first_name", "last_name"],
        "severity": "error"
      },
      {
        "ruleType": "unique",
        "name": "UniqueEmail",
        "columns": ["email"],
        "severity": "error"
      },
      {
        "ruleType": "range",
        "name": "ValidAge",
        "columns": ["age"],
        "severity": "warning",
        "parameters": {
          "min": 18,
          "max": 120
        }
      }
    ]
  },
  "errorHandlingConfig": {
    "dlqConfig": {
      "dlqType": "s3",
      "bucketPath": "s3a://imports/dlq/users/"
    }
  }
}
```

---

## Programmatic Usage

### Create Rules Programmatically

```scala
import com.etl.quality._

// Create NotNullRule
val notNullRule = NotNullRule(Seq("user_id", "email"), Severity.Error)

// Create RangeRule
val rangeRule = RangeRule("age", 0, 150, Severity.Error)

// Create UniqueRule
val uniqueRule = UniqueRule.primaryKey("user_id")

// Validate DataFrame
val rules = Seq(notNullRule, rangeRule, uniqueRule)
val report = DataQualityValidator.validate(df, rules, OnFailureAction.Abort)

// Check results
if (report.overallPassed) {
  println(s"✓ All rules passed: ${report.summary}")
} else {
  println(s"✗ Validation failed: ${report.summary}")
  report.failures.foreach { result =>
    println(s"  - ${result.summaryMessage}")
  }
}
```

### Handle Validation Results

```scala
import com.etl.quality._

try {
  val report = DataQualityValidator.validate(df, rules)
  println(s"Validation passed: ${report.summary}")

} catch {
  case ex: DataQualityException =>
    println(s"Validation failed: ${ex.getMessage}")
    println(ex.getFailureSummary)

    // Access detailed report
    val report = ex.report
    report.failures.foreach { result =>
      println(s"Failed rule: ${result.ruleName}")
      println(s"Failed records: ${result.failedRecords}")
      println(s"Failure rate: ${(result.failureRate * 100).formatted("%.2f")}%")

      // Sample failures
      result.sampleFailures.take(5).foreach { row =>
        println(s"  Sample: ${row.mkString(", ")}")
      }
    }
}
```

---

## Best Practices

### 1. Rule Ordering

Order rules from most critical to least critical:

```json
{
  "rules": [
    { "ruleType": "not_null", "columns": ["id"], "severity": "error" },
    { "ruleType": "unique", "columns": ["id"], "severity": "error" },
    { "ruleType": "range", "columns": ["amount"], "severity": "warning" }
  ]
}
```

### 2. Validation Stages

**Validate After Extraction** when:
- You want to validate raw source data
- Data quality issues are in the source
- You want to fail fast before transformation

**Validate After Transformation** when:
- You want to validate business logic
- Transformations might introduce issues
- You want to validate before loading

**Validate Both** when:
- You want comprehensive quality checks
- Different rules apply at different stages

### 3. Severity Levels

- **Error**: Must-pass business rules (primary keys, required fields)
- **Warning**: Important but non-blocking (recommended ranges)
- **Info**: Data profiling, statistics

### 4. OnFailure Strategy

- **abort**: Production pipelines with strict quality requirements
- **continue**: Best-effort pipelines, incremental loads
- **warn**: Development, testing, data exploration

### 5. Rule Naming

Use descriptive names that indicate business purpose:

```json
// Good
{"name": "UniqueCustomerEmail", ...}
{"name": "PositiveOrderAmount", ...}
{"name": "ValidUserAge", ...}

// Avoid
{"name": "Rule1", ...}
{"name": "Check", ...}
```

---

## Troubleshooting

### Issue: Validation Passes But Data Still Has Issues

**Cause**: Rules not configured correctly or missing columns

**Solution**:
1. Check column names match DataFrame schema exactly
2. Add debug logging: `"logLevel": "DEBUG"`
3. Review validation report for actual vs expected behavior

### Issue: Performance Degradation

**Cause**: Multiple validations triggering DataFrame actions

**Solution**:
1. Enable DataFrame caching before validation
2. Validate after transformation only (skip after extraction)
3. Reduce number of rules or use sampling

### Issue: False Positives

**Cause**: Business rules too strict or not accounting for edge cases

**Solution**:
1. Adjust min/max ranges
2. Use warning severity instead of error
3. Add null handling in upstream transformations

### Issue: Duplicate Detection Not Working

**Cause**: Data might be deduplicated at load time

**Solution**:
1. Validate after extraction, not transformation
2. Check if transformations are aggregating data
3. Verify column names are correct

---

## Performance Considerations

### DataFrame Actions

Each rule triggers DataFrame actions (count, filter, collect). To optimize:

1. **Cache DataFrames** before validation:
   ```scala
   val cachedDf = df.cache()
   val report = DataQualityValidator.validate(cachedDf, rules)
   ```

2. **Minimize Rules**: Use only necessary rules

3. **Sample for Development**:
   ```scala
   val sampleDf = df.sample(fraction = 0.1)
   val report = DataQualityValidator.validate(sampleDf, rules)
   ```

### Execution Time

Typical execution times (10M records):
- NotNullRule: 2-5 seconds
- RangeRule: 3-8 seconds
- UniqueRule: 10-30 seconds (requires groupBy)

---

## Related Documentation

- [ERROR_HANDLING.md](ERROR_HANDLING.md) - Error handling and recovery
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) - Common issues and solutions
- [README.md](README.md) - Project overview and architecture

---

**Documentation Version**: 1.0.0
**Last Updated**: 2025-10-07
**Feature Status**: ✅ Complete
