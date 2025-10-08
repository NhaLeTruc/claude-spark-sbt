# Delta Lake User Guide

Complete guide for using Delta Lake with the ETL framework.

**Version**: 1.0.0
**Last Updated**: 2025-10-08

---

## Table of Contents

1. [Introduction](#introduction)
2. [Getting Started](#getting-started)
3. [Core Concepts](#core-concepts)
4. [Configuration Reference](#configuration-reference)
5. [Write Operations](#write-operations)
6. [Read Operations](#read-operations)
7. [Time Travel](#time-travel)
8. [Change Data Feed](#change-data-feed)
9. [Schema Evolution](#schema-evolution)
10. [Performance Tuning](#performance-tuning)
11. [Troubleshooting](#troubleshooting)

---

## Introduction

### What is Delta Lake?

Delta Lake is an open-source storage framework that brings ACID transactions, scalable metadata handling, and unified streaming and batch data processing to data lakes.

**Key Benefits for ETL**:
- ✅ **ACID Transactions**: No more partial writes or inconsistent data
- ✅ **Upsert/Merge**: True update capability for S3 (previously impossible)
- ✅ **Time Travel**: Query historical versions, rollback bad data
- ✅ **Schema Evolution**: Add columns without breaking pipelines
- ✅ **Performance**: Data skipping, Z-ordering for faster queries
- ✅ **Audit Trail**: Complete change history for compliance

### When to Use Delta Lake

**Use Delta Lake when you need**:
- Upsert/merge operations on S3 or HDFS
- ACID guarantees for data correctness
- Time travel for rollback or audit
- Incremental ETL with CDC
- Schema evolution without downtime

**Stick with Parquet when**:
- Simple append-only workloads
- No need for updates or deletes
- Reading from external systems that don't support Delta

---

## Getting Started

### 1. Enable Delta Lake

Delta Lake is already configured in the framework. No additional setup required!

The framework automatically configures:
```scala
// Automatically applied when using DeltaLakeLoader/Extractor
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
```

### 2. Your First Delta Lake Pipeline

**Write to Delta Lake**:
```json
{
  "pipelineId": "my-first-delta-pipeline",
  "extract": {
    "sourceType": "PostgreSQL",
    "connectionParams": {
      "host": "postgres.example.com",
      "database": "source_db",
      "query": "SELECT * FROM users"
    },
    "credentialId": "postgres-creds"
  },
  "transform": [],
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://my-bucket/delta/users"
    },
    "credentialId": "s3-creds"
  },
  "writeMode": "Append"
}
```

**Read from Delta Lake**:
```json
{
  "pipelineId": "read-delta-pipeline",
  "extract": {
    "sourceType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://my-bucket/delta/users"
    },
    "credentialId": "s3-creds"
  },
  "transform": [],
  "load": {
    "sinkType": "PostgreSQL",
    "table": "analytics.users",
    "credentialId": "postgres-creds"
  },
  "writeMode": "Append"
}
```

---

## Core Concepts

### 1. Transaction Log

Every Delta table has a `_delta_log` directory containing the transaction log:
```
s3://bucket/delta-table/
├── _delta_log/
│   ├── 00000000000000000000.json  # Version 0
│   ├── 00000000000000000001.json  # Version 1
│   ├── 00000000000000000002.json  # Version 2
│   └── 00000000000000000010.checkpoint.parquet  # Checkpoint
└── part-*.parquet  # Data files
```

Each transaction creates a new version, ensuring ACID guarantees.

### 2. ACID Transactions

All Delta Lake writes are atomic:
- **Atomicity**: All-or-nothing commits (no partial writes)
- **Consistency**: Schema enforcement, constraint checks
- **Isolation**: Snapshot isolation (consistent reads)
- **Durability**: Changes persisted to transaction log

### 3. Optimistic Concurrency

Multiple writers can operate concurrently:
1. Read current version
2. Perform operation
3. Attempt commit
4. If conflict, retry (configurable)

Default: 10 retry attempts for concurrent writes.

### 4. Data Skipping

Delta collects statistics (min/max) for each file:
```scala
// Query with data skipping
spark.read.format("delta").load(path).filter("date = '2024-01-01'")
// Only reads files where min_date <= '2024-01-01' <= max_date
```

Up to 10-100x faster than full table scans!

---

## Configuration Reference

### DeltaLakeLoader Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | String | **Required** | Delta table path |
| `mergeKeys` | JSON Array | Optional | Columns for merge condition (required for Upsert) |
| `updateCondition` | String | Optional | SQL condition for updates |
| `deleteCondition` | String | Optional | SQL condition for deletes |
| `mergeSchema` | Boolean | `false` | Enable schema evolution |
| `partitionBy` | JSON Array | Optional | Partition columns |
| `replaceWhere` | String | Optional | Partition filter for overwrite |
| `optimizeWrite` | Boolean | `false` | Auto-optimize during write |
| `autoCompact` | Boolean | `false` | Auto-compact small files |
| `optimizeAfterWrite` | Boolean | `false` | Run OPTIMIZE after write |
| `zOrderColumns` | JSON Array | Optional | Columns for Z-ordering |
| `vacuumRetentionHours` | Integer | `168` (7 days) | Retention for old versions |
| `enableChangeDataFeed` | Boolean | `false` | Enable CDC tracking |

### DeltaLakeExtractor Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `path` | String | **Required** | Delta table path |
| `versionAsOf` | Long | Latest | Read specific version |
| `timestampAsOf` | String | Latest | Read as of timestamp |
| `readChangeFeed` | Boolean | `false` | Read CDC instead of current state |
| `startingVersion` | Long | Optional | Starting version for CDC |
| `startingTimestamp` | String | Optional | Starting timestamp for CDC |
| `endingVersion` | Long | Optional | Ending version for CDC |
| `endingTimestamp` | String | Optional | Ending timestamp for CDC |
| `ignoreDeletes` | Boolean | `false` | Ignore deleted rows in CDC |
| `ignoreChanges` | Boolean | `false` | Ignore updated rows in CDC |

---

## Write Operations

### Append Mode

Add new records to the table without modifying existing data.

```json
{
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users",
      "partitionBy": "[\"country\"]"
    }
  },
  "writeMode": "Append"
}
```

**Use Case**: Incremental loads, event streams, log aggregation

### Overwrite Mode

#### Full Overwrite
Replace entire table:
```json
{
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users"
    }
  },
  "writeMode": "Overwrite"
}
```

#### Partition Overwrite
Replace specific partitions only:
```json
{
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/events",
      "replaceWhere": "event_date = '2024-01-01'",
      "partitionBy": "[\"event_date\"]"
    }
  },
  "writeMode": "Overwrite"
}
```

**Use Case**: Daily/hourly partition refreshes, snapshot replacements

### Upsert Mode

Update existing records + insert new ones atomically.

#### Simple Upsert
```json
{
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users",
      "mergeKeys": "[\"id\"]"
    }
  },
  "writeMode": "Upsert"
}
```

SQL Generated:
```sql
MERGE INTO target
USING source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

#### Conditional Upsert
Only update if source is newer:
```json
{
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users",
      "mergeKeys": "[\"id\"]",
      "updateCondition": "source.updated_at > target.updated_at"
    }
  },
  "writeMode": "Upsert"
}
```

#### Upsert with Deletes (CDC Pattern)
```json
{
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users",
      "mergeKeys": "[\"id\"]",
      "deleteCondition": "source.deleted = true"
    }
  },
  "writeMode": "Upsert"
}
```

**Use Case**: CDC from databases, SCD Type 1 dimensions, real-time updates

---

## Read Operations

### Latest Version

Default behavior - read current state:
```json
{
  "extract": {
    "sourceType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users"
    }
  }
}
```

### Partitioned Reads

Automatic partition pruning:
```json
{
  "extract": {
    "sourceType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/events"
    }
  },
  "transform": [
    {
      "type": "filter",
      "parameters": {
        "condition": "event_date = '2024-01-01'"
      }
    }
  ]
}
```

Only reads partition `event_date=2024-01-01` (data skipping).

---

## Time Travel

### By Version Number

Read specific version:
```json
{
  "extract": {
    "sourceType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users",
      "versionAsOf": "42"
    }
  }
}
```

### By Timestamp

Read as of specific time:
```json
{
  "extract": {
    "sourceType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users",
      "timestampAsOf": "2024-01-01 12:00:00"
    }
  }
}
```

### Use Cases

**1. Rollback Bad Data**
```json
// Extract last known good version
{
  "extract": {
    "sourceType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users",
      "versionAsOf": "10"
    }
  },
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users"
    }
  },
  "writeMode": "Overwrite"
}
```

**2. Audit/Compliance**
Query historical state for audits:
```json
{
  "extract": {
    "sourceType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/transactions",
      "timestampAsOf": "2024-01-01 00:00:00"
    }
  }
}
```

**3. A/B Testing**
Compare current vs historical data:
```json
// Pipeline 1: Current data
{
  "extract": {
    "sourceType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/metrics"
    }
  }
}

// Pipeline 2: Yesterday's data
{
  "extract": {
    "sourceType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/metrics",
      "timestampAsOf": "2024-01-01 00:00:00"
    }
  }
}
```

---

## Change Data Feed

### Enable CDC

**Option 1**: Enable during table creation
```json
{
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users",
      "enableChangeDataFeed": "true"
    }
  },
  "writeMode": "Overwrite"
}
```

**Option 2**: Enable on existing table (via Spark SQL)
```sql
ALTER TABLE delta.`s3a://bucket/delta/users`
SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
```

### Read CDC

**All changes from version N:**
```json
{
  "extract": {
    "sourceType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users",
      "readChangeFeed": "true",
      "startingVersion": "100"
    }
  }
}
```

**Changes in time range:**
```json
{
  "extract": {
    "sourceType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users",
      "readChangeFeed": "true",
      "startingTimestamp": "2024-01-01 00:00:00",
      "endingTimestamp": "2024-01-02 00:00:00"
    }
  }
}
```

**Only inserts (ignore updates/deletes):**
```json
{
  "extract": {
    "sourceType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users",
      "readChangeFeed": "true",
      "startingVersion": "100",
      "ignoreDeletes": "true",
      "ignoreChanges": "true"
    }
  }
}
```

### CDC Schema

Change feed adds metadata columns:
- `_change_type`: `insert`, `update_preimage`, `update_postimage`, `delete`
- `_commit_version`: Version number of the change
- `_commit_timestamp`: Timestamp of the change

**Example CDC DataFrame**:
```
+---+-------+-----------+--------------+----------------+--------------------+
| id| name  |_change_type|_commit_version|_commit_timestamp|
+---+-------+-----------+--------------+----------------+--------------------+
|  1| Alice | insert    |            1 | 2024-01-01 10:00:00|
|  1| Alice | update_preimage|       2 | 2024-01-01 11:00:00|
|  1| Alice2| update_postimage|      2 | 2024-01-01 11:00:00|
|  2| Bob   | delete    |            3 | 2024-01-01 12:00:00|
+---+-------+-----------+--------------+----------------+--------------------+
```

### Use Cases

**1. Incremental ETL**
Process only changes since last run:
```json
{
  "pipelineId": "incremental-etl",
  "extract": {
    "sourceType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/source",
      "readChangeFeed": "true",
      "startingVersion": "${LAST_PROCESSED_VERSION}"
    }
  },
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/target",
      "mergeKeys": "[\"id\"]",
      "deleteCondition": "source._change_type = 'delete'"
    }
  },
  "writeMode": "Upsert"
}
```

**2. Event Streaming**
Publish changes to Kafka:
```json
{
  "extract": {
    "sourceType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users",
      "readChangeFeed": "true",
      "startingVersion": "100"
    }
  },
  "load": {
    "sinkType": "Kafka",
    "topic": "user-changes"
  }
}
```

---

## Schema Evolution

### Auto-Merge Schema

Enable automatic schema evolution:
```json
{
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users",
      "mergeSchema": "true"
    }
  },
  "writeMode": "Append"
}
```

**Behavior**:
- New columns added automatically
- Existing columns preserved
- Old records have `null` for new columns

**Example**:
```
Version 0 schema: (id: Int, name: String)
Version 1 schema: (id: Int, name: String, email: String)  // email added
```

### Schema Enforcement

Delta enforces schema by default:
- Column types must match
- Column names must match (case-insensitive by default)
- Extra columns rejected (unless mergeSchema=true)

**Error Example**:
```
DataFrame schema: (id: String, name: String)  // id is String
Table schema:     (id: Int, name: String)     // id is Int

Error: Schema mismatch - id type doesn't match (String vs Int)
```

**Fix**: Cast before write or enable mergeSchema.

---

## Performance Tuning

### 1. Optimize Write

Enable auto-optimization during write:
```json
{
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users",
      "optimizeWrite": "true",
      "autoCompact": "true"
    }
  }
}
```

**Effect**: Fewer, larger files (better read performance).

### 2. Z-Ordering

Co-locate data for faster range queries:
```json
{
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/events",
      "optimizeAfterWrite": "true",
      "zOrderColumns": "[\"date\", \"user_id\"]"
    }
  }
}
```

**When to use**:
- Queries filtering on multiple columns
- Range queries (e.g., `WHERE date BETWEEN ... AND user_id = ...`)

**Example**:
```sql
-- Without Z-order: Reads all files
SELECT * FROM events WHERE date = '2024-01-01' AND user_id = 123

-- With Z-order on (date, user_id): Reads ~10% of files
```

### 3. Partitioning

Partition large tables for query pruning:
```json
{
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/events",
      "partitionBy": "[\"date\"]"
    }
  }
}
```

**Guidelines**:
- Partition on frequently filtered columns
- Keep partitions 1GB-10GB each
- Avoid over-partitioning (< 1GB partitions)

### 4. OPTIMIZE

Run OPTIMIZE periodically to compact small files:
```json
{
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users",
      "optimizeAfterWrite": "true"
    }
  }
}
```

**Frequency**:
- High-write tables: Daily
- Medium-write tables: Weekly
- Low-write tables: Monthly

### 5. VACUUM

Reclaim storage by removing old versions:
```json
{
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users",
      "vacuumRetentionHours": "168"
    }
  }
}
```

**Retention Guidelines**:
- Audit/compliance: 30-90 days
- Development: 7 days
- Production: 14-30 days

**Warning**: Vacuum deletes old versions permanently. Time travel to vacuumed versions will fail.

---

## Troubleshooting

### Issue 1: Concurrent Write Conflicts

**Error**:
```
org.apache.spark.sql.delta.ConcurrentWriteException:
Concurrent write detected. Another operation modified the table.
```

**Cause**: Multiple writers modified the same table simultaneously.

**Solutions**:
1. **Increase retries** (default: 10):
   ```scala
   spark.conf.set("spark.databricks.delta.maxCommitAttempts", "20")
   ```

2. **Partition data** to reduce conflicts:
   ```json
   "partitionBy": "[\"date\"]"
   ```

3. **Serialize writes** for same partition:
   Schedule pipelines to avoid overlaps.

### Issue 2: Schema Mismatch

**Error**:
```
org.apache.spark.sql.AnalysisException:
Cannot write to Delta table. Schema mismatch.
```

**Solutions**:
1. **Enable schema evolution**:
   ```json
   "mergeSchema": "true"
   ```

2. **Cast columns** before write:
   ```json
   {
     "transform": [
       {
         "type": "sql",
         "query": "SELECT CAST(id AS INT) as id, name FROM __THIS__"
       }
     ]
   }
   ```

### Issue 3: Slow Reads

**Symptom**: Queries taking minutes instead of seconds.

**Diagnosis**:
```sql
DESCRIBE DETAIL delta.`s3a://bucket/delta/users`
```
Check `numFiles` - if > 1000, need optimization.

**Solutions**:
1. **OPTIMIZE**:
   ```json
   "optimizeAfterWrite": "true"
   ```

2. **Z-order** frequently queried columns:
   ```json
   "zOrderColumns": "[\"date\", \"user_id\"]"
   ```

3. **Partition** large tables:
   ```json
   "partitionBy": "[\"date\"]"
   ```

### Issue 4: Storage Costs High

**Cause**: Old versions accumulating.

**Solution**: Vacuum old versions:
```json
{
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/delta/users",
      "vacuumRetentionHours": "168"
    }
  }
}
```

**Before vacuum**: Check storage usage:
```sql
DESCRIBE DETAIL delta.`s3a://bucket/delta/users`
-- Check sizeInBytes
```

**After vacuum**: Re-check:
```sql
-- Should see 20-50% reduction
```

### Issue 5: Time Travel Fails

**Error**:
```
Version 42 is not available. Available versions: [50, 51, 52, ...]
```

**Cause**: Version vacuumed (deleted).

**Solutions**:
1. **Increase retention**:
   ```json
   "vacuumRetentionHours": "720"  // 30 days
   ```

2. **Restore from backup** if needed.

3. **Use timestamp travel** instead:
   ```json
   "timestampAsOf": "2024-01-01 00:00:00"
   ```

### Issue 6: Upsert Slow

**Symptom**: Upsert taking > 1 hour for 1M records.

**Solutions**:
1. **Partition** by merge key:
   ```json
   "partitionBy": "[\"date\"]",
   "mergeKeys": "[\"id\", \"date\"]"
   ```

2. **Z-order** merge keys:
   ```json
   "zOrderColumns": "[\"id\"]"
   ```

3. **Compact before merge**:
   ```json
   "optimizeAfterWrite": "true"
   ```

**Expected Performance**:
- 10K-50K records/sec for upsert (depending on table size)

---

## Best Practices

### 1. Design Principles

✅ **DO**:
- Use Delta Lake for tables with updates/deletes
- Enable CDC for incremental processing
- Partition large tables (> 100GB)
- OPTIMIZE weekly, VACUUM monthly
- Use upsert for idempotent writes

❌ **DON'T**:
- Over-partition (< 1GB partitions)
- Vacuum too aggressively (< 7 days retention)
- Skip optimization for high-write tables
- Use Overwrite when Upsert is appropriate

### 2. Monitoring

Track these metrics:
- **Table size**: `DESCRIBE DETAIL`
- **Version count**: `DESCRIBE HISTORY`
- **File count**: `DESCRIBE DETAIL` (numFiles)
- **Partition count**: Check _delta_log

Alert on:
- File count > 10,000 (need OPTIMIZE)
- Version count > 1,000 (need VACUUM)
- Partition count > 10,000 (over-partitioned)

### 3. Security

- **Encryption**: Enable S3 SSE-KMS for at-rest encryption
- **Credentials**: Use IAM roles (not access keys)
- **Access Control**: S3 bucket policies + IAM permissions
- **Audit**: Enable CloudTrail for S3 access logs

---

## Additional Resources

- [Delta Lake Documentation](https://docs.delta.io/)
- [Delta Lake Best Practices](https://docs.delta.io/latest/best-practices.html)
- [API Documentation](API_DOCUMENTATION.md) - Framework API reference
- [Deployment Guide](DEPLOYMENT_GUIDE.md) - Production deployment

---

**Happy Delta Lake ETL! 🚀**
