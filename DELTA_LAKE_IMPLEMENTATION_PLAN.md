# Delta Lake Support Implementation Plan

**Feature**: Item #19 from Technical Debt - Delta Lake Support
**Effort Estimate**: 12-16 hours
**Priority**: IMPROVEMENT (Medium Impact)
**Status**: PLANNED

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Background and Motivation](#background-and-motivation)
3. [Current State Analysis](#current-state-analysis)
4. [Delta Lake Benefits](#delta-lake-benefits)
5. [Technical Design](#technical-design)
6. [Implementation Plan](#implementation-plan)
7. [Testing Strategy](#testing-strategy)
8. [Migration Guide](#migration-guide)
9. [Performance Considerations](#performance-considerations)
10. [Risks and Mitigation](#risks-and-mitigation)

---

## Executive Summary

This plan outlines the implementation of Delta Lake support for the ETL framework, adding ACID transactions, time travel, schema evolution, and UPSERT capabilities to S3-based data storage.

### Key Deliverables

1. **DeltaLakeLoader** - New loader with ACID transactions, merge/upsert, time travel
2. **DeltaLakeExtractor** - New extractor with time travel queries, versioning
3. **Delta Lake Configuration** - New config options for Delta-specific features
4. **Unit Tests** - Comprehensive test coverage for Delta operations
5. **Integration Tests** - End-to-end Delta Lake pipeline tests
6. **Documentation** - Usage guide and migration instructions

### Impact

- ✅ **ACID Transactions**: Atomic S3 writes (no partial failures)
- ✅ **Upsert/Merge**: True upsert support for S3 (currently impossible)
- ✅ **Time Travel**: Query historical versions, audit trail
- ✅ **Schema Evolution**: Automatic schema evolution without pipeline changes
- ✅ **Performance**: Faster reads via data skipping, Z-ordering
- ✅ **Data Quality**: Consistent reads, no partial writes

---

## Background and Motivation

### Current Limitations (from TECHNICAL_DEBT_AND_IMPROVEMENTS.md:860-884)

**Problem**:
- Only supports Parquet, CSV, JSON, Avro
- No ACID transactions for S3 writes
- No time travel or schema evolution
- No merge/upsert for S3

**From S3Loader.scala:37-40**:
```scala
// Validate write mode - S3 doesn't support Upsert
if (mode == WriteMode.Upsert) {
  throw new IllegalArgumentException(
    "S3 does not support Upsert mode. Use Append or Overwrite."
  )
}
```

**Current Pain Points**:
1. **No Upsert**: Cannot update existing records in S3, only Append/Overwrite
2. **No Atomicity**: Partial writes on failure leave inconsistent data
3. **No Versioning**: Cannot query previous versions or rollback bad data
4. **Schema Changes**: Breaking schema changes require full pipeline rewrites
5. **Slow Reads**: Full table scans without data skipping

### Why Delta Lake?

Delta Lake is the **most mature** open table format (compared to Iceberg, Hudi):
- **Spark Native**: Built by Databricks, seamless Spark integration
- **Production Ready**: Used by thousands of organizations (Netflix, Comcast, etc.)
- **Active Development**: Frequent releases, bug fixes
- **Rich Features**: ACID, time travel, schema evolution, Z-ordering, data skipping

**Alternatives Considered**:
- **Apache Iceberg**: More vendor-neutral, but less mature Spark integration
- **Apache Hudi**: Good for CDC, but more complex configuration
- **Parquet Only**: Current state - no ACID, no upsert

**Decision**: Start with Delta Lake due to Spark integration and maturity.

---

## Current State Analysis

### Existing S3 Implementation

#### S3Extractor ([src/main/scala/com/etl/extract/S3Extractor.scala](src/main/scala/com/etl/extract/S3Extractor.scala))

**Supported Formats**: CSV, JSON, Parquet, Avro
**Key Features**:
- Vault-based credential management
- Format-specific options (headers, compression, schema)
- IAM role support
- Partitioned data reading

**Pattern**:
```scala
var reader = spark.read.format(format)  // csv, json, parquet, avro
// Apply format-specific options
val df = reader.load(s3Path)
```

#### S3Loader ([src/main/scala/com/etl/load/S3Loader.scala](src/main/scala/com/etl/load/S3Loader.scala))

**Supported Formats**: CSV, JSON, Parquet, Avro
**Write Modes**: Append, Overwrite (Upsert throws exception)
**Key Features**:
- Partitioning support
- Format-specific compression
- AWS credential management

**Pattern**:
```scala
var writer = df.write.format(format).mode(saveMode)
// Apply partitioning, compression
writer.save(s3Path)
```

### Configuration Model

**ExtractConfig** (used by extractors):
```scala
case class ExtractConfig(
  sourceType: SourceType,
  connectionParams: Map[String, String],  // Format, path, options
  credentialId: Option[String],
  path: Option[String],
  query: Option[String],
  schemaName: Option[String]
)
```

**LoadConfig** (used by loaders):
```scala
case class LoadConfig(
  sinkType: SinkType,
  connectionParams: Map[String, String],  // Format, path, options
  credentialId: Option[String],
  path: Option[String],
  table: Option[String],
  schemaName: Option[String]
)
```

**WriteMode** enum:
```scala
sealed trait WriteMode
object WriteMode {
  case object Append extends WriteMode
  case object Overwrite extends WriteMode
  case object Upsert extends WriteMode  // Only supported for JDBC, not S3
}
```

---

## Delta Lake Benefits

### 1. ACID Transactions

**Current Problem**: S3 writes can fail partially, leaving corrupt data
```scala
// Current S3Loader - NOT atomic
writer.save(s3Path)  // If fails midway, partial files left in S3
```

**With Delta Lake**: All-or-nothing writes
```scala
// Delta Lake - Atomic commit
deltaTable.write.format("delta").save(path)  // Transactional, no partial writes
```

### 2. Upsert/Merge Operations

**Current Problem**: Cannot update existing records in S3
```scala
// Current limitation in S3Loader.scala:37-40
if (mode == WriteMode.Upsert) {
  throw new IllegalArgumentException("S3 does not support Upsert mode")
}
```

**With Delta Lake**: True upsert via MERGE
```scala
// Delta Lake - Upsert support
deltaTable.as("target")
  .merge(
    updates.as("source"),
    "target.id = source.id"
  )
  .whenMatched.updateAll()
  .whenNotMatched.insertAll()
  .execute()
```

### 3. Time Travel

**Current Problem**: Cannot query historical data or rollback
```scala
// Current - No versioning
spark.read.parquet(s3Path)  // Always reads latest data only
```

**With Delta Lake**: Query any historical version
```scala
// Read data as of specific timestamp
spark.read
  .format("delta")
  .option("timestampAsOf", "2024-01-01 00:00:00")
  .load(path)

// Read data as of specific version
spark.read
  .format("delta")
  .option("versionAsOf", "42")
  .load(path)
```

### 4. Schema Evolution

**Current Problem**: Schema changes break pipelines
```scala
// Current - Schema mismatch causes failure
df.write.mode("append").parquet(path)  // Fails if schema differs
```

**With Delta Lake**: Automatic schema evolution
```scala
// Delta Lake - Automatic schema merge
df.write
  .format("delta")
  .option("mergeSchema", "true")
  .mode("append")
  .save(path)
```

### 5. Performance Optimizations

**Current Problem**: Full table scans, no data skipping
```scala
// Current Parquet - Full scan
spark.read.parquet(path).filter("date = '2024-01-01'")  // Reads all files
```

**With Delta Lake**: Data skipping, Z-ordering
```scala
// Delta Lake - Data skipping via stats
spark.read.format("delta").load(path).filter("date = '2024-01-01'")  // Skips irrelevant files

// Z-ordering for co-located data
OPTIMIZE deltaTable ZORDER BY (date, user_id)  // Faster range queries
```

### 6. Audit and Compliance

**Current Problem**: No audit trail of changes
```scala
// Current - No change tracking
// Cannot answer: "What changed? When? By whom?"
```

**With Delta Lake**: Full audit trail
```scala
// View change history
deltaTable.history()  // All operations, timestamps, users

// Vacuum old versions (GDPR compliance)
deltaTable.vacuum(retentionHours = 168)  // Keep 7 days of history
```

---

## Technical Design

### 1. New Components

#### 1.1 DeltaLakeLoader

**File**: `src/main/scala/com/etl/load/DeltaLakeLoader.scala`

**Features**:
- ACID writes (transactional commits)
- Upsert/Merge operations via `MERGE INTO`
- Partition evolution
- Schema evolution
- Optimize and vacuum operations
- Change data feed (CDC)

**Configuration Parameters** (via `connectionParams`):
```scala
// Required
"format" -> "delta"
"path" -> "s3a://bucket/delta-tables/users"

// Merge/Upsert (required for WriteMode.Upsert)
"mergeKeys" -> "id"  // Or JSON array: ["id", "date"]
"updateCondition" -> "source.timestamp > target.timestamp"  // Optional

// Schema Evolution
"mergeSchema" -> "true"  // Auto-add new columns

// Optimizations
"optimizeWrite" -> "true"  // Auto-optimize during write
"autoCompact" -> "true"  // Auto-compact small files

// Partitioning
"partitionBy" -> ["date", "region"]  // JSON array
"replaceWhere" -> "date = '2024-01-01'"  // Partition overwrite

// Change Data Feed
"enableChangeDataFeed" -> "true"  // Track all changes

// Data Skipping
"dataSkippingNumIndexedCols" -> "32"  // Stats for first 32 cols
```

**Method Signature**:
```scala
class DeltaLakeLoader extends Loader {
  override def load(df: DataFrame, config: LoadConfig, mode: WriteMode): LoadResult = {
    mode match {
      case WriteMode.Append => loadAppend(df, config)
      case WriteMode.Overwrite => loadOverwrite(df, config)
      case WriteMode.Upsert => loadMerge(df, config)  // NEW: Upsert support!
    }
  }

  private def loadAppend(df: DataFrame, config: LoadConfig): LoadResult
  private def loadOverwrite(df: DataFrame, config: LoadConfig): LoadResult
  private def loadMerge(df: DataFrame, config: LoadConfig): LoadResult  // NEW
  private def optimizeTable(path: String): Unit  // NEW
  private def vacuumTable(path: String, hours: Int): Unit  // NEW
}
```

#### 1.2 DeltaLakeExtractor

**File**: `src/main/scala/com/etl/extract/DeltaLakeExtractor.scala`

**Features**:
- Time travel queries (by version or timestamp)
- Change data feed reading (CDC)
- Incremental reads (from last checkpoint)
- Schema evolution handling

**Configuration Parameters** (via `connectionParams`):
```scala
// Required
"format" -> "delta"
"path" -> "s3a://bucket/delta-tables/users"

// Time Travel
"versionAsOf" -> "42"  // Read specific version
"timestampAsOf" -> "2024-01-01 12:00:00"  // Read as of timestamp

// Change Data Feed (CDC)
"readChangeFeed" -> "true"
"startingVersion" -> "10"  // CDC from version 10
"startingTimestamp" -> "2024-01-01"

// Incremental Processing
"ignoreDeletes" -> "true"  // Ignore deleted records
"ignoreChanges" -> "false"  // Include updates
```

**Method Signature**:
```scala
class DeltaLakeExtractor extends Extractor {
  override def extractWithVault(config: ExtractConfig, vault: CredentialVault)(implicit spark: SparkSession): DataFrame = {
    if (isTimeTravelQuery(config)) {
      extractTimeTravel(config, vault)
    } else if (isChangeFeedQuery(config)) {
      extractChangeFeed(config, vault)
    } else {
      extractLatest(config, vault)
    }
  }

  private def extractLatest(config: ExtractConfig, vault: CredentialVault)(implicit spark: SparkSession): DataFrame
  private def extractTimeTravel(config: ExtractConfig, vault: CredentialVault)(implicit spark: SparkSession): DataFrame
  private def extractChangeFeed(config: ExtractConfig, vault: CredentialVault)(implicit spark: SparkSession): DataFrame
}
```

### 2. Configuration Enhancements

#### 2.1 New SourceType and SinkType Values

**In PipelineConfig.scala**:
```scala
// Add to SourceType enum
case object DeltaLake extends SourceType

// Add to SinkType enum
case object DeltaLake extends SinkType
```

#### 2.2 Delta-Specific Configuration

**Optional**: Create dedicated Delta config classes for type safety

**File**: `src/main/scala/com/etl/config/DeltaConfig.scala`
```scala
case class DeltaMergeConfig(
  mergeKeys: Seq[String],
  updateCondition: Option[String] = None,
  deleteCondition: Option[String] = None,
  updateColumns: Option[Seq[String]] = None,  // Subset of columns to update
  insertColumns: Option[Seq[String]] = None   // Subset of columns to insert
)

case class DeltaOptimizeConfig(
  enabled: Boolean = false,
  zOrderColumns: Option[Seq[String]] = None,
  autoCompact: Boolean = false,
  targetFileSize: Option[String] = None  // e.g., "128MB"
)

case class DeltaTimeTravelConfig(
  versionAsOf: Option[Long] = None,
  timestampAsOf: Option[String] = None
)

case class DeltaChangeFeedConfig(
  enabled: Boolean = false,
  startingVersion: Option[Long] = None,
  startingTimestamp: Option[String] = None,
  endingVersion: Option[Long] = None,
  endingTimestamp: Option[String] = None
)
```

**Factory to Parse from connectionParams**:
```scala
object DeltaConfigFactory {
  def parseMergeConfig(params: Map[String, String]): Option[DeltaMergeConfig] = {
    params.get("mergeKeys").map { keys =>
      DeltaMergeConfig(
        mergeKeys = Json.parse(keys).as[Seq[String]],
        updateCondition = params.get("updateCondition"),
        deleteCondition = params.get("deleteCondition")
      )
    }
  }

  def parseTimeTravelConfig(params: Map[String, String]): Option[DeltaTimeTravelConfig] = {
    if (params.contains("versionAsOf") || params.contains("timestampAsOf")) {
      Some(DeltaTimeTravelConfig(
        versionAsOf = params.get("versionAsOf").map(_.toLong),
        timestampAsOf = params.get("timestampAsOf")
      ))
    } else None
  }
}
```

### 3. Dependency Updates

#### 3.1 build.sbt Changes

**Add Delta Lake dependency**:
```scala
libraryDependencies ++= Seq(
  // Existing dependencies...

  // Delta Lake (NEW)
  "io.delta" %% "delta-core" % "3.1.0",  // Compatible with Spark 3.5.x
  "io.delta" %% "delta-storage" % "3.1.0",  // S3/HDFS optimizations

  // Existing dependencies continue...
)
```

**Version Compatibility Matrix**:
- Spark 3.5.x → Delta Lake 3.1.x or 3.2.x
- Scala 2.12.18 → Compatible with Delta 3.x
- Java 11 → Compatible with Delta 3.x

**Important**: Delta Lake 3.x requires Spark 3.5+, which we already have.

#### 3.2 Spark Configuration (Recommended)

**For optimal Delta Lake performance**:
```scala
// In Main.scala or SparkSession builder
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

// Performance tuning
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

### 4. Write Mode Support

#### 4.1 Append Mode

**Existing behavior** + Delta ACID:
```scala
df.write
  .format("delta")
  .mode("append")
  .option("mergeSchema", mergeSchema)  // Schema evolution
  .save(deltaPath)
```

**Transaction guarantees**: All-or-nothing append

#### 4.2 Overwrite Mode

**Two flavors**:

1. **Full Overwrite** (replaces entire table):
```scala
df.write
  .format("delta")
  .mode("overwrite")
  .save(deltaPath)
```

2. **Partition Overwrite** (replaces specific partitions):
```scala
df.write
  .format("delta")
  .mode("overwrite")
  .option("replaceWhere", "date >= '2024-01-01' AND date < '2024-02-01'")
  .save(deltaPath)
```

#### 4.3 Upsert Mode (NEW!)

**MERGE operation** - Most important feature:
```scala
import io.delta.tables.DeltaTable

val deltaTable = DeltaTable.forPath(spark, deltaPath)

deltaTable.as("target")
  .merge(
    df.as("source"),
    mergeCondition  // e.g., "target.id = source.id"
  )
  .whenMatched(updateCondition)  // Optional: "source.timestamp > target.timestamp"
    .updateAll()
  .whenNotMatched()
    .insertAll()
  .execute()
```

**Advanced Merge** (delete, conditional update):
```scala
deltaTable.as("target")
  .merge(df.as("source"), "target.id = source.id")
  .whenMatched("source.deleted = true")
    .delete()  // Delete if marked deleted
  .whenMatched("source.timestamp > target.timestamp")
    .updateAll()  // Update if newer
  .whenNotMatched()
    .insertAll()
  .execute()
```

---

## Implementation Plan

### Phase 1: Core Implementation (6-8 hours)

#### Step 1.1: Add Delta Lake Dependency (15 min)
- Update `build.sbt` with Delta Lake dependencies
- Test compilation: `sbt clean compile`

#### Step 1.2: Implement DeltaLakeLoader (3 hours)

**Tasks**:
1. Create `src/main/scala/com/etl/load/DeltaLakeLoader.scala`
2. Implement `loadAppend()` - Transactional append
3. Implement `loadOverwrite()` - Full/partition overwrite
4. Implement `loadMerge()` - Upsert via MERGE INTO
5. Add credential management (reuse S3Loader pattern)
6. Add partition support
7. Add schema evolution support
8. Error handling and metrics

**File Structure**:
```scala
package com.etl.load

import com.etl.config.LoadConfig
import com.etl.model.{LoadResult, WriteMode}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
import play.api.libs.json.Json

class DeltaLakeLoader extends Loader {
  private val logger = LoggerFactory.getLogger(getClass)

  override def load(df: DataFrame, config: LoadConfig, mode: WriteMode): LoadResult = {
    // Implementation
  }

  private def loadAppend(df: DataFrame, config: LoadConfig): LoadResult = ???
  private def loadOverwrite(df: DataFrame, config: LoadConfig): LoadResult = ???
  private def loadMerge(df: DataFrame, config: LoadConfig): LoadResult = ???

  private def configureDeltaWrite(
    df: DataFrame,
    config: LoadConfig,
    mode: SaveMode
  ): DataFrameWriter[Row] = ???

  private def optimizeTable(path: String, spark: SparkSession, config: LoadConfig): Unit = ???
  private def vacuumTable(path: String, spark: SparkSession, hours: Int): Unit = ???
}
```

#### Step 1.3: Implement DeltaLakeExtractor (2 hours)

**Tasks**:
1. Create `src/main/scala/com/etl/extract/DeltaLakeExtractor.scala`
2. Implement `extractLatest()` - Read latest version
3. Implement `extractTimeTravel()` - Read historical version
4. Implement `extractChangeFeed()` - Read CDC changes
5. Add credential management
6. Error handling and metrics

**File Structure**:
```scala
package com.etl.extract

import com.etl.config.{CredentialVault, ExtractConfig}
import com.etl.util.CredentialHelper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class DeltaLakeExtractor extends Extractor {
  private val logger = LoggerFactory.getLogger(getClass)

  override def extractWithVault(config: ExtractConfig, vault: CredentialVault)(implicit spark: SparkSession): DataFrame = {
    // Implementation
  }

  private def extractLatest(config: ExtractConfig, vault: CredentialVault)(implicit spark: SparkSession): DataFrame = ???
  private def extractTimeTravel(config: ExtractConfig, vault: CredentialVault)(implicit spark: SparkSession): DataFrame = ???
  private def extractChangeFeed(config: ExtractConfig, vault: CredentialVault)(implicit spark: SparkSession): DataFrame = ???

  private def configureS3Credentials(config: ExtractConfig, vault: CredentialVault)(implicit spark: SparkSession): Unit = ???
}
```

#### Step 1.4: Update PipelineConfig (30 min)

**Tasks**:
1. Add `DeltaLake` to `SourceType` enum
2. Add `DeltaLake` to `SinkType` enum
3. Update factory methods if needed

**File**: `src/main/scala/com/etl/config/PipelineConfig.scala`
```scala
sealed trait SourceType
object SourceType {
  // Existing...
  case object DeltaLake extends SourceType  // NEW
}

sealed trait SinkType
object SinkType {
  // Existing...
  case object DeltaLake extends SinkType  // NEW
}
```

#### Step 1.5: Update Factory/Registry (30 min)

**If there's a factory pattern for extractors/loaders**:
```scala
// In ExtractorFactory or similar
def create(sourceType: SourceType): Extractor = sourceType match {
  // Existing cases...
  case SourceType.DeltaLake => new DeltaLakeExtractor()
}

// In LoaderFactory or similar
def create(sinkType: SinkType): Loader = sinkType match {
  // Existing cases...
  case SinkType.DeltaLake => new DeltaLakeLoader()
}
```

#### Step 1.6: Spark Configuration Helper (30 min)

**Optional but recommended** - Centralize Delta config:

**File**: `src/main/scala/com/etl/config/DeltaConfig.scala`
```scala
package com.etl.config

import org.apache.spark.sql.SparkSession

object DeltaConfig {
  def configure(spark: SparkSession): Unit = {
    // Enable Delta Lake SQL extensions
    spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    // Performance optimizations
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

    // Retention (vacuum threshold)
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

    // Schema evolution
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
  }
}
```

**Call from Main.scala**:
```scala
// In Main.scala, after SparkSession creation
DeltaConfig.configure(spark)
```

### Phase 2: Testing (4-5 hours)

#### Step 2.1: Unit Tests - DeltaLakeLoader (2 hours)

**File**: `src/test/scala/unit/load/DeltaLakeLoaderSpec.scala`

**Test Cases**:
1. **Append Mode**:
   - Should append records to Delta table
   - Should preserve existing data
   - Should handle schema evolution (new columns)
   - Should commit atomically (all-or-nothing)

2. **Overwrite Mode**:
   - Should overwrite entire table
   - Should overwrite specific partitions (replaceWhere)
   - Should preserve table properties

3. **Upsert Mode** (CRITICAL):
   - Should insert new records
   - Should update existing records (matching keys)
   - Should handle conditional updates (timestamp check)
   - Should delete records (when condition matches)
   - Should merge on composite keys

4. **Error Handling**:
   - Should fail gracefully on invalid merge keys
   - Should rollback on write failure
   - Should return LoadResult.failure with error message

5. **Optimizations**:
   - Should optimize table when configured
   - Should vacuum old versions
   - Should Z-order when specified

**Example Test**:
```scala
class DeltaLakeLoaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  implicit var spark: SparkSession = _
  var tempDir: String = _

  override def beforeEach(): Unit = {
    spark = SparkSession.builder()
      .appName("DeltaLakeLoaderTest")
      .master("local[2]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    tempDir = Files.createTempDirectory("delta-test-").toString
  }

  "DeltaLakeLoader" should "perform upsert correctly" in {
    val loader = new DeltaLakeLoader()

    // Initial data
    val initial = Seq((1, "Alice", 100), (2, "Bob", 200)).toDF("id", "name", "amount")
    val config1 = LoadConfig(
      sinkType = SinkType.DeltaLake,
      connectionParams = Map("path" -> tempDir),
      // ...
    )
    loader.load(initial, config1, WriteMode.Overwrite)

    // Upsert: Update Alice, Insert Charlie
    val updates = Seq((1, "Alice", 150), (3, "Charlie", 300)).toDF("id", "name", "amount")
    val config2 = LoadConfig(
      sinkType = SinkType.DeltaLake,
      connectionParams = Map(
        "path" -> tempDir,
        "mergeKeys" -> "[\"id\"]"
      ),
      // ...
    )
    val result = loader.load(updates, config2, WriteMode.Upsert)

    // Verify
    result.isSuccess shouldBe true
    val final = spark.read.format("delta").load(tempDir)
    final.count() shouldBe 3
    final.filter("id = 1").select("amount").as[Int].head() shouldBe 150  // Updated
    final.filter("id = 3").count() shouldBe 1  // Inserted
  }
}
```

#### Step 2.2: Unit Tests - DeltaLakeExtractor (1.5 hours)

**File**: `src/test/scala/unit/extract/DeltaLakeExtractorSpec.scala`

**Test Cases**:
1. **Latest Read**:
   - Should read latest version
   - Should handle partitioned tables

2. **Time Travel**:
   - Should read specific version (versionAsOf)
   - Should read specific timestamp (timestampAsOf)
   - Should fail gracefully on invalid version

3. **Change Data Feed**:
   - Should read changes from specific version
   - Should include insert/update/delete operations
   - Should filter by timestamp range

4. **Schema Evolution**:
   - Should handle schema evolution gracefully
   - Should read old versions with old schema

**Example Test**:
```scala
"DeltaLakeExtractor" should "read time-travel queries correctly" in {
  val extractor = new DeltaLakeExtractor()

  // Write version 0
  val v0 = Seq((1, "Alice")).toDF("id", "name")
  v0.write.format("delta").mode("overwrite").save(tempDir)

  // Write version 1
  val v1 = Seq((1, "Alice"), (2, "Bob")).toDF("id", "name")
  v1.write.format("delta").mode("overwrite").save(tempDir)

  // Read version 0 (time travel)
  val config = ExtractConfig(
    sourceType = SourceType.DeltaLake,
    connectionParams = Map(
      "path" -> tempDir,
      "versionAsOf" -> "0"
    ),
    // ...
  )
  val df = extractor.extractWithVault(config, InMemoryVault())(spark)

  df.count() shouldBe 1  // Version 0 had 1 record
  df.select("name").as[String].head() shouldBe "Alice"
}
```

#### Step 2.3: Integration Tests (1.5 hours)

**File**: `src/test/scala/integration/DeltaLakePipelineIntegrationSpec.scala`

**Test Cases**:
1. **End-to-End Append Pipeline**:
   - S3 Parquet → Transform → Delta Lake (Append)
   - Verify ACID guarantees

2. **End-to-End Upsert Pipeline**:
   - Delta Lake → Transform → Delta Lake (Upsert)
   - Verify updates and inserts

3. **Time Travel Pipeline**:
   - Delta Lake (version 0) → Transform → S3
   - Verify historical data

4. **Schema Evolution**:
   - Write v1 schema → Write v2 schema with new columns
   - Read merged schema

**Example Test**:
```scala
class DeltaLakePipelineIntegrationSpec extends IntegrationTestBase {

  it("should perform end-to-end upsert pipeline") {
    // Stage 1: Initial load
    val initialData = (1 to 100).map(i => (i, s"user_$i", i * 10)).toDF("id", "name", "amount")
    val deltaPath = tempFilePath("users-delta")
    initialData.write.format("delta").mode("overwrite").save(deltaPath)

    // Stage 2: Upsert - Update 50 records, Insert 50 new
    val updates = ((51 to 100).map(i => (i, s"user_$i", i * 20)) ++
                   (101 to 150).map(i => (i, s"user_$i", i * 10))).toDF("id", "name", "amount")

    val loader = new DeltaLakeLoader()
    val config = LoadConfig(
      sinkType = SinkType.DeltaLake,
      connectionParams = Map(
        "path" -> deltaPath,
        "mergeKeys" -> "[\"id\"]"
      ),
      // ...
    )

    val result = loader.load(updates, config, WriteMode.Upsert)

    // Verify
    result.isSuccess shouldBe true
    val final = spark.read.format("delta").load(deltaPath)
    final.count() shouldBe 150  // 100 original + 50 new

    // Verify updates
    final.filter("id = 51").select("amount").as[Int].head() shouldBe 51 * 20  // Updated
    final.filter("id = 50").select("amount").as[Int].head() shouldBe 50 * 10  // Not updated
  }
}
```

### Phase 3: Documentation (2-3 hours)

#### Step 3.1: Update API Documentation (1 hour)

**File**: `API_DOCUMENTATION.md`

**Add sections**:
1. **DeltaLakeLoader** - Configuration, write modes, merge examples
2. **DeltaLakeExtractor** - Time travel, CDC, configuration
3. **Delta Lake Benefits** - ACID, upsert, schema evolution
4. **Migration from Parquet** - How to convert existing tables

**Example Addition**:
```markdown
## Delta Lake Loader

### Overview
DeltaLakeLoader provides ACID transactions, upsert/merge, and time travel for S3-based storage.

### Configuration

```scala
val config = LoadConfig(
  sinkType = SinkType.DeltaLake,
  connectionParams = Map(
    "path" -> "s3a://bucket/delta-tables/users",
    "mergeKeys" -> "[\"id\"]",  // For upsert mode
    "mergeSchema" -> "true",     // Schema evolution
    "optimizeWrite" -> "true"    // Auto-optimize
  ),
  credentialId = Some("s3-credentials")
)
```

### Write Modes

#### Upsert (Merge)
```scala
// Upsert based on primary key
val result = loader.load(df, config, WriteMode.Upsert)
```

Generates:
```sql
MERGE INTO target
USING source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```
```

#### Step 3.2: Create Delta Lake Guide (1 hour)

**File**: `DELTA_LAKE_GUIDE.md`

**Sections**:
1. Introduction to Delta Lake
2. Getting Started
3. Configuration Reference
4. Write Modes (Append, Overwrite, Upsert)
5. Time Travel Examples
6. Schema Evolution
7. Performance Tuning (Optimize, Z-Order, Vacuum)
8. Troubleshooting

#### Step 3.3: Update DEPLOYMENT_GUIDE.md (30 min)

**Add Delta Lake section**:
- Spark configuration for Delta
- S3 permissions for Delta (requires list/delete for transactions)
- Retention and vacuum policies

#### Step 3.4: Create Migration Guide (30 min)

**File**: `DELTA_LAKE_MIGRATION.md`

**Content**:
1. Converting Parquet to Delta
2. Updating pipeline configs
3. Testing migration
4. Rollback procedure

**Example**:
```scala
// Convert existing Parquet table to Delta
val parquetPath = "s3a://bucket/users-parquet/"
val deltaPath = "s3a://bucket/users-delta/"

val df = spark.read.parquet(parquetPath)
df.write.format("delta").mode("overwrite").save(deltaPath)

// Now use DeltaLake extractor/loader instead of S3
```

---

## Testing Strategy

### 1. Unit Test Coverage

**Target**: 100% line coverage for Delta Lake components

**Test Suites**:
1. `DeltaLakeLoaderSpec.scala` - 15-20 test cases
2. `DeltaLakeExtractorSpec.scala` - 10-15 test cases

**Critical Scenarios**:
- ✅ Append mode with ACID guarantees
- ✅ Overwrite mode (full and partition)
- ✅ Upsert mode (insert, update, delete)
- ✅ Merge with composite keys
- ✅ Conditional merge (timestamp checks)
- ✅ Schema evolution (new columns, type changes)
- ✅ Time travel (version and timestamp)
- ✅ Change data feed (CDC)
- ✅ Error handling (invalid keys, rollback)
- ✅ Optimizations (optimize, Z-order, vacuum)

### 2. Integration Test Coverage

**Test Suites**:
1. `DeltaLakePipelineIntegrationSpec.scala` - 5-7 end-to-end tests

**Scenarios**:
- ✅ Parquet → Delta migration
- ✅ Delta → Transform → Delta (upsert)
- ✅ Time travel pipeline
- ✅ CDC-based incremental pipeline
- ✅ Schema evolution across pipeline runs

### 3. Performance Benchmarks

**Optional but recommended**:

**File**: `src/test/scala/performance/DeltaLakeBenchmarkSpec.scala`

**Benchmarks**:
1. Upsert performance vs Overwrite
2. Read performance vs Parquet
3. Time travel overhead
4. Optimize/Z-order impact

**Metrics**:
- Records/second for upsert
- Query latency for time travel
- Storage size comparison (Delta vs Parquet)

### 4. Contract Tests

**Ensure compatibility**:
- Delta Lake version compatibility (3.1.x with Spark 3.5.x)
- S3 permissions (list, delete for transactions)
- Concurrent write handling

---

## Migration Guide

### Converting Existing Parquet Tables to Delta

#### Step 1: Backup Existing Data
```bash
aws s3 sync s3://bucket/users-parquet/ s3://bucket/backups/users-parquet/
```

#### Step 2: Convert to Delta (In-Place)
```scala
// Option A: In-place conversion (dangerous - no rollback!)
import io.delta.tables.DeltaTable

DeltaTable.convertToDelta(
  spark,
  "parquet.`s3a://bucket/users-parquet/`"
)
```

```scala
// Option B: Side-by-side conversion (safer)
val df = spark.read.parquet("s3a://bucket/users-parquet/")
df.write.format("delta").mode("overwrite").save("s3a://bucket/users-delta/")

// Test new Delta table
// Then update pipeline configs to use users-delta
```

#### Step 3: Update Pipeline Config
```json
// Before (Parquet)
{
  "extract": {
    "sourceType": "S3",
    "connectionParams": {
      "format": "parquet",
      "path": "s3a://bucket/users-parquet/"
    }
  }
}

// After (Delta)
{
  "extract": {
    "sourceType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/users-delta/"
    }
  }
}
```

#### Step 4: Update Write Mode (Leverage Upsert)
```json
// Before (Overwrite)
{
  "load": {
    "sinkType": "S3",
    "connectionParams": {
      "format": "parquet"
    }
  },
  "writeMode": "Overwrite"
}

// After (Upsert)
{
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://bucket/users-delta/",
      "mergeKeys": "[\"id\"]"
    }
  },
  "writeMode": "Upsert"
}
```

#### Step 5: Validate and Monitor
```scala
// Verify record count matches
val parquetCount = spark.read.parquet("s3a://bucket/users-parquet/").count()
val deltaCount = spark.read.format("delta").load("s3a://bucket/users-delta/").count()
assert(parquetCount == deltaCount, "Record count mismatch!")

// Check schema
val parquetSchema = spark.read.parquet("s3a://bucket/users-parquet/").schema
val deltaSchema = spark.read.format("delta").load("s3a://bucket/users-delta/").schema
assert(parquetSchema == deltaSchema, "Schema mismatch!")
```

#### Step 6: Decommission Parquet (After Testing)
```bash
# After successful validation, archive or delete old Parquet
aws s3 mv s3://bucket/users-parquet/ s3://bucket/archived/users-parquet/ --recursive
```

---

## Performance Considerations

### 1. Write Performance

**Optimize Small Files** (Auto-Compact):
```scala
// Enable auto-compaction during write
df.write
  .format("delta")
  .option("dataChange", "false")  // Optimization, not data change
  .save(path)

// Or configure globally
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
```

**Optimize After Write**:
```scala
import io.delta.tables.DeltaTable

val deltaTable = DeltaTable.forPath(spark, path)
deltaTable.optimize().executeCompaction()
```

**Z-Ordering** (for range queries):
```scala
// Z-order by frequently filtered columns
deltaTable.optimize().executeZOrderBy("date", "user_id")
```

### 2. Read Performance

**Data Skipping**:
```scala
// Delta automatically skips files based on min/max stats
spark.read.format("delta").load(path).filter("date = '2024-01-01'")
// Only reads files with date = '2024-01-01' (no full scan)
```

**Partition Pruning**:
```scala
// Write with partitions
df.write.format("delta").partitionBy("date").save(path)

// Read specific partition (fast)
spark.read.format("delta").load(path).filter("date = '2024-01-01'")
```

**Caching**:
```scala
// Cache frequently accessed Delta tables
val df = spark.read.format("delta").load(path)
df.cache()
```

### 3. Storage Optimization

**Vacuum Old Versions** (reclaim space):
```scala
import io.delta.tables.DeltaTable

val deltaTable = DeltaTable.forPath(spark, path)

// Delete files older than 7 days (retain 7 days for time travel)
deltaTable.vacuum(168)  // 168 hours = 7 days
```

**Retention Policy**:
```scala
// Set retention to 30 days (for compliance/audit)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
deltaTable.vacuum(720)  // 720 hours = 30 days
```

### 4. Performance Benchmarks (Expected)

**Write Performance**:
- Append: Similar to Parquet (~100K records/sec)
- Overwrite: Slightly slower than Parquet (transaction overhead)
- Upsert: Depends on merge ratio (10-50K records/sec)

**Read Performance**:
- Latest version: Similar to Parquet (with data skipping, 10-20% faster)
- Time travel: Slightly slower (metadata lookup overhead)

**Storage Size**:
- Delta Lake: 5-10% larger than Parquet (transaction logs, stats)
- After vacuum: Similar to Parquet

---

## Risks and Mitigation

### Risk 1: Delta Lake Version Compatibility

**Risk**: Delta Lake 3.x requires Spark 3.5+
**Current State**: We have Spark 3.5.6 ✅
**Mitigation**: No issue - compatible versions

### Risk 2: S3 Eventual Consistency

**Risk**: S3 is eventually consistent, Delta requires listing for transactions
**Impact**: Rare cases of concurrent writes may conflict
**Mitigation**:
- Use S3 Strong Consistency (enabled by default in all regions since 2020)
- Configure Delta for retries: `spark.databricks.delta.maxCommitAttempts = 10`
- Use S3 optimizations: `spark.databricks.delta.optimizeWrite.enabled = true`

### Risk 3: Learning Curve

**Risk**: Team unfamiliar with Delta Lake concepts (merge, time travel, optimize)
**Mitigation**:
- Comprehensive documentation (DELTA_LAKE_GUIDE.md)
- Training examples in API_DOCUMENTATION.md
- Start with simple append/overwrite, then introduce upsert

### Risk 4: Increased Storage Costs

**Risk**: Delta Lake stores transaction logs and old versions
**Impact**: 5-10% storage increase (before vacuum)
**Mitigation**:
- Regular vacuum operations (weekly/monthly)
- Set retention policy based on business needs (7-30 days)
- Monitor storage with CloudWatch/Grafana

### Risk 5: Migration Complexity

**Risk**: Converting existing Parquet tables to Delta
**Impact**: Downtime during migration, potential data loss
**Mitigation**:
- Side-by-side migration (not in-place)
- Thorough testing in staging environment
- Rollback plan (keep Parquet backups for 30 days)
- Gradual rollout (migrate one table at a time)

### Risk 6: Concurrent Write Conflicts

**Risk**: Multiple pipelines writing to same Delta table
**Impact**: OptimisticLockException, retries, failures
**Mitigation**:
- Partition data to reduce conflicts
- Use different write paths for different pipelines
- Configure isolation levels: `spark.databricks.delta.isolationLevel = "Serializable"`
- Implement retry logic in DeltaLakeLoader

### Risk 7: Performance Regression

**Risk**: Delta Lake slower than Parquet for some workloads
**Impact**: Pipeline SLA violations
**Mitigation**:
- Performance benchmarks before production
- Optimize tables regularly (compaction, Z-order)
- Use broadcast joins for small dimension tables
- Monitor metrics (compare Delta vs Parquet latency)

---

## Success Criteria

### Functional Requirements

✅ **DeltaLakeLoader**:
- Supports Append, Overwrite, Upsert modes
- Handles schema evolution
- Provides ACID guarantees
- Integrates with CredentialVault

✅ **DeltaLakeExtractor**:
- Reads latest version
- Supports time travel (version and timestamp)
- Supports change data feed (CDC)
- Integrates with CredentialVault

✅ **Configuration**:
- Delta-specific options parsed from connectionParams
- SourceType.DeltaLake and SinkType.DeltaLake added
- Backward compatible with existing configs

### Non-Functional Requirements

✅ **Performance**:
- Upsert: ≥10K records/sec (acceptable for most use cases)
- Read: Similar or better than Parquet (with data skipping)
- Storage: ≤10% increase (before vacuum)

✅ **Testing**:
- Unit test coverage: 100% for Delta components
- Integration tests: 5+ end-to-end scenarios
- All tests pass consistently

✅ **Documentation**:
- API_DOCUMENTATION.md updated
- DELTA_LAKE_GUIDE.md created
- DELTA_LAKE_MIGRATION.md created
- DEPLOYMENT_GUIDE.md updated

✅ **Production Readiness**:
- Error handling and logging
- Metrics integration (Prometheus)
- Graceful degradation (fallback to Parquet if needed)

---

## Timeline and Effort Estimate

| Phase | Tasks | Effort | Duration |
|-------|-------|--------|----------|
| **Phase 1: Core Implementation** | Add dependency, DeltaLakeLoader, DeltaLakeExtractor, Config updates | 6-8 hours | 1-2 days |
| **Phase 2: Testing** | Unit tests, integration tests, benchmarks | 4-5 hours | 1 day |
| **Phase 3: Documentation** | API docs, guides, migration docs | 2-3 hours | 0.5 day |
| **Total** | | **12-16 hours** | **2-3 days** |

### Recommended Schedule

**Day 1** (8 hours):
- ✅ Add Delta Lake dependency (15 min)
- ✅ Implement DeltaLakeLoader (3 hours)
- ✅ Implement DeltaLakeExtractor (2 hours)
- ✅ Update PipelineConfig (30 min)
- ✅ Unit tests - DeltaLakeLoader (2 hours)

**Day 2** (6 hours):
- ✅ Unit tests - DeltaLakeExtractor (1.5 hours)
- ✅ Integration tests (1.5 hours)
- ✅ Update API_DOCUMENTATION.md (1 hour)
- ✅ Create DELTA_LAKE_GUIDE.md (1 hour)
- ✅ Create DELTA_LAKE_MIGRATION.md (30 min)
- ✅ Update DEPLOYMENT_GUIDE.md (30 min)

**Day 3** (Optional - 2 hours):
- ✅ Performance benchmarks
- ✅ Additional integration tests
- ✅ Code review and refinements

---

## Example Configurations

### Example 1: Simple Append

```json
{
  "pipelineId": "users-delta-append",
  "extract": {
    "sourceType": "PostgreSQL",
    "connectionParams": {
      "host": "postgres.example.com",
      "database": "users",
      "query": "SELECT * FROM users WHERE updated_at > '2024-01-01'"
    },
    "credentialId": "postgres-creds"
  },
  "transform": [],
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://data-lake/delta/users",
      "mergeSchema": "true",
      "partitionBy": "[\"country\", \"created_date\"]"
    },
    "credentialId": "s3-creds"
  },
  "writeMode": "Append"
}
```

### Example 2: Upsert with Merge

```json
{
  "pipelineId": "users-delta-upsert",
  "extract": {
    "sourceType": "Kafka",
    "connectionParams": {
      "topic": "user-updates",
      "kafka.bootstrap.servers": "kafka:9092"
    }
  },
  "transform": [],
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://data-lake/delta/users",
      "mergeKeys": "[\"user_id\"]",
      "updateCondition": "source.updated_at > target.updated_at",
      "mergeSchema": "true"
    },
    "credentialId": "s3-creds"
  },
  "writeMode": "Upsert"
}
```

### Example 3: Time Travel Read

```json
{
  "pipelineId": "users-snapshot-report",
  "extract": {
    "sourceType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://data-lake/delta/users",
      "timestampAsOf": "2024-01-01 00:00:00"
    },
    "credentialId": "s3-creds"
  },
  "transform": [
    {
      "type": "aggregation",
      "parameters": {
        "groupBy": "[\"country\"]",
        "aggregations": "[{\"column\": \"user_id\", \"function\": \"count\", \"alias\": \"user_count\"}]"
      }
    }
  ],
  "load": {
    "sinkType": "S3",
    "connectionParams": {
      "format": "parquet",
      "path": "s3a://reports/users-snapshot-2024-01-01/"
    },
    "credentialId": "s3-creds"
  },
  "writeMode": "Overwrite"
}
```

### Example 4: Change Data Feed (CDC)

```json
{
  "pipelineId": "users-cdc-processing",
  "extract": {
    "sourceType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://data-lake/delta/users",
      "readChangeFeed": "true",
      "startingVersion": "100",
      "endingVersion": "150"
    },
    "credentialId": "s3-creds"
  },
  "transform": [],
  "load": {
    "sinkType": "Kafka",
    "connectionParams": {
      "topic": "user-changes",
      "kafka.bootstrap.servers": "kafka:9092"
    }
  },
  "writeMode": "Append"
}
```

### Example 5: Partition Overwrite

```json
{
  "pipelineId": "daily-partition-refresh",
  "extract": {
    "sourceType": "PostgreSQL",
    "connectionParams": {
      "query": "SELECT * FROM events WHERE event_date = '2024-01-01'"
    },
    "credentialId": "postgres-creds"
  },
  "transform": [],
  "load": {
    "sinkType": "DeltaLake",
    "connectionParams": {
      "path": "s3a://data-lake/delta/events",
      "replaceWhere": "event_date = '2024-01-01'",
      "partitionBy": "[\"event_date\"]"
    },
    "credentialId": "s3-creds"
  },
  "writeMode": "Overwrite"
}
```

---

## Appendix: Delta Lake Key Concepts

### 1. Transaction Log

Delta Lake maintains a transaction log (_delta_log/) that records:
- Every write operation (append, overwrite, merge)
- Schema changes
- Table properties
- Partitioning information

**Structure**:
```
s3://bucket/delta-table/
├── _delta_log/
│   ├── 00000000000000000000.json  # Version 0
│   ├── 00000000000000000001.json  # Version 1
│   ├── 00000000000000000002.json  # Version 2
│   └── 00000000000000000010.checkpoint.parquet  # Checkpoint every 10 versions
└── part-00000-*.parquet  # Data files
```

### 2. Optimistic Concurrency Control

Delta Lake uses optimistic locking:
1. Read current version
2. Perform operation
3. Attempt commit
4. If conflict, retry

**Conflict Resolution**:
- Serializable isolation (default)
- Automatic retries on conflict
- Configuration: `spark.databricks.delta.maxCommitAttempts`

### 3. Data Skipping

Delta Lake collects statistics (min/max) for each data file:
- Prune files based on filter predicates
- Significantly faster reads for selective queries

**Example**:
```sql
-- Query with data skipping
SELECT * FROM delta_table WHERE date = '2024-01-01'
-- Delta reads only files with min_date <= '2024-01-01' <= max_date
```

### 4. Z-Ordering

Z-ordering co-locates related data in the same files:
- Multi-dimensional clustering
- Improves performance for multi-column filters

**Example**:
```scala
OPTIMIZE delta_table ZORDER BY (date, user_id)
```

### 5. Time Travel

Access historical versions:
- By version number (monotonically increasing)
- By timestamp (commit time)

**Use Cases**:
- Audit and compliance
- Rollback bad data
- Reproduce ML training datasets

---

## Next Steps

1. **Review and Approve Plan**: Stakeholder sign-off on design
2. **Allocate Resources**: Assign developer(s) for 2-3 days
3. **Execute Phase 1**: Core implementation (6-8 hours)
4. **Execute Phase 2**: Testing (4-5 hours)
5. **Execute Phase 3**: Documentation (2-3 hours)
6. **Code Review**: Peer review of Delta Lake components
7. **Merge to Main**: Integrate Delta Lake support
8. **Production Pilot**: Test on non-critical pipeline
9. **Full Rollout**: Migrate additional pipelines as needed

---

**End of Delta Lake Implementation Plan**
