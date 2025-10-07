# ETL Pipeline Troubleshooting Guide

Common issues and solutions for the Spark-based ETL Pipeline Framework.

## Table of Contents
1. [Build and Compilation Issues](#build-and-compilation-issues)
2. [Runtime Errors](#runtime-errors)
3. [Source Connectivity Issues](#source-connectivity-issues)
4. [Sink Connectivity Issues](#sink-connectivity-issues)
5. [Performance Problems](#performance-problems)
6. [Data Quality Issues](#data-quality-issues)
7. [Docker Environment Issues](#docker-environment-issues)

---

## Build and Compilation Issues

### Issue: `sbt assembly` fails with "OutOfMemoryError"

**Symptoms:**
```
[error] java.lang.OutOfMemoryError: Java heap space
```

**Solution:**
```bash
# Increase SBT memory
export SBT_OPTS="-Xmx4G -XX:+UseG1GC"
sbt clean assembly
```

### Issue: Dependency conflicts during build

**Symptoms:**
```
[error] Modules were resolved with conflicting cross-version suffixes
```

**Solution:**
Check [build.sbt](build.sbt:1) and ensure all dependencies use compatible versions:
```bash
# Check dependency tree
sbt dependencyTree

# Update dependencies
sbt clean update
sbt compile
```

### Issue: Test compilation fails with "not found: type FlatSpec"

**Symptoms:**
```
[error] not found: type FlatSpec
```

**Solution:**
Ensure ScalaTest is properly imported:
```scala
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MySpec extends AnyFlatSpec with Matchers {
  // tests...
}
```

---

## Runtime Errors

### Issue: "Configuration validation failed"

**Symptoms:**
```
[ERROR] Configuration validation failed: Missing required field: extract.sourceType
```

**Solution:**
Validate your configuration against the schema:
```bash
# Check config structure
cat src/main/resources/configs/your-config.json

# Ensure all required fields are present:
# - pipelineId, extract.sourceType, load.sinkType, load.writeMode
```

**Example valid config:**
```json
{
  "pipelineId": "my-pipeline",
  "extract": {
    "sourceType": "Kafka",
    "connectionParams": { ... }
  },
  "load": {
    "sinkType": "PostgreSQL",
    "writeMode": "Append",
    "connectionParams": { ... }
  }
}
```

### Issue: "Pipeline execution failed after N attempts"

**Symptoms:**
```
[ERROR] Pipeline execution failed after 3 attempts. Error: ...
```

**Solution:**
1. Check retry configuration in your pipeline config:
```json
"retry": {
  "maxAttempts": 5,
  "delaySeconds": 30,
  "backoffMultiplier": 2.0
}
```

2. Examine the underlying error in logs:
```bash
# Check Spark driver logs
grep "ERROR" logs/driver.log

# Look for root cause
grep -A 10 "Pipeline execution failed" logs/driver.log
```

3. Common root causes:
   - Network connectivity issues → Check source/sink availability
   - Authentication failures → Verify credentials
   - Schema mismatches → Validate data schemas
   - Resource exhaustion → Check memory/disk space

### Issue: "Join transformer requires special initialization"

**Symptoms:**
```
[ERROR] Join transformer requires special initialization with right DataFrame
```

**Solution:**
The join transformer needs a right DataFrame. Current implementation requires custom pipeline builder:

```scala
// Option 1: Build custom pipeline
val leftDf = extractor.extract(config.extract)(spark)
val rightDf = // load from another source

val joinTransformer = new JoinTransformer(
  rightDf = rightDf,
  joinColumns = Seq("user_id"),
  joinType = "inner"
)

val pipeline = ETLPipeline(extractor, Seq(joinTransformer), loader)
```

**Workaround:** Use separate pipelines for each source, then join in a third pipeline reading from intermediate sinks.

---

## Source Connectivity Issues

### Issue: Kafka connection timeout

**Symptoms:**
```
[ERROR] org.apache.kafka.common.errors.TimeoutException: Failed to get offsets by times
```

**Solution:**
```bash
# 1. Verify Kafka is running
docker ps | grep kafka

# 2. Test Kafka connectivity
docker exec -it etl-kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# 3. Check Kafka topics exist
docker exec -it etl-kafka kafka-topics --list --bootstrap-server localhost:9092

# 4. Verify bootstrap servers in config
cat your-config.json | grep "kafka.bootstrap.servers"
# Should be: "localhost:9092" for local, "kafka:29092" from Docker container
```

### Issue: PostgreSQL "Connection refused"

**Symptoms:**
```
[ERROR] java.net.ConnectException: Connection refused
```

**Solution:**
```bash
# 1. Check PostgreSQL is running
docker ps | grep postgres

# 2. Test connection
docker exec -it etl-postgres pg_isready -U etl_user -d etl_database

# 3. Verify connection params in config
# From local machine: jdbc:postgresql://localhost:5432/etl_database
# From Docker: jdbc:postgresql://postgres:5432/etl_database

# 4. Check credentials
docker exec -it etl-postgres psql -U etl_user -d etl_database -c "SELECT 1;"
```

### Issue: MySQL authentication failed

**Symptoms:**
```
[ERROR] java.sql.SQLException: Access denied for user 'etl_user'@'localhost'
```

**Solution:**
```bash
# 1. Verify MySQL credentials
docker exec -it etl-mysql mysql -u etl_user -petl_password -e "SELECT 1;"

# 2. Check JDBC URL format
# Correct: jdbc:mysql://localhost:3306/etl_database
# Include parameters: jdbc:mysql://localhost:3306/etl_database?useSSL=false&allowPublicKeyRetrieval=true

# 3. Verify user permissions
docker exec -it etl-mysql mysql -u root -proot_password -e \
  "SHOW GRANTS FOR 'etl_user'@'%';"
```

### Issue: S3 "Access Denied" with LocalStack

**Symptoms:**
```
[ERROR] com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied
```

**Solution:**
```bash
# 1. Verify LocalStack is running
curl http://localhost:4566/_localstack/health

# 2. Check S3 buckets exist
aws --endpoint-url=http://localhost:4566 s3 ls

# 3. Create bucket if missing
aws --endpoint-url=http://localhost:4566 s3 mb s3://etl-source-data

# 4. Verify config parameters
cat your-config.json | jq '.extract.connectionParams'
# Required for LocalStack:
# - endpoint: "http://localhost:4566"
# - pathStyleAccess: "true"
# - accessKeyId: "test"
# - secretAccessKey: "test"
```

---

## Sink Connectivity Issues

### Issue: Kafka producer fails with "LEADER_NOT_AVAILABLE"

**Symptoms:**
```
[WARN] Error while fetching metadata with correlation id: LEADER_NOT_AVAILABLE
```

**Solution:**
```bash
# 1. Wait for topic creation (auto-create takes time)
sleep 10

# 2. Manually create topic with proper partitions
docker exec -it etl-kafka kafka-topics \
  --create \
  --if-not-exists \
  --topic your-topic \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# 3. Verify topic is ready
docker exec -it etl-kafka kafka-topics \
  --describe \
  --topic your-topic \
  --bootstrap-server localhost:9092
```

### Issue: PostgreSQL "relation does not exist"

**Symptoms:**
```
[ERROR] org.postgresql.util.PSQLException: ERROR: relation "user_events" does not exist
```

**Solution:**
```bash
# 1. List existing tables
docker exec -it etl-postgres psql -U etl_user -d etl_database -c "\dt"

# 2. Create table
docker exec -it etl-postgres psql -U etl_user -d etl_database -f \
  docker/init-scripts/postgres/01-create-tables.sql

# 3. Verify table creation
docker exec -it etl-postgres psql -U etl_user -d etl_database -c \
  "SELECT table_name FROM information_schema.tables WHERE table_schema='public';"
```

### Issue: S3 write fails with "bucket does not exist"

**Symptoms:**
```
[ERROR] The specified bucket does not exist
```

**Solution:**
```bash
# Create bucket
aws --endpoint-url=http://localhost:4566 s3 mb s3://etl-sink-data

# Verify
aws --endpoint-url=http://localhost:4566 s3 ls

# Test write
echo "test" > test.txt
aws --endpoint-url=http://localhost:4566 s3 cp test.txt s3://etl-sink-data/
```

---

## Performance Problems

### Issue: Pipeline runs very slowly

**Symptoms:**
- Extraction/transformation taking hours for small datasets
- High CPU usage
- Spark UI shows data skew

**Diagnostics:**
```bash
# 1. Check Spark UI (usually http://localhost:4040)
# Look for:
# - Tasks distribution across executors
# - Stage durations
# - Shuffle read/write sizes

# 2. Check executor logs
tail -f logs/executor-*.log | grep "WARN\|ERROR"

# 3. Monitor resource usage
docker stats
```

**Solutions:**

**Problem: Too many shuffle partitions**
```json
// Reduce in config
"performance": {
  "parallelism": 4  // Instead of 200
}
```

**Problem: Data skew**
```scala
// Add salting to skewed keys
df.withColumn("salted_key", concat(col("user_id"), lit("_"), (rand() * 10).cast("int")))
```

**Problem: Small files**
```scala
// Coalesce before write
df.coalesce(10).write.parquet("output")
```

### Issue: Out of memory errors

**Symptoms:**
```
[ERROR] java.lang.OutOfMemoryError: Java heap space
[ERROR] Container killed by YARN for exceeding memory limits
```

**Solutions:**

```bash
# 1. Increase driver/executor memory
spark-submit \
  --driver-memory 4g \
  --executor-memory 4g \
  --conf spark.memory.fraction=0.8 \
  ...

# 2. Reduce batch size in config
```json
"performance": {
  "batchSize": 10000  // Smaller batches
}
```

# 3. Enable dynamic allocation
spark-submit \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=10 \
  ...
```

### Issue: Slow JDBC reads/writes

**Symptoms:**
- Single executor doing all JDBC work
- Long task durations for JDBC operations

**Solutions:**

```json
// Add partitioning parameters to JDBC source
"connectionParams": {
  "url": "jdbc:postgresql://localhost:5432/etl_database",
  "partitionColumn": "user_id",
  "numPartitions": "10",
  "lowerBound": "0",
  "upperBound": "1000000"
}
```

```bash
# Increase fetchSize for reads
"fetchSize": "50000"

# Increase batchSize for writes
"batchsize": "10000"
```

---

## Data Quality Issues

### Issue: Schema mismatch errors

**Symptoms:**
```
[ERROR] Schema mismatch: expected Long but found String for field 'timestamp'
```

**Solution:**

```bash
# 1. Print DataFrame schema
df.printSchema()

# 2. Check Avro schema
cat src/main/resources/schemas/user-event.avsc | jq '.fields'

# 3. Add explicit schema casting in transformer
df.withColumn("timestamp", col("timestamp").cast(LongType))
```

### Issue: Null values causing failures

**Symptoms:**
```
[ERROR] NullPointerException during transformation
```

**Solution:**

```scala
// Add null checks before transformation
df.filter(col("user_id").isNotNull)
  .filter(col("event_type").isNotNull)

// Or replace nulls
df.na.fill(Map(
  "amount" -> 0.0,
  "properties" -> "{}"
))
```

### Issue: Duplicate records in output

**Symptoms:**
- More records in sink than expected
- Duplicate primary keys

**Solution:**

```scala
// Add deduplication before load
df.dropDuplicates("event_id")

// Or use Upsert mode
"writeMode": "Upsert",
"upsertKeys": ["event_id"]
```

---

## Docker Environment Issues

### Issue: "Cannot connect to Docker daemon"

**Symptoms:**
```
ERROR: Cannot connect to the Docker daemon. Is the docker daemon running?
```

**Solution:**
```bash
# Start Docker service
sudo systemctl start docker

# Or on Mac/Windows
# Start Docker Desktop

# Verify
docker ps
```

### Issue: Port already in use

**Symptoms:**
```
ERROR: Bind for 0.0.0.0:9092 failed: port is already allocated
```

**Solution:**
```bash
# Find process using port
lsof -i :9092

# Kill process or change port in docker-compose.yml
# Update ports section:
ports:
  - "9093:9092"  # Map to different host port
```

### Issue: Containers keep restarting

**Symptoms:**
```
docker ps shows "Restarting (1) 5 seconds ago"
```

**Solution:**
```bash
# Check container logs
docker logs etl-kafka

# Common issues:
# - Insufficient memory → Increase Docker memory limit
# - Dependency not ready → Add health checks and depends_on
# - Configuration error → Check environment variables

# Restart with fresh state
docker-compose down -v
docker-compose up -d
```

### Issue: Cannot resolve service hostname

**Symptoms:**
```
[ERROR] java.net.UnknownHostException: kafka
```

**Solution:**
```bash
# Use correct hostname based on where code runs:
# - From host machine: localhost:9092
# - From Docker container: kafka:29092

# If running Spark in Docker, ensure it's on same network
docker network ls
docker network inspect etl-network
```

---

## Getting Help

If your issue isn't covered here:

1. **Check logs:**
   ```bash
   # Application logs
   cat logs/etl-pipeline.log

   # Spark logs
   cat logs/driver.log
   cat logs/executor-*.log

   # Docker logs
   docker-compose logs -f
   ```

2. **Enable debug logging:**
   ```json
   "logging": {
     "level": "DEBUG",
     "format": "json"
   }
   ```

3. **Validate configuration:**
   ```bash
   # Use a JSON validator
   cat your-config.json | jq .
   ```

4. **Test components in isolation:**
   ```bash
   # Test extractor only
   # Test transformer only
   # Test loader only
   ```

5. **Check Spark UI** (http://localhost:4040) for:
   - Job stages and tasks
   - Executor status
   - SQL execution plans
   - Environment variables

6. **Create minimal reproducible example:**
   - Use smallest possible dataset
   - Simplify configuration
   - Isolate failing component
