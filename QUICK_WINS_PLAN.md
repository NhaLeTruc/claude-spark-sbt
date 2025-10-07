# Quick Wins Implementation Plan

**Target**: Complete all 8 quick wins in 1-2 days
**Impact**: Improved developer experience, easier testing, better documentation
**Effort**: Low (2-4 hours each)

---

## Overview

These improvements provide immediate value with minimal effort. They enhance the developer experience, make local testing easier, and improve the overall quality of the framework.

**Total Estimated Time**: 28 hours (can be parallelized)

---

## 1. Add Scaladoc Comments ⏱️ 2-3 hours

### Objective
Add comprehensive Scaladoc to all public APIs for auto-generated documentation.

### Files to Document (Priority Order)

#### High Priority (Core Public APIs)
```scala
// 1. Main entry point
src/main/scala/com/etl/Main.scala
  /**
   * Main entry point for ETL pipeline execution.
   *
   * Supports both batch and streaming execution modes with configurable pipelines.
   *
   * @example
   * {{{
   * spark-submit --class com.etl.Main \
   *   etl-pipeline.jar \
   *   --config /path/to/pipeline.json \
   *   --mode batch
   * }}}
   *
   * @see [[com.etl.config.PipelineConfig]] for configuration schema
   * @see [[com.etl.core.ETLPipeline]] for pipeline implementation
   */

// 2. Core traits
src/main/scala/com/etl/core/Pipeline.scala
  /**
   * Core pipeline abstraction for ETL operations.
   *
   * Implementations define the orchestration logic for extract, transform, and load stages.
   * All pipelines must be idempotent and support retry semantics.
   *
   * @example
   * {{{
   * class MyPipeline extends Pipeline {
   *   def run(context: ExecutionContext): PipelineResult = {
   *     // Extract
   *     val df = extractor.extract(context.config.extract)(context.spark)
   *     // Transform
   *     val transformed = transformer.transform(df, context.config.transforms.head)
   *     // Load
   *     val result = loader.load(transformed, context.config.load, WriteMode.Append)
   *     PipelineSuccess(context.metrics)
   *   }
   * }
   * }}}
   */

src/main/scala/com/etl/extract/Extractor.scala
src/main/scala/com/etl/transform/Transformer.scala
src/main/scala/com/etl/load/Loader.scala

// 3. Configuration classes
src/main/scala/com/etl/config/PipelineConfig.scala
  /**
   * Pipeline configuration loaded from JSON.
   *
   * @param pipelineId Unique identifier for the pipeline
   * @param extract Source configuration
   * @param transforms Sequence of transformations to apply
   * @param load Sink configuration
   * @param retry Retry configuration for fault tolerance
   * @param performance Performance tuning parameters
   * @param logging Logging configuration
   *
   * @example
   * {{{
   * {
   *   "pipelineId": "user-events-batch",
   *   "extract": {
   *     "sourceType": "Kafka",
   *     "topic": "events",
   *     "schemaName": "user-event"
   *   },
   *   "transforms": [...],
   *   "load": {...}
   * }
   * }}}
   */
```

#### Medium Priority (Implementations)
```scala
// Extractors
src/main/scala/com/etl/extract/KafkaExtractor.scala
  /**
   * Kafka extractor supporting both batch and streaming modes.
   *
   * ==Streaming Mode==
   * Set `startingOffsets` without `endingOffsets` to enable continuous streaming.
   *
   * ==Batch Mode==
   * Set both `startingOffsets` and `endingOffsets` for bounded read.
   *
   * @example
   * {{{
   * val config = ExtractConfig(
   *   sourceType = SourceType.Kafka,
   *   topic = Some("events"),
   *   connectionParams = Map(
   *     "kafka.bootstrap.servers" -> "localhost:9092",
   *     "startingOffsets" -> "earliest"
   *   )
   * )
   *
   * val extractor = new KafkaExtractor()
   * val df = extractor.extract(config)(spark)
   * }}}
   *
   * @see [[https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html Kafka Integration]]
   */

// Similar for PostgreSQL, MySQL, S3 extractors
```

#### Lower Priority
- Transformers (AggregationTransformer, JoinTransformer, WindowTransformer)
- Loaders (KafkaLoader, PostgreSQLLoader, MySQLLoader, S3Loader)
- Utilities (Retry, Logging)

### Implementation Steps

1. **Create Scaladoc template** (30 min)
   ```scala
   /**
    * Brief one-line description.
    *
    * Detailed description explaining:
    * - What the class/method does
    * - When to use it
    * - Important constraints or requirements
    *
    * @param paramName Description of parameter
    * @return Description of return value
    * @throws ExceptionType When this exception is thrown
    *
    * @example
    * {{{
    * // Example code here
    * }}}
    *
    * @see [[RelatedClass]]
    * @note Important note
    * @since 1.0.0
    */
   ```

2. **Document each file** (90-120 min)
   - Follow priority order
   - Focus on public APIs
   - Include examples for main classes
   - Add @see links to related components

3. **Generate HTML docs** (15 min)
   ```bash
   sbt doc
   # Output: target/scala-2.12/api/index.html
   ```

4. **Review and publish** (15 min)
   - Check generated docs in browser
   - Fix any formatting issues
   - Add link to README

### Deliverables
- ✅ Scaladoc for all 31 main source files
- ✅ Generated HTML documentation
- ✅ README section linking to docs

---

## 2. Create Docker Compose for Local Testing ⏱️ 4 hours

### Objective
Provide one-command local infrastructure for testing pipelines.

### Services to Include
1. **Kafka** (with Zookeeper)
2. **PostgreSQL**
3. **MySQL**
4. **LocalStack** (S3-compatible)
5. **Schema Registry** (for Avro)

### Implementation

**File**: `docker-compose.yml`

```yaml
version: '3.8'

services:
  # Zookeeper for Kafka
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    hostname: zookeeper
    container_name: etl-zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - etl-network

  # Kafka broker
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    hostname: kafka
    container_name: etl-kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "9101:9101"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: 'zookeeper:2181'
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_METRIC_REPORTERS: io.confluent.metrics.reporter.ConfluentMetricsReporter
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_CONFLUENT_METRICS_REPORTER_BOOTSTRAP_SERVERS: kafka:29092
      KAFKA_CONFLUENT_METRICS_REPORTER_TOPIC_REPLICAS: 1
      KAFKA_CONFLUENT_METRICS_ENABLE: 'true'
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
    networks:
      - etl-network

  # Schema Registry
  schema-registry:
    image: confluentinc/cp-schema-registry:7.5.0
    hostname: schema-registry
    container_name: etl-schema-registry
    depends_on:
      - kafka
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: 'kafka:29092'
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    networks:
      - etl-network

  # PostgreSQL
  postgres:
    image: postgres:15-alpine
    container_name: etl-postgres
    ports:
      - "5432:5432"
    environment:
      POSTGRES_USER: etl_user
      POSTGRES_PASSWORD: etl_password
      POSTGRES_DB: etl_test
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./docker/postgres/init.sql:/docker-entrypoint-initdb.d/init.sql
    networks:
      - etl-network
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U etl_user"]
      interval: 10s
      timeout: 5s
      retries: 5

  # MySQL
  mysql:
    image: mysql:8.0
    container_name: etl-mysql
    ports:
      - "3306:3306"
    environment:
      MYSQL_ROOT_PASSWORD: root_password
      MYSQL_DATABASE: etl_test
      MYSQL_USER: etl_user
      MYSQL_PASSWORD: etl_password
    volumes:
      - mysql-data:/var/lib/mysql
      - ./docker/mysql/init.sql:/docker-entrypoint-initdb.d/init.sql
    networks:
      - etl-network
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 10s
      timeout: 5s
      retries: 5

  # LocalStack (S3-compatible)
  localstack:
    image: localstack/localstack:latest
    container_name: etl-localstack
    ports:
      - "4566:4566"  # LocalStack gateway
      - "4510-4559:4510-4559"  # External services port range
    environment:
      SERVICES: s3
      DEBUG: 1
      DATA_DIR: /tmp/localstack/data
      AWS_DEFAULT_REGION: us-east-1
      DOCKER_HOST: unix:///var/run/docker.sock
    volumes:
      - localstack-data:/tmp/localstack
      - /var/run/docker.sock:/var/run/docker.sock
      - ./docker/localstack/init-s3.sh:/etc/localstack/init/ready.d/init-s3.sh
    networks:
      - etl-network

  # Kafka UI (for monitoring)
  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: etl-kafka-ui
    depends_on:
      - kafka
      - schema-registry
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081
    networks:
      - etl-network

networks:
  etl-network:
    driver: bridge

volumes:
  postgres-data:
  mysql-data:
  localstack-data:
```

### Initialization Scripts

**File**: `docker/postgres/init.sql`
```sql
-- Create test tables
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_summary (
    user_id VARCHAR(50) PRIMARY KEY,
    total_events BIGINT,
    total_amount DOUBLE PRECISION,
    last_event_time BIGINT,
    updated_at BIGINT
);

-- Insert sample data
INSERT INTO users (user_id, name, email) VALUES
    ('user1', 'Alice', 'alice@example.com'),
    ('user2', 'Bob', 'bob@example.com'),
    ('user3', 'Charlie', 'charlie@example.com');
```

**File**: `docker/mysql/init.sql`
```sql
-- Create test tables
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50),
    amount DECIMAL(10, 2),
    currency VARCHAR(3),
    transaction_time BIGINT,
    status VARCHAR(20),
    INDEX idx_user_id (user_id)
);

-- Insert sample data
INSERT INTO transactions (transaction_id, user_id, amount, currency, transaction_time, status) VALUES
    ('txn1', 'user1', 100.50, 'USD', UNIX_TIMESTAMP() * 1000, 'COMPLETED'),
    ('txn2', 'user1', 50.25, 'USD', UNIX_TIMESTAMP() * 1000, 'COMPLETED'),
    ('txn3', 'user2', 200.00, 'USD', UNIX_TIMESTAMP() * 1000, 'PENDING');
```

**File**: `docker/localstack/init-s3.sh`
```bash
#!/bin/bash

# Wait for LocalStack to be ready
sleep 5

# Create S3 buckets
awslocal s3 mb s3://etl-input
awslocal s3 mb s3://etl-output
awslocal s3 mb s3://etl-checkpoints

# Upload sample data
cat > /tmp/sample-events.csv << EOF
event_id,user_id,event_type,timestamp,amount
evt1,user1,PAGE_VIEW,1704067200000,
evt2,user1,BUTTON_CLICK,1704067210000,10.5
evt3,user2,PAGE_VIEW,1704067220000,
EOF

awslocal s3 cp /tmp/sample-events.csv s3://etl-input/events/sample.csv

echo "S3 buckets created and sample data uploaded"
```

### Helper Scripts

**File**: `docker/start.sh`
```bash
#!/bin/bash
set -e

echo "Starting ETL test infrastructure..."
docker-compose up -d

echo "Waiting for services to be ready..."
sleep 10

echo "Checking service health..."
docker-compose ps

echo ""
echo "Services available at:"
echo "  Kafka:          localhost:9092"
echo "  Kafka UI:       http://localhost:8080"
echo "  Schema Registry: http://localhost:8081"
echo "  PostgreSQL:     localhost:5432 (user: etl_user, password: etl_password)"
echo "  MySQL:          localhost:3306 (user: etl_user, password: etl_password)"
echo "  LocalStack S3:  http://localhost:4566"
echo ""
echo "To stop: docker-compose down"
echo "To reset data: docker-compose down -v"
```

**File**: `docker/test-pipeline.sh`
```bash
#!/bin/bash
set -e

echo "Testing pipeline with local infrastructure..."

# Set AWS credentials for LocalStack
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

# Run example pipeline
spark-submit \
  --class com.etl.Main \
  --master local[4] \
  target/scala-2.12/claude-spark-etl-1.0.0.jar \
  --config docker/configs/local-test-pipeline.json \
  --mode batch

echo "Pipeline execution complete!"
```

**File**: `docker/configs/local-test-pipeline.json`
```json
{
  "pipelineId": "local-test",
  "extract": {
    "sourceType": "S3",
    "path": "s3a://etl-input/events/sample.csv",
    "schemaName": "user-event",
    "connectionParams": {
      "format": "csv",
      "header": "true",
      "fs.s3a.access.key": "test",
      "fs.s3a.secret.key": "test",
      "fs.s3a.endpoint": "http://localhost:4566",
      "fs.s3a.path.style.access": "true"
    }
  },
  "transforms": [],
  "load": {
    "sinkType": "PostgreSQL",
    "table": "user_summary",
    "writeMode": "Append",
    "connectionParams": {
      "host": "localhost",
      "port": "5432",
      "database": "etl_test",
      "user": "etl_user",
      "password": "etl_password"
    }
  },
  "retry": {
    "maxAttempts": 3,
    "delaySeconds": 5
  },
  "performance": {
    "batchSize": 1000,
    "parallelism": 2
  },
  "logging": {
    "level": "INFO",
    "structuredLogging": true
  }
}
```

### Implementation Steps

1. **Create directory structure** (10 min)
   ```bash
   mkdir -p docker/{postgres,mysql,localstack,configs}
   ```

2. **Create docker-compose.yml** (60 min)
   - Define all services
   - Configure networks and volumes
   - Set up health checks

3. **Create initialization scripts** (45 min)
   - PostgreSQL init.sql
   - MySQL init.sql
   - LocalStack init-s3.sh

4. **Create helper scripts** (30 min)
   - start.sh
   - test-pipeline.sh
   - stop.sh, reset.sh

5. **Create test configuration** (15 min)
   - local-test-pipeline.json

6. **Test infrastructure** (60 min)
   - Start services
   - Verify connectivity
   - Run test pipeline
   - Fix issues

7. **Document in README** (30 min)
   - Quick start guide
   - Service URLs
   - Troubleshooting

### Deliverables
- ✅ docker-compose.yml with 7 services
- ✅ Initialization scripts for databases and S3
- ✅ Helper scripts for common operations
- ✅ Test pipeline configuration
- ✅ Documentation in README

---

## 3. Add More Example Pipelines ⏱️ 2 hours

### Objective
Provide real-world pipeline examples covering common use cases.

### Examples to Create

#### Example 1: Real-time Event Processing
**File**: `src/main/resources/configs/streaming-event-aggregation.json`
```json
{
  "pipelineId": "realtime-event-aggregation",
  "extract": {
    "sourceType": "Kafka",
    "topic": "user-events",
    "schemaName": "user-event",
    "connectionParams": {
      "kafka.bootstrap.servers": "localhost:9092",
      "subscribe": "user-events",
      "startingOffsets": "latest",
      "maxOffsetsPerTrigger": "10000"
    }
  },
  "transforms": [
    {
      "transformType": "Window",
      "parameters": {
        "partitionBy": "[\"user_id\"]",
        "orderBy": "[\"timestamp\"]",
        "windowFunction": "row_number",
        "outputColumn": "event_sequence"
      }
    },
    {
      "transformType": "Aggregation",
      "parameters": {
        "groupBy": "[\"user_id\", \"window(timestamp, '5 minutes')\"]",
        "aggregations": "{\"event_id\": \"count\", \"amount\": \"sum,avg\"}"
      }
    }
  ],
  "load": {
    "sinkType": "Kafka",
    "topic": "event-aggregates",
    "writeMode": "Append",
    "connectionParams": {
      "kafka.bootstrap.servers": "localhost:9092",
      "checkpointLocation": "s3a://etl-checkpoints/event-aggregation/"
    }
  },
  "retry": {
    "maxAttempts": 3,
    "delaySeconds": 5
  },
  "performance": {
    "batchSize": 50000,
    "parallelism": 16
  },
  "logging": {
    "level": "INFO",
    "structuredLogging": true
  }
}
```

#### Example 2: Database CDC (Change Data Capture)
**File**: `src/main/resources/configs/mysql-to-s3-cdc.json`
```json
{
  "pipelineId": "mysql-cdc-to-datalake",
  "extract": {
    "sourceType": "MySQL",
    "query": "SELECT * FROM transactions WHERE updated_at > FROM_UNIXTIME(${checkpoint_timestamp}/1000)",
    "schemaName": "transaction",
    "credentialId": "mysql.production",
    "connectionParams": {
      "host": "mysql.production.internal",
      "port": "3306",
      "database": "commerce",
      "user": "readonly_user",
      "useSSL": "true",
      "serverTimezone": "UTC"
    }
  },
  "transforms": [],
  "load": {
    "sinkType": "S3",
    "path": "s3a://datalake/raw/transactions/",
    "writeMode": "Append",
    "connectionParams": {
      "format": "parquet",
      "compression": "snappy",
      "partitionBy": "[\"DATE_FORMAT(transaction_time, 'yyyy-MM-dd')\"]"
    }
  },
  "retry": {
    "maxAttempts": 5,
    "delaySeconds": 10
  },
  "performance": {
    "batchSize": 100000,
    "parallelism": 32
  },
  "logging": {
    "level": "INFO",
    "structuredLogging": true
  }
}
```

#### Example 3: Multi-Source Join
**File**: `src/main/resources/configs/multi-source-enrichment.json`
```json
{
  "pipelineId": "user-transaction-enrichment",
  "extract": {
    "sourceType": "PostgreSQL",
    "table": "user_events",
    "schemaName": "user-event",
    "credentialId": "postgres.analytics",
    "connectionParams": {
      "host": "postgres.analytics.internal",
      "port": "5432",
      "database": "analytics",
      "user": "etl_reader",
      "numPartitions": "8",
      "partitionColumn": "event_id",
      "lowerBound": "0",
      "upperBound": "10000000"
    }
  },
  "transforms": [
    {
      "transformType": "Join",
      "parameters": {
        "joinType": "left",
        "joinColumns": "[\"user_id\"]",
        "rightDataset": {
          "sourceType": "MySQL",
          "table": "user_profiles",
          "connectionParams": {
            "host": "mysql.users.internal",
            "database": "users"
          }
        }
      }
    }
  ],
  "load": {
    "sinkType": "S3",
    "path": "s3a://analytics/enriched-events/",
    "writeMode": "Overwrite",
    "connectionParams": {
      "format": "avro",
      "compression": "snappy"
    }
  },
  "retry": {
    "maxAttempts": 3,
    "delaySeconds": 5
  },
  "performance": {
    "batchSize": 50000,
    "parallelism": 16
  },
  "logging": {
    "level": "INFO",
    "structuredLogging": true
  }
}
```

#### Example 4: Data Quality Pipeline
**File**: `src/main/resources/configs/data-quality-validation.json`
```json
{
  "pipelineId": "data-quality-check",
  "extract": {
    "sourceType": "S3",
    "path": "s3a://raw-data/incoming/",
    "schemaName": "user-event",
    "connectionParams": {
      "format": "json"
    }
  },
  "transforms": [
    {
      "transformType": "Aggregation",
      "parameters": {
        "groupBy": "[\"event_type\"]",
        "aggregations": "{\"event_id\": \"count\", \"amount\": \"sum,min,max,avg\"}"
      }
    }
  ],
  "load": {
    "sinkType": "PostgreSQL",
    "table": "data_quality_metrics",
    "writeMode": "Append",
    "connectionParams": {
      "host": "localhost",
      "port": "5432",
      "database": "monitoring",
      "user": "metrics_writer"
    },
    "credentialId": "postgres.monitoring"
  },
  "retry": {
    "maxAttempts": 3,
    "delaySeconds": 5
  },
  "performance": {
    "batchSize": 10000,
    "parallelism": 4
  },
  "logging": {
    "level": "DEBUG",
    "structuredLogging": true
  }
}
```

#### Example 5: Incremental Load Pattern
**File**: `src/main/resources/configs/incremental-load.json`
```json
{
  "pipelineId": "incremental-user-summary",
  "extract": {
    "sourceType": "Kafka",
    "topic": "user-events",
    "schemaName": "user-event",
    "connectionParams": {
      "kafka.bootstrap.servers": "localhost:9092",
      "subscribe": "user-events",
      "startingOffsets": "earliest",
      "endingOffsets": "latest"
    }
  },
  "transforms": [
    {
      "transformType": "Aggregation",
      "parameters": {
        "groupBy": "[\"user_id\"]",
        "aggregations": "{\"event_id\": \"count\", \"amount\": \"sum\", \"timestamp\": \"max\"}"
      }
    }
  ],
  "load": {
    "sinkType": "PostgreSQL",
    "table": "user_summary",
    "writeMode": "Upsert",
    "connectionParams": {
      "host": "localhost",
      "port": "5432",
      "database": "analytics",
      "user": "etl_writer",
      "primaryKey": "user_id"
    },
    "credentialId": "postgres.analytics"
  },
  "retry": {
    "maxAttempts": 3,
    "delaySeconds": 5
  },
  "performance": {
    "batchSize": 10000,
    "parallelism": 8
  },
  "logging": {
    "level": "INFO",
    "structuredLogging": true
  }
}
```

### Implementation Steps

1. **Create 5 example configs** (60 min)
   - One for each use case
   - With detailed inline comments
   - Realistic parameters

2. **Create example data** (30 min)
   - Sample CSV/JSON files
   - Kafka test messages script
   - Database seed data

3. **Update configs README** (30 min)
   - Add examples section
   - Explain each use case
   - Add run instructions

### Deliverables
- ✅ 5 new example pipeline configurations
- ✅ Sample data for each example
- ✅ Updated README with use cases

---

## 4. Create Troubleshooting Guide ⏱️ 2 hours

### Objective
Comprehensive guide for common issues and solutions.

**File**: `TROUBLESHOOTING.md`

```markdown
# Troubleshooting Guide

Common issues and solutions for the ETL Pipeline Framework.

## Table of Contents
1. [Build & Compilation](#build--compilation)
2. [Runtime Errors](#runtime-errors)
3. [Source Connectivity](#source-connectivity)
4. [Sink Connectivity](#sink-connectivity)
5. [Performance Issues](#performance-issues)
6. [Data Issues](#data-issues)

---

## Build & Compilation

### Issue: "sbt: command not found"
**Symptoms**: Cannot run `sbt compile`

**Solution**:
```bash
# macOS
brew install sbt

# Linux (Debian/Ubuntu)
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
sudo apt-get update
sudo apt-get install sbt
```

### Issue: "OutOfMemoryError during compilation"
**Symptoms**: SBT crashes with heap space error

**Solution**:
```bash
# Increase SBT memory
export SBT_OPTS="-Xmx4G -Xss2M"
sbt compile
```

---

## Runtime Errors

### Issue: "ClassNotFoundException: org.postgresql.Driver"
**Symptoms**: Pipeline fails with JDBC driver not found

**Solution**:
```bash
# Add JDBC drivers to spark-submit
spark-submit \
  --class com.etl.Main \
  --jars postgresql-42.7.1.jar,mysql-connector-java-8.0.33.jar \
  etl-pipeline.jar \
  --config pipeline.json
```

### Issue: "Pipeline execution failed after 3 attempts"
**Symptoms**: All retries exhausted

**Diagnosis**:
1. Check logs for root cause:
   ```bash
   grep "ERROR" spark-logs/driver.log
   ```

2. Common causes:
   - Network connectivity
   - Invalid credentials
   - Resource exhaustion
   - Data validation failures

**Solutions**:
- Increase retry attempts in config
- Fix underlying issue (credentials, network, etc.)
- Add exponential backoff

---

## Source Connectivity

### Issue: "Kafka connection timeout"
**Symptoms**: `org.apache.kafka.common.errors.TimeoutException`

**Diagnosis**:
```bash
# Test Kafka connectivity
kafka-topics.sh --bootstrap-server localhost:9092 --list

# Check if topic exists
kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic my-topic
```

**Solutions**:
1. Verify bootstrap servers in config
2. Check network firewall rules
3. Verify Kafka is running:
   ```bash
   docker ps | grep kafka
   # or
   systemctl status kafka
   ```

4. Test with kafkacat:
   ```bash
   kafkacat -b localhost:9092 -L
   ```

### Issue: "PostgreSQL authentication failed"
**Symptoms**: `FATAL: password authentication failed for user`

**Solutions**:
1. Check credentials:
   ```bash
   psql -h localhost -U etl_user -d analytics
   ```

2. Verify credential vault:
   ```bash
   export VAULT_MASTER_KEY=your-key
   # Check if credential exists
   ```

3. Update pg_hba.conf if needed

### Issue: "S3 Access Denied"
**Symptoms**: `Status Code: 403, Error Code: AccessDenied`

**Diagnosis**:
```bash
# Test S3 access
aws s3 ls s3://my-bucket --profile etl

# Check IAM permissions
aws iam get-user --profile etl
```

**Solutions**:
1. Verify credentials in config
2. Check bucket policy
3. Verify IAM role permissions
4. For LocalStack:
   ```bash
   export AWS_ACCESS_KEY_ID=test
   export AWS_SECRET_ACCESS_KEY=test
   ```

---

## Sink Connectivity

### Issue: "Table does not exist"
**Symptoms**: `ERROR: relation "user_summary" does not exist`

**Solutions**:
```sql
-- Create table
CREATE TABLE user_summary (
    user_id VARCHAR(50) PRIMARY KEY,
    total_events BIGINT,
    total_amount DOUBLE PRECISION,
    last_event_time BIGINT,
    updated_at BIGINT
);
```

### Issue: "Upsert not working"
**Symptoms**: Duplicate key violations or upsert silently fails

**Diagnosis**:
- Check logs for warnings about upsert implementation
- Verify primaryKey is set in config
- Check if table has primary key constraint

**Current Limitation**:
Upsert is partially implemented. It logs a warning but doesn't execute the merge SQL.

**Workaround**:
Use Overwrite mode or implement application-level deduplication.

---

## Performance Issues

### Issue: "Pipeline is slow (< 1K records/sec)"
**Diagnosis**:
1. Check Spark UI (http://localhost:4040)
   - Stage execution times
   - Task distribution
   - Shuffle read/write

2. Check executor logs for GC pressure

**Solutions**:

**Increase parallelism**:
```json
{
  "performance": {
    "parallelism": 32,  // Increase from 4
    "batchSize": 50000  // Increase batch size
  }
}
```

**Tune Spark parameters**:
```bash
spark-submit \
  --executor-memory 8G \
  --executor-cores 4 \
  --num-executors 10 \
  --conf spark.sql.shuffle.partitions=200 \
  ...
```

**Enable compression**:
```json
{
  "load": {
    "connectionParams": {
      "compression": "snappy"
    }
  }
}
```

### Issue: "OutOfMemoryError during execution"
**Symptoms**: Executor crashes with OOM

**Solutions**:

**Increase memory**:
```bash
spark-submit \
  --executor-memory 16G \
  --driver-memory 8G \
  ...
```

**Reduce batch size**:
```json
{
  "performance": {
    "batchSize": 10000  // Reduce from 100000
  }
}
```

**Add caching strategically** (requires code change):
```scala
val df = extractor.extract(config)
df.cache()  // Cache if reused
```

---

## Data Issues

### Issue: "Schema validation failed"
**Symptoms**: `ValidationResult.isValid = false`

**Diagnosis**:
```scala
// Check validation errors
val result = SchemaValidator.validate(df, "user-event")
result.errors.foreach(println)
```

**Common causes**:
1. Missing required fields
2. Type mismatches
3. Wrong schema name

**Solutions**:
- Fix source data to match schema
- Update Avro schema if intentional
- Add transformations to fix schema

### Issue: "Null values in non-nullable columns"
**Symptoms**: Data quality issues or sink errors

**Solutions**:

**Filter nulls**:
```json
{
  "transforms": [{
    "transformType": "Aggregation",
    "parameters": {
      "filterCondition": "user_id IS NOT NULL"
    }
  }]
}
```

**Or fill with defaults** (requires code change)

---

## Logging & Debugging

### Enable Debug Logging
```json
{
  "logging": {
    "level": "DEBUG",
    "structuredLogging": true
  }
}
```

### View Structured Logs
```bash
# Pretty-print JSON logs
cat spark-logs/driver.log | jq '.'

# Filter by pipeline ID
cat spark-logs/driver.log | jq 'select(.pipelineId == "my-pipeline")'

# Filter errors
cat spark-logs/driver.log | jq 'select(.level == "ERROR")'
```

### Monitor Pipeline Execution
```bash
# Tail logs in real-time
tail -f spark-logs/driver.log | jq '.'

# Watch metrics
watch -n 1 'cat spark-logs/driver.log | jq "select(.recordsLoaded)" | tail -1'
```

---

## Getting Help

If issues persist:

1. **Check GitHub Issues**: [github.com/yourorg/etl-pipeline/issues](https://github.com/yourorg/etl-pipeline/issues)
2. **Review Documentation**: See [README.md](README.md) and [specs/](specs/)
3. **Enable Debug Logs**: Set `logging.level` to `DEBUG`
4. **Collect Diagnostics**:
   ```bash
   # Spark UI
   # Executor logs
   # Pipeline config
   # Schema files
   # Sample problematic data
   ```

5. **Create Issue** with:
   - Pipeline configuration
   - Error messages and stack traces
   - Steps to reproduce
   - Environment details (Spark version, cluster type)
```

### Implementation Steps

1. **Create troubleshooting doc** (90 min)
2. **Test each solution** (30 min)
3. **Add to README** (15 min)

### Deliverables
- ✅ TROUBLESHOOTING.md with 20+ common issues
- ✅ Link in README

---

## 5. Add Input Validation to Transformers ⏱️ 4 hours

### Objective
Validate transformer inputs to fail fast with clear errors.

### Validation to Add

#### AggregationTransformer Validation
```scala
// src/main/scala/com/etl/transform/AggregationTransformer.scala

override def transform(df: DataFrame, config: TransformConfig): DataFrame = {
  // Validate config
  validateConfig(config)

  // Validate DataFrame has groupBy columns
  validateGroupByColumns(df, groupByCols)

  // Validate aggregation columns exist
  validateAggregationColumns(df, aggregationsMap)

  // Validate aggregation functions are valid
  validateAggregationFunctions(aggregationsMap)

  // Proceed with transformation
  ...
}

private def validateConfig(config: TransformConfig): Unit = {
  require(
    config.parameters.contains("groupBy"),
    "groupBy parameter is required"
  )
  require(
    config.parameters.contains("aggregations"),
    "aggregations parameter is required"
  )
}

private def validateGroupByColumns(df: DataFrame, columns: Seq[String]): Unit = {
  val dfColumns = df.schema.fieldNames.toSet
  val missingColumns = columns.filterNot(dfColumns.contains)

  require(
    missingColumns.isEmpty,
    s"GroupBy columns not found in DataFrame: ${missingColumns.mkString(", ")}"
  )
}

private def validateAggregationColumns(df: DataFrame, aggs: Map[String, String]): Unit = {
  val dfColumns = df.schema.fieldNames.toSet
  val missingColumns = aggs.keys.filterNot(dfColumns.contains)

  require(
    missingColumns.isEmpty,
    s"Aggregation columns not found in DataFrame: ${missingColumns.mkString(", ")}"
  )
}

private def validateAggregationFunctions(aggs: Map[String, String]): Unit = {
  val validFunctions = Set("sum", "avg", "min", "max", "count", "first", "last")

  aggs.foreach { case (column, functions) =>
    val funcs = functions.split(",").map(_.trim.toLowerCase)
    val invalidFuncs = funcs.filterNot(validFunctions.contains)

    require(
      invalidFuncs.isEmpty,
      s"Invalid aggregation functions for column $column: ${invalidFuncs.mkString(", ")}. " +
      s"Valid functions: ${validFunctions.mkString(", ")}"
    )
  }
}
```

#### JoinTransformer Validation
```scala
// src/main/scala/com/etl/transform/JoinTransformer.scala

override def transform(df: DataFrame, config: TransformConfig): DataFrame = {
  // Validate join type
  validateJoinType(config)

  // Validate join columns exist in both DataFrames
  validateJoinColumns(df, rightDf, joinColumns)

  // Warn about potential cross joins
  warnIfCrossJoin(df, rightDf, joinColumns)

  // Proceed with join
  ...
}

private def validateJoinType(config: TransformConfig): Unit = {
  val validJoinTypes = Set("inner", "left", "right", "outer", "left_semi", "left_anti", "cross")
  val joinType = config.parameters("joinType").toLowerCase

  require(
    validJoinTypes.contains(joinType),
    s"Invalid join type: $joinType. Valid types: ${validJoinTypes.mkString(", ")}"
  )
}

private def validateJoinColumns(
  left: DataFrame,
  right: DataFrame,
  columns: Seq[String]
): Unit = {
  val leftColumns = left.schema.fieldNames.toSet
  val rightColumns = right.schema.fieldNames.toSet

  columns.foreach { col =>
    require(
      leftColumns.contains(col),
      s"Join column '$col' not found in left DataFrame"
    )
    require(
      rightColumns.contains(col),
      s"Join column '$col' not found in right DataFrame"
    )
  }
}

private def warnIfCrossJoin(
  left: DataFrame,
  right: DataFrame,
  columns: Seq[String]
): Unit = {
  if (columns.isEmpty) {
    logger.warn(
      s"No join columns specified. This will result in a cross join. " +
      s"Left rows: ${left.count()}, Right rows: ${right.count()}"
    )
  }
}
```

#### WindowTransformer Validation
```scala
// src/main/scala/com/etl/transform/WindowTransformer.scala

override def transform(df: DataFrame, config: TransformConfig): DataFrame = {
  // Validate required parameters
  validateRequiredParameters(config)

  // Validate columns exist
  validateColumns(df, config)

  // Validate window function
  validateWindowFunction(config)

  // Validate aggregate column for functions that need it
  validateAggregateColumn(df, config)

  // Proceed with transformation
  ...
}

private def validateRequiredParameters(config: TransformConfig): Unit = {
  val required = Seq("partitionBy", "orderBy", "windowFunction", "outputColumn")
  val missing = required.filterNot(config.parameters.contains)

  require(
    missing.isEmpty,
    s"Missing required parameters: ${missing.mkString(", ")}"
  )
}

private def validateColumns(df: DataFrame, config: TransformConfig): Unit = {
  val dfColumns = df.schema.fieldNames.toSet

  // Validate partition columns
  val partitionCols = Json.parse(config.parameters("partitionBy")).as[Seq[String]]
  val missingPartition = partitionCols.filterNot(dfColumns.contains)
  require(
    missingPartition.isEmpty,
    s"Partition columns not found: ${missingPartition.mkString(", ")}"
  )

  // Validate order columns (strip DESC/ASC)
  val orderSpecs = Json.parse(config.parameters("orderBy")).as[Seq[String]]
  val orderCols = orderSpecs.map(_.split("\\s+").head)
  val missingOrder = orderCols.filterNot(dfColumns.contains)
  require(
    missingOrder.isEmpty,
    s"Order columns not found: ${missingOrder.mkString(", ")}"
  )

  // Validate aggregate column if present
  config.parameters.get("aggregateColumn").foreach { col =>
    require(
      dfColumns.contains(col),
      s"Aggregate column not found: $col"
    )
  }
}

private def validateWindowFunction(config: TransformConfig): Unit = {
  val validFunctions = Set(
    "row_number", "rank", "dense_rank",
    "sum", "avg", "min", "max", "count",
    "lag", "lead"
  )

  val function = config.parameters("windowFunction").toLowerCase
  require(
    validFunctions.contains(function),
    s"Invalid window function: $function. Valid: ${validFunctions.mkString(", ")}"
  )
}

private def validateAggregateColumn(df: DataFrame, config: TransformConfig): Unit = {
  val function = config.parameters("windowFunction").toLowerCase
  val needsAggCol = Set("sum", "avg", "min", "max", "count", "lag", "lead")

  if (needsAggCol.contains(function)) {
    require(
      config.parameters.contains("aggregateColumn"),
      s"Window function '$function' requires 'aggregateColumn' parameter"
    )
  }
}
```

### Create Validation Tests
```scala
// src/test/scala/unit/transform/AggregationTransformerValidationSpec.scala

class AggregationTransformerValidationSpec extends AnyFlatSpec with Matchers {

  "AggregationTransformer" should "fail when groupBy parameter is missing" in {
    val config = TransformConfig(
      transformType = TransformType.Aggregation,
      parameters = Map("aggregations" -> "{\"amount\": \"sum\"}")
    )

    val transformer = new AggregationTransformer()

    an[IllegalArgumentException] should be thrownBy {
      transformer.transform(df, config)
    }
  }

  it should "fail when groupBy column doesn't exist in DataFrame" in {
    val config = TransformConfig(
      transformType = TransformType.Aggregation,
      parameters = Map(
        "groupBy" -> "[\"nonexistent_column\"]",
        "aggregations" -> "{\"amount\": \"sum\"}"
      )
    )

    val transformer = new AggregationTransformer()

    val exception = the[IllegalArgumentException] thrownBy {
      transformer.transform(df, config)
    }

    exception.getMessage should include("not found in DataFrame")
  }

  it should "fail when aggregation function is invalid" in {
    val config = TransformConfig(
      transformType = TransformType.Aggregation,
      parameters = Map(
        "groupBy" -> "[\"user_id\"]",
        "aggregations" -> "{\"amount\": \"invalid_function\"}"
      )
    )

    val transformer = new AggregationTransformer()

    val exception = the[IllegalArgumentException] thrownBy {
      transformer.transform(df, config)
    }

    exception.getMessage should include("Invalid aggregation function")
  }
}
```

### Implementation Steps

1. **Add validation to each transformer** (120 min)
2. **Create validation tests** (60 min)
3. **Update documentation** (15 min)

### Deliverables
- ✅ Input validation in 3 transformers
- ✅ 15+ validation test cases
- ✅ Better error messages

---

## 6. Implement Graceful Shutdown ⏱️ 4 hours

### Objective
Handle SIGTERM/SIGINT gracefully, allowing in-flight operations to complete.

### Implementation

**File**: `src/main/scala/com/etl/core/GracefulShutdown.scala`
```scala
package com.etl.core

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
 * Manages graceful shutdown of the ETL pipeline.
 *
 * Handles SIGTERM and SIGINT signals, allowing running operations to complete
 * before terminating. Ensures proper cleanup of resources.
 */
class GracefulShutdown(spark: SparkSession) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val shutdownPromise = Promise[Unit]()
  @volatile private var shuttingDown = false

  /**
   * Register shutdown hooks for SIGTERM and SIGINT.
   */
  def registerShutdownHook(): Unit = {
    val shutdownThread = new Thread("shutdown-hook") {
      override def run(): Unit = {
        logger.info("Shutdown signal received, initiating graceful shutdown...")
        initiateShutdown()
      }
    }

    Runtime.getRuntime.addShutdownHook(shutdownThread)
    logger.info("Shutdown hook registered")
  }

  /**
   * Check if shutdown has been requested.
   */
  def isShuttingDown: Boolean = shuttingDown

  /**
   * Wait for shutdown signal.
   *
   * @param timeoutMs Maximum time to wait in milliseconds
   * @return true if shutdown was signaled, false if timeout
   */
  def awaitShutdown(timeoutMs: Long = Long.MaxValue): Boolean = {
    import scala.concurrent.duration._
    import scala.concurrent.Await

    try {
      Await.ready(shutdownPromise.future, timeoutMs.milliseconds)
      true
    } catch {
      case _: java.util.concurrent.TimeoutException => false
    }
  }

  /**
   * Initiate graceful shutdown sequence.
   */
  private def initiateShutdown(): Unit = {
    if (!shuttingDown) {
      shuttingDown = true

      logger.info("Step 1/3: Stopping new pipeline executions...")
      // Mark as shutting down to prevent new work

      logger.info("Step 2/3: Waiting for in-flight operations to complete...")
      // Wait for current pipelines (up to 5 minutes)
      Thread.sleep(5000) // Simple implementation

      logger.info("Step 3/3: Stopping Spark session...")
      if (!spark.sparkContext.isStopped) {
        spark.stop()
        logger.info("Spark session stopped")
      }

      shutdownPromise.success(())
      logger.info("Graceful shutdown complete")
    }
  }

  /**
   * Force immediate shutdown (for emergencies).
   */
  def forceShutdown(): Unit = {
    logger.warn("Force shutdown requested!")
    spark.stop()
    shutdownPromise.success(())
  }
}

object GracefulShutdown {
  def apply(spark: SparkSession): GracefulShutdown = new GracefulShutdown(spark)
}
```

**Update Main.scala**:
```scala
// src/main/scala/com/etl/Main.scala

object Main {
  def main(args: Array[String]): Unit = {
    logger.info("Starting ETL Pipeline Application")

    try {
      val parsedArgs = parseArguments(args)
      val configPath = parsedArgs.getOrElse("config", {
        logger.error("Missing required argument: --config")
        printUsage()
        System.exit(1)
        ""
      })

      val mode = parsedArgs.getOrElse("mode", "batch")
      val config = loadConfiguration(configPath)
      val spark = initializeSparkSession(config, mode)

      // Register graceful shutdown
      val shutdownHandler = GracefulShutdown(spark)
      shutdownHandler.registerShutdownHook()

      try {
        val pipeline = buildPipeline(config)
        val executor = new PipelineExecutor()

        // Execute with shutdown awareness
        val result = if (shutdownHandler.isShuttingDown) {
          logger.warn("Shutdown requested before pipeline start, exiting")
          System.exit(0)
          null // Never reached
        } else {
          executor.execute(pipeline, config)(spark)
        }

        if (result.isSuccess) {
          logger.info(s"Pipeline completed successfully")
          System.exit(0)
        } else {
          logger.error(s"Pipeline failed")
          System.exit(1)
        }
      } finally {
        // Cleanup happens in shutdown hook
        logger.info("Pipeline execution ended")
      }

    } catch {
      case e: Exception =>
        logger.error(s"Fatal error: ${e.getMessage}", e)
        System.exit(1)
    }
  }
}
```

**Create tests**:
```scala
// src/test/scala/unit/core/GracefulShutdownSpec.scala

class GracefulShutdownSpec extends AnyFlatSpec with Matchers {

  "GracefulShutdown" should "handle shutdown signal" in {
    val spark = SparkSession.builder()
      .appName("ShutdownTest")
      .master("local[2]")
      .getOrCreate()

    val shutdown = GracefulShutdown(spark)
    shutdown.registerShutdownHook()

    shutdown.isShuttingDown shouldBe false

    // Simulate shutdown
    // (actual signal testing requires integration test)

    shutdown.forceShutdown()
    shutdown.isShuttingDown shouldBe true
  }

  it should "complete within timeout" in {
    val spark = SparkSession.builder()
      .appName("ShutdownTimeoutTest")
      .master("local[2]")
      .getOrCreate()

    val shutdown = GracefulShutdown(spark)

    // Should timeout waiting for shutdown signal
    val completed = shutdown.awaitShutdown(timeoutMs = 100)
    completed shouldBe false
  }
}
```

### Implementation Steps

1. **Create GracefulShutdown class** (90 min)
2. **Integrate with Main** (45 min)
3. **Add tests** (30 min)
4. **Manual testing** (45 min)
5. **Documentation** (30 min)

### Deliverables
- ✅ GracefulShutdown implementation
- ✅ Integration with Main.scala
- ✅ Unit tests
- ✅ Documentation in README

---

## 7. Add Health Check Endpoint ⏱️ 3 hours

### Objective
HTTP endpoint for monitoring pipeline health and readiness.

### Implementation

**File**: `src/main/scala/com/etl/monitoring/HealthCheck.scala`
```scala
package com.etl.monitoring

import com.etl.config.PipelineConfig
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.slf4j.LoggerFactory
import play.api.libs.json.{Json, Writes}

import java.net.InetSocketAddress
import scala.util.{Failure, Success, Try}

/**
 * Health check status for a component.
 */
sealed trait HealthStatus {
  def isHealthy: Boolean
  def message: String
}

case class Healthy(message: String = "OK") extends HealthStatus {
  val isHealthy = true
}

case class Unhealthy(message: String) extends HealthStatus {
  val isHealthy = false
}

/**
 * Health check result.
 */
case class HealthCheckResult(
  status: String,
  checks: Map[String, ComponentHealth],
  timestamp: Long = System.currentTimeMillis()
)

case class ComponentHealth(
  healthy: Boolean,
  message: String,
  details: Map[String, String] = Map.empty
)

/**
 * HTTP server providing health check endpoints.
 */
class HealthCheckServer(port: Int = 8888, config: PipelineConfig) {
  private val logger = LoggerFactory.getLogger(getClass)
  private var server: Option[HttpServer] = None

  implicit val componentHealthWrites: Writes[ComponentHealth] = Json.writes[ComponentHealth]
  implicit val healthCheckResultWrites: Writes[HealthCheckResult] = Json.writes[HealthCheckResult]

  /**
   * Start health check server.
   */
  def start(): Unit = {
    Try {
      val httpServer = HttpServer.create(new InetSocketAddress(port), 0)

      // Liveness probe (is the app running?)
      httpServer.createContext("/health/live", new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          handleLiveness(exchange)
        }
      })

      // Readiness probe (is the app ready to accept traffic?)
      httpServer.createContext("/health/ready", new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          handleReadiness(exchange)
        }
      })

      // Detailed health check
      httpServer.createContext("/health", new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          handleHealthCheck(exchange)
        }
      })

      httpServer.setExecutor(null) // Default executor
      httpServer.start()

      server = Some(httpServer)
      logger.info(s"Health check server started on port $port")
    } match {
      case Success(_) => ()
      case Failure(e) =>
        logger.error(s"Failed to start health check server: ${e.getMessage}", e)
    }
  }

  /**
   * Stop health check server.
   */
  def stop(): Unit = {
    server.foreach { s =>
      s.stop(0)
      logger.info("Health check server stopped")
    }
    server = None
  }

  private def handleLiveness(exchange: HttpExchange): Unit = {
    // Simple check: is the JVM running?
    val response = """{"status":"UP"}"""
    sendResponse(exchange, 200, response)
  }

  private def handleReadiness(exchange: HttpExchange): Unit = {
    // Check if system is ready to process pipelines
    val checks = performReadinessChecks()
    val allHealthy = checks.values.forall(_.healthy)

    val result = HealthCheckResult(
      status = if (allHealthy) "UP" else "DOWN",
      checks = checks
    )

    val statusCode = if (allHealthy) 200 else 503
    val response = Json.toJson(result).toString()

    sendResponse(exchange, statusCode, response)
  }

  private def handleHealthCheck(exchange: HttpExchange): Unit = {
    val checks = performAllHealthChecks()
    val allHealthy = checks.values.forall(_.healthy)

    val result = HealthCheckResult(
      status = if (allHealthy) "UP" else "DOWN",
      checks = checks
    )

    val statusCode = if (allHealthy) 200 else 503
    val response = Json.toJson(result).toString()

    sendResponse(exchange, statusCode, response)
  }

  private def performReadinessChecks(): Map[String, ComponentHealth] = {
    Map(
      "app" -> ComponentHealth(
        healthy = true,
        message = "Application is running"
      )
    )
  }

  private def performAllHealthChecks(): Map[String, ComponentHealth] = {
    Map(
      "app" -> checkApplication(),
      "config" -> checkConfiguration(),
      "spark" -> checkSpark(),
      "source" -> checkSource(),
      "sink" -> checkSink()
    )
  }

  private def checkApplication(): ComponentHealth = {
    ComponentHealth(
      healthy = true,
      message = "Application is running",
      details = Map(
        "uptime" -> s"${Runtime.getRuntime.freeMemory() / 1024 / 1024} MB"
      )
    )
  }

  private def checkConfiguration(): ComponentHealth = {
    ComponentHealth(
      healthy = config != null,
      message = if (config != null) "Configuration loaded" else "No configuration",
      details = Map(
        "pipelineId" -> config.pipelineId
      )
    )
  }

  private def checkSpark(): ComponentHealth = {
    // Check if Spark session is available
    ComponentHealth(
      healthy = true,
      message = "Spark context available"
    )
  }

  private def checkSource(): ComponentHealth = {
    // In a real implementation, test source connectivity
    ComponentHealth(
      healthy = true,
      message = s"Source configured: ${config.extract.sourceType}"
    )
  }

  private def checkSink(): ComponentHealth = {
    // In a real implementation, test sink connectivity
    ComponentHealth(
      healthy = true,
      message = s"Sink configured: ${config.load.sinkType}"
    )
  }

  private def sendResponse(exchange: HttpExchange, statusCode: Int, response: String): Unit = {
    val bytes = response.getBytes("UTF-8")
    exchange.getResponseHeaders.add("Content-Type", "application/json")
    exchange.sendResponseHeaders(statusCode, bytes.length)
    val os = exchange.getResponseBody
    os.write(bytes)
    os.close()
  }
}
```

**Update Main.scala**:
```scala
object Main {
  def main(args: Array[String]): Unit = {
    // ... existing code ...

    val config = loadConfiguration(configPath)
    val spark = initializeSparkSession(config, mode)

    // Start health check server
    val healthServer = new HealthCheckServer(port = 8888, config)
    healthServer.start()

    try {
      // ... execute pipeline ...
    } finally {
      healthServer.stop()
      spark.stop()
    }
  }
}
```

### Test with curl
```bash
# Liveness probe
curl http://localhost:8888/health/live
# {"status":"UP"}

# Readiness probe
curl http://localhost:8888/health/ready
# {"status":"UP","checks":{...}}

# Full health check
curl http://localhost:8888/health
# {"status":"UP","checks":{"app":{...},"config":{...},...}}
```

### Implementation Steps

1. **Create HealthCheckServer** (90 min)
2. **Integrate with Main** (30 min)
3. **Add connectivity checks** (45 min)
4. **Manual testing** (30 min)
5. **Documentation** (15 min)

### Deliverables
- ✅ Health check HTTP server
- ✅ 3 endpoints (live, ready, health)
- ✅ Integration with Main
- ✅ Documentation

---

## 8. Create Unit Test Helpers ⏱️ 4 hours

### Objective
Reusable test utilities to simplify writing tests.

**File**: `src/test/scala/helpers/TestHelpers.scala`
```scala
package com.etl.helpers

import com.etl.config._
import com.etl.model._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import java.util.UUID

/**
 * Test helpers for creating test data and configurations.
 */
object TestHelpers {

  /**
   * Create a test PipelineConfig with minimal required fields.
   */
  def createTestConfig(
    pipelineId: String = "test-pipeline",
    sourceType: SourceType = SourceType.Kafka,
    sinkType: SinkType = SinkType.S3,
    retryAttempts: Int = 3
  ): PipelineConfig = {
    PipelineConfig(
      pipelineId = pipelineId,
      extract = ExtractConfig(
        sourceType = sourceType,
        schemaName = "test-schema",
        connectionParams = Map("test" -> "value")
      ),
      transforms = Seq.empty,
      load = LoadConfig(
        sinkType = sinkType,
        writeMode = WriteMode.Append,
        connectionParams = Map("test" -> "value")
      ),
      retry = RetryConfig(maxAttempts = retryAttempts, delaySeconds = 1),
      performance = PerformanceConfig(batchSize = 1000, parallelism = 2),
      logging = LoggingConfig(level = "ERROR", structuredLogging = false)
    )
  }

  /**
   * Create test ExecutionMetrics.
   */
  def createTestMetrics(
    pipelineId: String = "test-pipeline",
    recordsExtracted: Long = 0,
    recordsLoaded: Long = 0
  ): ExecutionMetrics = {
    ExecutionMetrics.initial(pipelineId, UUID.randomUUID().toString)
      .copy(
        recordsExtracted = recordsExtracted,
        recordsLoaded = recordsLoaded
      )
  }

  /**
   * Create a simple test DataFrame.
   */
  def createTestDataFrame(spark: SparkSession, rows: Seq[Row], schema: StructType): DataFrame = {
    spark.createDataFrame(
      spark.sparkContext.parallelize(rows),
      schema
    )
  }

  /**
   * Create test DataFrame with user events.
   */
  def createUserEventsDataFrame(spark: SparkSession, count: Int = 10): DataFrame = {
    import spark.implicits._

    (1 to count).map { i =>
      (s"event$i", s"user${i % 3}", "BUTTON_CLICK", System.currentTimeMillis(), i * 10.0)
    }.toDF("event_id", "user_id", "event_type", "timestamp", "amount")
  }

  /**
   * Create test DataFrame with transactions.
   */
  def createTransactionsDataFrame(spark: SparkSession, count: Int = 10): DataFrame = {
    import spark.implicits._

    (1 to count).map { i =>
      (s"txn$i", s"user${i % 3}", i * 100.0, "USD", System.currentTimeMillis(), "COMPLETED")
    }.toDF("transaction_id", "user_id", "amount", "currency", "transaction_time", "status")
  }

  /**
   * Assert DataFrames are equal (ignoring order).
   */
  def assertDataFrameEquals(actual: DataFrame, expected: DataFrame): Unit = {
    import org.scalatest.matchers.should.Matchers._

    // Check schema
    actual.schema shouldBe expected.schema

    // Check row count
    actual.count() shouldBe expected.count()

    // Check data (order-independent)
    val actualRows = actual.collect().sortBy(_.toString())
    val expectedRows = expected.collect().sortBy(_.toString())

    actualRows shouldBe expectedRows
  }

  /**
   * Create a temporary directory for testing.
   */
  def createTempDir(): java.io.File = {
    val dir = java.nio.file.Files.createTempDirectory("etl-test-").toFile
    dir.deleteOnExit()
    dir
  }

  /**
   * Write DataFrame to temporary location and return path.
   */
  def writeTempParquet(df: DataFrame): String = {
    val path = createTempDir().getAbsolutePath + "/data.parquet"
    df.write.parquet(path)
    path
  }

  /**
   * Create a mock Extract config.
   */
  def mockExtractConfig(sourceType: SourceType = SourceType.Kafka): ExtractConfig = {
    ExtractConfig(
      sourceType = sourceType,
      schemaName = "test-schema",
      connectionParams = Map(
        "test" -> "value",
        "kafka.bootstrap.servers" -> "localhost:9092",
        "topic" -> "test-topic"
      )
    )
  }

  /**
   * Create a mock Transform config.
   */
  def mockTransformConfig(transformType: TransformType = TransformType.Aggregation): TransformConfig = {
    TransformConfig(
      transformType = transformType,
      parameters = Map(
        "groupBy" -> "[\"user_id\"]",
        "aggregations" -> "{\"amount\": \"sum\"}"
      )
    )
  }

  /**
   * Create a mock Load config.
   */
  def mockLoadConfig(sinkType: SinkType = SinkType.S3): LoadConfig = {
    LoadConfig(
      sinkType = sinkType,
      writeMode = WriteMode.Append,
      connectionParams = Map(
        "path" -> "s3a://test-bucket/output",
        "format" -> "parquet"
      )
    )
  }
}
```

**Usage Example**:
```scala
import com.etl.helpers.TestHelpers._

class MySpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  implicit var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("Test")
      .master("local[2]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
  }

  "MyComponent" should "process data correctly" in {
    // Use helpers to create test data
    val df = createUserEventsDataFrame(spark, count = 100)
    val config = createTestConfig(sourceType = SourceType.Kafka)

    // Test component
    val result = myComponent.process(df, config)

    // Assert results
    result.count() should be > 0L
  }
}
```

### Implementation Steps

1. **Create TestHelpers object** (90 min)
2. **Add data generators** (60 min)
3. **Add assertion helpers** (30 min)
4. **Refactor existing tests to use helpers** (60 min)
5. **Documentation** (30 min)

### Deliverables
- ✅ TestHelpers with 15+ utility methods
- ✅ Refactored tests using helpers
- ✅ Documentation

---

## Implementation Timeline

### Day 1 (8 hours)
- **Morning** (4 hours)
  - 1. Scaladoc comments (2-3 hours)
  - 2. Docker compose (start) (1 hour)

- **Afternoon** (4 hours)
  - 2. Docker compose (finish) (3 hours)
  - 3. Example pipelines (1 hour)

### Day 2 (8 hours)
- **Morning** (4 hours)
  - 3. Example pipelines (finish) (1 hour)
  - 4. Troubleshooting guide (2 hours)
  - 5. Input validation (start) (1 hour)

- **Afternoon** (4 hours)
  - 5. Input validation (finish) (3 hours)
  - 6. Graceful shutdown (start) (1 hour)

### Day 3 (Optional - 12 hours)
- **Morning** (4 hours)
  - 6. Graceful shutdown (finish) (3 hours)
  - 7. Health check (start) (1 hour)

- **Afternoon** (4 hours)
  - 7. Health check (finish) (2 hours)
  - 8. Test helpers (start) (2 hours)

- **Evening** (4 hours)
  - 8. Test helpers (finish) (2 hours)
  - Final testing and documentation (2 hours)

---

## Success Criteria

- ✅ All 8 features implemented
- ✅ Tests pass
- ✅ Documentation updated
- ✅ Examples work end-to-end
- ✅ Docker infrastructure tested
- ✅ Health checks responding
- ✅ Validation catching errors early

---

## Next Steps After Quick Wins

Once quick wins are complete, prioritize:

1. **Integration Tests** (from IMPROVEMENT_ROADMAP.md)
2. **Performance Benchmarks**
3. **Credential Security Hardening**
4. **Circuit Breaker & DLQ**
