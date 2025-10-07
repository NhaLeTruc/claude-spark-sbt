# Pipeline Configuration Examples

This directory contains example pipeline configurations for common ETL scenarios.

## Configuration Files

### 1. `example-batch-pipeline.json`
**Scenario**: Batch ETL from S3 to PostgreSQL with aggregation

- **Extract**: Read CSV files from S3
- **Transform**: Aggregate events by user (sum amounts, count events)
- **Load**: Upsert aggregated data into PostgreSQL

**Use Case**: Daily batch processing of user event data from data lake to analytics database.

**To Run**:
```bash
spark-submit --class com.etl.Main \
  --master local[4] \
  etl-pipeline-assembly.jar \
  --config src/main/resources/configs/example-batch-pipeline.json \
  --mode batch
```

### 2. `example-streaming-pipeline.json`
**Scenario**: Streaming ETL from Kafka to S3 with windowing

- **Extract**: Stream events from Kafka topic
- **Transform**: Add event sequence numbers per user
- **Load**: Write Parquet files to S3 (partitioned by date)

**Use Case**: Real-time ingestion of user events from Kafka to data lake with transformation.

**To Run**:
```bash
spark-submit --class com.etl.Main \
  --master local[4] \
  etl-pipeline-assembly.jar \
  --config src/main/resources/configs/example-streaming-pipeline.json \
  --mode streaming
```

### 3. `example-mysql-to-kafka.json`
**Scenario**: Batch ETL from MySQL to Kafka with aggregation

- **Extract**: Query transactions from MySQL (last 24 hours)
- **Transform**: Aggregate by user and currency
- **Load**: Publish summaries to Kafka topic

**Use Case**: Periodic extraction of transaction summaries from operational database to event stream.

**To Run**:
```bash
spark-submit --class com.etl.Main \
  --master yarn \
  --deploy-mode cluster \
  etl-pipeline-assembly.jar \
  --config /path/to/example-mysql-to-kafka.json \
  --mode batch
```

## Configuration Schema

All configuration files follow this structure:

```json
{
  "pipelineId": "unique-pipeline-identifier",
  "extract": {
    "sourceType": "Kafka|PostgreSQL|MySQL|S3",
    "connectionParams": { /* source-specific parameters */ }
  },
  "transforms": [
    {
      "transformType": "Aggregation|Window|Join",
      "parameters": { /* transform-specific parameters */ }
    }
  ],
  "load": {
    "sinkType": "Kafka|PostgreSQL|MySQL|S3",
    "writeMode": "Append|Overwrite|Upsert",
    "connectionParams": { /* sink-specific parameters */ }
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
    "level": "INFO|DEBUG|WARN|ERROR",
    "structuredLogging": true
  }
}
```

## Source Types

### Kafka
```json
{
  "sourceType": "Kafka",
  "topic": "topic-name",
  "connectionParams": {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "topic-name",
    "startingOffsets": "earliest|latest",
    "maxOffsetsPerTrigger": "10000"
  }
}
```

### PostgreSQL / MySQL
```json
{
  "sourceType": "PostgreSQL",
  "table": "table_name",
  "query": "SELECT * FROM table WHERE ...",
  "credentialId": "postgres.password",
  "connectionParams": {
    "host": "localhost",
    "port": "5432|3306",
    "database": "db_name",
    "user": "username"
  }
}
```

### S3
```json
{
  "sourceType": "S3",
  "path": "s3a://bucket/path/",
  "connectionParams": {
    "format": "csv|json|parquet|avro",
    "header": "true",
    "fs.s3a.access.key": "ACCESS_KEY",
    "fs.s3a.secret.key": "SECRET_KEY"
  }
}
```

## Transform Types

### Aggregation
```json
{
  "transformType": "Aggregation",
  "parameters": {
    "groupBy": "[\"col1\", \"col2\"]",
    "aggregations": "{\"col3\": \"sum\", \"col4\": \"avg,count\"}"
  }
}
```

### Window
```json
{
  "transformType": "Window",
  "parameters": {
    "partitionBy": "[\"user_id\"]",
    "orderBy": "[\"timestamp DESC\"]",
    "windowFunction": "row_number|rank|lag|lead",
    "outputColumn": "result_column"
  }
}
```

## Sink Types

Same format as sources, plus `writeMode`:
- **Append**: Add new records
- **Overwrite**: Replace all data
- **Upsert**: Update existing, insert new (requires `primaryKey`)

## Credentials

Sensitive credentials (passwords, API keys) should be stored in the credential vault:

1. Create vault file: `vault.enc`
2. Set master key: `export VAULT_MASTER_KEY=your-master-key`
3. Reference in config: `"credentialId": "postgres.password"`

## Notes

- Replace placeholder values (`YOUR_ACCESS_KEY`, etc.) with actual credentials
- Adjust `parallelism` based on cluster size
- For streaming, ensure `checkpointLocation` is set
- Test configurations in local mode before deploying to cluster
