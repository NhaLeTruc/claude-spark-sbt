# Docker Environment for ETL Pipeline Testing

Complete local development and testing environment for the ETL pipeline framework.

## Services

The Docker Compose setup includes:

- **Kafka + Zookeeper**: Message streaming platform (ports 9092, 2181)
- **Schema Registry**: Avro schema management (port 8081)
- **Kafka UI**: Web interface for Kafka management (port 8080)
- **PostgreSQL**: Relational database (port 5432)
- **MySQL**: Relational database (port 3306)
- **LocalStack**: AWS S3 emulation (port 4566)

## Quick Start

### 1. Start Infrastructure

```bash
./docker/scripts/start-infrastructure.sh
```

This script will:
- Start all services via docker-compose
- Wait for all services to become healthy
- Create Kafka topics (user-events, user-summary, transactions)
- Initialize databases with sample tables and data
- Display service URLs and credentials

### 2. Test Pipeline

```bash
./docker/scripts/test-pipeline.sh
```

This script will:
- Build the project with `sbt assembly`
- Submit a test pipeline to Spark using example configuration
- Show execution results

### 3. Stop Infrastructure

```bash
./docker/scripts/stop-infrastructure.sh
```

To remove all data volumes:
```bash
docker-compose down -v
```

## Service Access

### Kafka UI
- URL: http://localhost:8080
- Browse topics, messages, consumer groups

### PostgreSQL
```bash
# Connect via psql
docker exec -it etl-postgres psql -U etl_user -d etl_database

# Example queries
SELECT * FROM users;
SELECT * FROM user_events;
SELECT * FROM user_summary;
```

Credentials:
- Host: localhost:5432
- User: etl_user
- Password: etl_password
- Database: etl_database

### MySQL
```bash
# Connect via mysql client
docker exec -it etl-mysql mysql -u etl_user -petl_password etl_database

# Example queries
SELECT * FROM users;
SELECT * FROM product_catalog;
```

Credentials:
- Host: localhost:3306
- User: etl_user
- Password: etl_password
- Database: etl_database

### LocalStack S3
```bash
# List buckets
aws --endpoint-url=http://localhost:4566 s3 ls

# List objects in bucket
aws --endpoint-url=http://localhost:4566 s3 ls s3://etl-source-data/ --recursive

# Download file
aws --endpoint-url=http://localhost:4566 s3 cp s3://etl-source-data/users/sample-users.csv ./
```

Credentials:
- Access Key: test
- Secret Key: test
- Region: us-east-1
- Endpoint: http://localhost:4566

### Schema Registry
```bash
# List all subjects
curl http://localhost:8081/subjects

# Get schema for subject
curl http://localhost:8081/subjects/user-events-value/versions/latest
```

## Sample Data

### PostgreSQL Tables
- **users**: 5 sample users
- **transactions**: 5 sample transactions
- **user_events**: Empty (target for pipeline)
- **user_summary**: Empty (target for aggregations)

### MySQL Tables
- **users**: 5 sample users
- **product_catalog**: 5 sample products
- **user_events**: Empty (target for pipeline)
- **user_summary**: Empty (target for aggregations)

### S3 Objects
- `s3://etl-source-data/users/sample-users.csv`: 5 users in CSV format
- `s3://etl-source-data/events/sample-events.json`: 5 events in JSON format
- `s3://etl-sink-data/`: Empty (target for pipeline output)
- `s3://etl-archive/`: Empty (target for archival)

### Kafka Topics
- **user-events**: 3 partitions, empty
- **user-summary**: 3 partitions, empty
- **transactions**: 3 partitions, empty

## Manual Testing Examples

### Test Kafka Source → PostgreSQL Sink

1. Produce messages to Kafka:
```bash
docker exec -it etl-kafka kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic user-events
```

2. Run pipeline with kafka-to-postgres config

3. Verify in PostgreSQL:
```bash
docker exec -it etl-postgres psql -U etl_user -d etl_database -c "SELECT COUNT(*) FROM user_events;"
```

### Test PostgreSQL Source → Kafka Sink

1. Insert data into PostgreSQL:
```bash
docker exec -it etl-postgres psql -U etl_user -d etl_database -c \
  "INSERT INTO users (user_id, username, email) VALUES ('user-999', 'testuser', 'test@example.com');"
```

2. Run pipeline with postgres-to-kafka config

3. Verify in Kafka:
```bash
docker exec -it etl-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic user-events \
  --from-beginning
```

### Test S3 Source → S3 Sink

1. Upload file to source bucket:
```bash
aws --endpoint-url=http://localhost:4566 s3 cp \
  local-file.csv \
  s3://etl-source-data/input/file.csv
```

2. Run pipeline with s3-to-s3 config

3. Verify in sink bucket:
```bash
aws --endpoint-url=http://localhost:4566 s3 ls s3://etl-sink-data/ --recursive
```

## Troubleshooting

### Service won't start
```bash
# Check service logs
docker-compose logs <service-name>

# Restart specific service
docker-compose restart <service-name>
```

### Kafka connection errors
```bash
# Verify Kafka is ready
docker exec etl-kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Check topics
docker exec etl-kafka kafka-topics --list --bootstrap-server localhost:9092
```

### Database connection errors
```bash
# Test PostgreSQL connection
docker exec etl-postgres pg_isready -U etl_user -d etl_database

# Test MySQL connection
docker exec etl-mysql mysqladmin ping -h localhost -u etl_user -petl_password
```

### S3 connection errors
```bash
# Check LocalStack health
curl http://localhost:4566/_localstack/health

# Verify S3 service
aws --endpoint-url=http://localhost:4566 s3 ls
```

### Clean slate (reset all data)
```bash
docker-compose down -v
./docker/scripts/start-infrastructure.sh
```

## Resource Usage

Default resource allocation:
- Total Memory: ~4-6 GB
- CPU: 4-8 cores recommended
- Disk: ~2-3 GB for images + volumes

To reduce resource usage:
- Comment out services you don't need in docker-compose.yml
- Reduce Kafka partitions for topics
- Limit Spark parallelism in test configs

## Network

All services are on the `etl-network` bridge network and can communicate with each other using their service names:
- `kafka:29092` (internal)
- `postgres:5432`
- `mysql:3306`
- `localstack:4566`
- `schema-registry:8081`
