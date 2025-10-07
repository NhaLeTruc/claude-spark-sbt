#!/bin/bash
# Start all infrastructure services for ETL pipeline testing

set -e

echo "Starting ETL infrastructure..."
echo "================================"

# Start services
docker-compose up -d

echo ""
echo "Waiting for services to be healthy..."
echo "This may take 30-60 seconds..."

# Wait for Kafka
echo -n "Waiting for Kafka..."
until docker exec etl-kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
  echo -n "."
  sleep 2
done
echo " ✓"

# Wait for PostgreSQL
echo -n "Waiting for PostgreSQL..."
until docker exec etl-postgres pg_isready -U etl_user -d etl_database > /dev/null 2>&1; do
  echo -n "."
  sleep 2
done
echo " ✓"

# Wait for MySQL
echo -n "Waiting for MySQL..."
until docker exec etl-mysql mysqladmin ping -h localhost -u etl_user -petl_password --silent > /dev/null 2>&1; do
  echo -n "."
  sleep 2
done
echo " ✓"

# Wait for Schema Registry
echo -n "Waiting for Schema Registry..."
until curl -s http://localhost:8081/subjects > /dev/null 2>&1; do
  echo -n "."
  sleep 2
done
echo " ✓"

# Wait for LocalStack
echo -n "Waiting for LocalStack..."
until curl -s http://localhost:4566/_localstack/health | grep -q '"s3".*"available"' 2>&1; do
  echo -n "."
  sleep 2
done
echo " ✓"

echo ""
echo "================================"
echo "Infrastructure is ready!"
echo "================================"
echo ""
echo "Service URLs:"
echo "  - Kafka UI:          http://localhost:8080"
echo "  - Schema Registry:   http://localhost:8081"
echo "  - PostgreSQL:        localhost:5432 (user: etl_user, pass: etl_password, db: etl_database)"
echo "  - MySQL:             localhost:3306 (user: etl_user, pass: etl_password, db: etl_database)"
echo "  - LocalStack S3:     http://localhost:4566"
echo ""
echo "Creating Kafka topics..."
docker exec etl-kafka kafka-topics --create --if-not-exists --topic user-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec etl-kafka kafka-topics --create --if-not-exists --topic user-summary --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec etl-kafka kafka-topics --create --if-not-exists --topic transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

echo ""
echo "Kafka topics created:"
docker exec etl-kafka kafka-topics --list --bootstrap-server localhost:9092

echo ""
echo "Ready for pipeline execution!"
echo "Use './docker/scripts/test-pipeline.sh' to run a test pipeline."
