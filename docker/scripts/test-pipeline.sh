#!/bin/bash
# Test pipeline execution with local infrastructure

set -e

echo "Testing ETL Pipeline with Docker infrastructure"
echo "==============================================="
echo ""

# Check if infrastructure is running
if ! docker ps | grep -q etl-kafka; then
  echo "Error: Infrastructure not running. Start it with:"
  echo "  ./docker/scripts/start-infrastructure.sh"
  exit 1
fi

# Build the project
echo "Building project..."
sbt clean assembly

# Find the assembled JAR
JAR_FILE=$(find target/scala-2.12 -name "*-assembly-*.jar" | head -n 1)

if [ -z "$JAR_FILE" ]; then
  echo "Error: Assembly JAR not found. Build may have failed."
  exit 1
fi

echo "Found JAR: $JAR_FILE"
echo ""

# Test configuration - using local config for testing
CONFIG_FILE="src/main/resources/configs/example-batch-pipeline.json"

if [ ! -f "$CONFIG_FILE" ]; then
  echo "Error: Test configuration not found: $CONFIG_FILE"
  exit 1
fi

echo "Using configuration: $CONFIG_FILE"
echo ""

# Run the pipeline
echo "Running ETL pipeline..."
echo "======================="
spark-submit \
  --class com.etl.Main \
  --master "local[4]" \
  --driver-memory 2g \
  --executor-memory 2g \
  --conf spark.sql.shuffle.partitions=4 \
  --conf spark.default.parallelism=4 \
  "$JAR_FILE" \
  --config "$CONFIG_FILE" \
  --mode batch

echo ""
echo "Pipeline execution complete!"
echo ""
echo "You can verify results by:"
echo "  - Checking Kafka UI: http://localhost:8080"
echo "  - Querying PostgreSQL: docker exec -it etl-postgres psql -U etl_user -d etl_database"
echo "  - Querying MySQL: docker exec -it etl-mysql mysql -u etl_user -petl_password etl_database"
echo "  - Checking S3: aws --endpoint-url=http://localhost:4566 s3 ls s3://etl-sink-data/"
