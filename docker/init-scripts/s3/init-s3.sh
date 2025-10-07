#!/bin/bash
# Initialize S3 buckets and upload sample data

set -e

echo "Waiting for LocalStack to be ready..."
sleep 10

# Create buckets
awslocal s3 mb s3://etl-source-data
awslocal s3 mb s3://etl-sink-data
awslocal s3 mb s3://etl-archive

echo "S3 buckets created:"
awslocal s3 ls

# Create sample CSV data
cat > /tmp/sample-users.csv << 'EOF'
user_id,username,email,created_at
user-001,alice,alice@example.com,2024-01-01T00:00:00Z
user-002,bob,bob@example.com,2024-01-02T00:00:00Z
user-003,charlie,charlie@example.com,2024-01-03T00:00:00Z
user-004,diana,diana@example.com,2024-01-04T00:00:00Z
user-005,eve,eve@example.com,2024-01-05T00:00:00Z
EOF

# Create sample JSON data
cat > /tmp/sample-events.json << 'EOF'
{"event_id":"evt-001","user_id":"user-001","event_type":"PAGE_VIEW","timestamp":1704067200000}
{"event_id":"evt-002","user_id":"user-001","event_type":"BUTTON_CLICK","timestamp":1704067260000}
{"event_id":"evt-003","user_id":"user-002","event_type":"FORM_SUBMIT","timestamp":1704067320000}
{"event_id":"evt-004","user_id":"user-003","event_type":"API_CALL","timestamp":1704067380000}
{"event_id":"evt-005","user_id":"user-002","event_type":"PAGE_VIEW","timestamp":1704067440000}
EOF

# Upload sample data
awslocal s3 cp /tmp/sample-users.csv s3://etl-source-data/users/sample-users.csv
awslocal s3 cp /tmp/sample-events.json s3://etl-source-data/events/sample-events.json

echo "Sample data uploaded to S3:"
awslocal s3 ls s3://etl-source-data/ --recursive

echo "S3 initialization complete!"
