#!/bin/bash
# Stop all infrastructure services

set -e

echo "Stopping ETL infrastructure..."
docker-compose down

echo ""
echo "Infrastructure stopped."
echo "To remove volumes (delete all data), run: docker-compose down -v"
