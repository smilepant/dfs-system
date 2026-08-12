#!/bin/bash
# Move to the root of the project
cd "$(dirname "$0")/.."
echo "Starting DFS Cluster via Docker Compose..."
docker-compose -f docker/docker-compose.yml up --build
