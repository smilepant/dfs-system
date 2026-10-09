#!/bin/bash
# Cross-compiles the Linux binaries on the host, then builds and starts the cluster.
set -e
cd "$(dirname "$0")/.."

echo "Compiling Linux binaries..."
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o docker/metadata-server-linux ./cmd/metadata-server
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o docker/storage-node-linux ./cmd/storage-node

echo "Starting DFS Cluster via Docker Compose..."
docker compose -f docker/docker-compose.yml up --build "$@"
