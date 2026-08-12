#!/bin/bash
cd "$(dirname "$0")/.."
echo "Running DFS tests..."
go test -v ./pkg/... ./internal/... ./tests/...
