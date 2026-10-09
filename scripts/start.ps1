# Cross-compiles the Linux binaries on the host, then builds and starts the cluster.
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$env:GOOS = "linux"; $env:GOARCH = "amd64"; $env:CGO_ENABLED = "0"
try {
    Write-Host "Compiling Linux binaries..."
    go build -ldflags="-s -w" -o docker/metadata-server-linux ./cmd/metadata-server
    if ($LASTEXITCODE -ne 0) { exit 1 }
    go build -ldflags="-s -w" -o docker/storage-node-linux ./cmd/storage-node
    if ($LASTEXITCODE -ne 0) { exit 1 }
} finally {
    Remove-Item Env:GOOS, Env:GOARCH, Env:CGO_ENABLED
}

Write-Host "Starting DFS Cluster via Docker Compose..."
docker compose -f docker/docker-compose.yml up --build @args
