# Distributed File System (DFS)

A modular, production-structured, and fully Dockerized Distributed File System written in Go.

---

## Architecture Overview

```
Client
  │
  ▼
Metadata Server (Gin + GORM + SQLite)
  │  ├── Streams the uploaded file in 4 MB chunks (one chunk in memory at a time)
  │  ├── Computes SHA-256 per chunk (pkg/hashing) — the chunk's content address
  │  ├── Discovers live nodes (and their free space) via etcd
  │  ├── Places each replica on the node with most projected free space,
  │  │   falling back to another node if a replica write fails
  │  ├── Stores file/chunk/replica metadata in SQLite via GORM
  │  ├── On download: fetches chunks (with prefetch), verifies hash, streams file
  │  ├── On delete: removes chunk data no other file still references
  │  └── Background repair: re-replicates chunks whose holders went offline
  │
  ├── etcd (node registry, lease-based heartbeats + free-space updates)
  │
  └── Storage Nodes (×3) (Gin + afero)
        ├── Registers with etcd (TTL lease + keep-alive), refreshes stats every interval
        ├── Also registers with Metadata Server via POST /nodes/register (retried)
        ├── Verifies chunk hash on write, writes atomically (temp file + rename)
        └── Serves POST /chunk/upload  GET|DELETE /chunk/:id  GET /health
```

---

## Project Structure

```
dfs-system/
├── cmd/
│   ├── metadata-server/main.go   # Metadata Server entry point
│   └── storage-node/main.go      # Storage Node entry point
│
├── internal/
│   ├── common/
│   │   ├── config/config.go      # YAML config loader
│   │   ├── logger/logger.go      # Zap logger initializer
│   │   └── types.go              # Shared types (NodeInfo, requests)
│   │
│   ├── coordinator/
│   │   └── etcd_client.go        # etcd: register, discover, keep-alive
│   │
│   ├── metadata/
│   │   ├── handler/handler.go    # Gin HTTP handlers
│   │   ├── model/models.go       # GORM models (File, Chunk, Replica, Node)
│   │   ├── repository/repository.go # GORM DB queries
│   │   ├── service/service.go    # Business logic (upload/download)
│   │   └── metadata.go
│   │
│   └── storage/
│       ├── handler/handler.go    # Gin HTTP handlers for storage node
│       ├── service/service.go    # afero-based chunk I/O + stats
│       ├── node/node.go          # Background etcd + metadata registration
│       └── storage.go
│
├── pkg/
│   ├── chunking/chunker.go       # Split / Merge file chunks (4 MB default)
│   ├── hashing/hashing.go        # SHA-256 integrity computation
│   └── errors/errors.go          # Domain error types
│
├── configs/
│   └── config.yaml               # Default configuration
│
├── docker/
│   ├── docker-compose.yml        # Full cluster orchestration
│   ├── metadata.Dockerfile       # Metadata Server image
│   └── storage.Dockerfile        # Storage Node image
│
├── scripts/
│   ├── start.sh                  # Launch cluster
│   └── test.sh                   # Run all tests
│
├── tests/
│   └── integration/dfs_test.go   # Integration tests (etcd + GORM)
│
├── go.mod
└── README.md
```

---

## Technology Stack

| Concern        | Library                          |
|----------------|----------------------------------|
| HTTP API       | `github.com/gin-gonic/gin`       |
| ORM            | `gorm.io/gorm` + SQLite driver   |
| Coordination   | `go.etcd.io/etcd/client/v3`      |
| Filesystem     | `github.com/spf13/afero`         |
| Logging        | `go.uber.org/zap`                |
| Testing        | `github.com/stretchr/testify`    |
| Config         | `gopkg.in/yaml.v3`               |

---

## API Reference

### Metadata Server (`:8000`)

| Method | Path                   | Description                              |
|--------|------------------------|------------------------------------------|
| POST   | `/upload`              | Upload a file (multipart form-data)      |
| GET    | `/download/:fileId`    | Download a file by its integer ID        |
| GET    | `/files`               | List all registered files (newest first) |
| GET    | `/files/:fileId`       | File metadata with chunk/replica map     |
| DELETE | `/files/:fileId`       | Delete a file and its unshared chunks    |
| POST   | `/nodes/register`      | Register a storage node in the DB        |
| GET    | `/nodes/health`        | Health of every registered storage node  |

Status codes: `404` unknown file, `409` file not `ACTIVE` (e.g. a `FAILED` upload),
`503` not enough live nodes / replicas to serve the request.

### Storage Node (`:8081`, `:8082`, `:8083`)

| Method | Path              | Description                                        |
|--------|-------------------|----------------------------------------------------|
| POST   | `/chunk/upload`   | Store a chunk (`?hash=<sha256>`); body must match  |
| GET    | `/chunk/:id`      | Retrieve a chunk by hash                           |
| DELETE | `/chunk/:id`      | Delete a chunk                                     |
| GET    | `/health`         | Node health + storage statistics                   |

Chunk IDs must be 64-char lowercase hex SHA-256 digests; anything else is rejected with `400`.

---

## API Examples

### Upload a file
```bash
curl -X POST http://localhost:8000/upload \
  -F "file=@/path/to/yourfile.txt"
```

**Response:**
```json
{
  "id": 1,
  "filename": "yourfile.txt",
  "size_bytes": 12345678,
  "status": "ACTIVE",
  "chunks": [
    { "sequence": 0, "hash": "abc123...", "size_bytes": 4194304 },
    { "sequence": 1, "hash": "def456...", "size_bytes": 4194304 }
  ]
}
```

### Download a file
```bash
curl http://localhost:8000/download/1 --output downloaded.txt
```

### List files
```bash
curl http://localhost:8000/files
```

### Check storage node health
```bash
curl http://localhost:8081/health
```
**Response:**
```json
{
  "node_id": "storage-node-1",
  "status": "UP",
  "total_space": 5368709120,
  "free_space": 5361234567
}
```

### Register a node manually
```bash
curl -X POST http://localhost:8000/nodes/register \
  -H "Content-Type: application/json" \
  -d '{"node_id":"node-1","address":"localhost:8081","total_space":5368709120,"free_space":5000000000}'
```

---

## Running with Docker Compose

### Prerequisites
- Docker Desktop installed and running

### Start the full cluster
```bash
cd docker/
docker-compose up --build
```

This starts:
- **etcd** on port `2379`
- **metadata-server** on port `8000`
- **storage-node-1** on port `8081`
- **storage-node-2** on port `8082`
- **storage-node-3** on port `8083`

### Stop the cluster
```bash
docker-compose down -v
```

---

## Running Locally (without Docker)

### 1. Start etcd
```bash
# Using Docker for etcd only:
docker run -d -p 2379:2379 \
  quay.io/coreos/etcd:v3.5.0 \
  /usr/local/bin/etcd \
  --listen-client-urls http://0.0.0.0:2379 \
  --advertise-client-urls http://localhost:2379
```

### 2. Start the Metadata Server
```bash
cd dfs-system/
go run ./cmd/metadata-server/main.go \
  -port 8000 \
  -db ./metadata.db \
  -etcd localhost:2379 \
  -rep 2
```

### 3. Start Storage Nodes (in separate terminals)
```bash
# Node 1
go run ./cmd/storage-node/main.go \
  -id storage-node-1 -port 8081 -dir ./data/node1 \
  -etcd localhost:2379 -metadata-url http://localhost:8000

# Node 2
go run ./cmd/storage-node/main.go \
  -id storage-node-2 -port 8082 -dir ./data/node2 \
  -etcd localhost:2379 -metadata-url http://localhost:8000

# Node 3
go run ./cmd/storage-node/main.go \
  -id storage-node-3 -port 8083 -dir ./data/node3 \
  -etcd localhost:2379 -metadata-url http://localhost:8000
```

### 4. Upload and Download a file
```bash
# Upload
curl -X POST http://localhost:8000/upload -F "file=@README.md"

# Download (use the file ID from upload response)
curl http://localhost:8000/download/1 --output downloaded.md

# Verify integrity
# On Linux/Mac:
sha256sum README.md downloaded.md
# On Windows PowerShell:
Get-FileHash README.md, downloaded.md -Algorithm SHA256
```

---

## Running Tests

```bash
cd dfs-system/
go test -v ./...
```

| Package                         | Tests                              |
|---------------------------------|------------------------------------|
| `pkg/chunking`                  | Split + Merge round-trip, streaming reader edge cases |
| `pkg/hashing`                   | SHA-256 correctness, determinism, chunk-ID validation |
| `internal/storage/service`      | Chunk save/read/delete, hash verification, path traversal, capacity, restart recovery |
| `internal/metadata/service`     | End-to-end upload/download against in-process storage nodes: placement spread, write failover, node loss, corruption, re-replication, dedup-safe delete |
| `tests/integration`             | GORM repo + etcd (skipped if down) |

---

## Configuration Reference (`configs/config.yaml`)

```yaml
metadata:
  port: 8000
  db_path: "./metadata.db"
  replication_factor: 2        # Number of storage node replicas per chunk
  etcd_endpoints: ["etcd:2379"]
  repair_interval: 30          # Seconds between re-replication passes (negative disables)

storage:
  node_id: "storage-node-1"
  port: 8081
  storage_root: "/data"
  etcd_endpoints: ["etcd:2379"]
  heartbeat_interval: 5        # Seconds between etcd stats refreshes (lease TTL = 3×)
  metadata_server_url: "http://metadata-server:8000"
  # advertise_address: "host:port"   # how others reach this node (default: localhost:<port>, or <node_id>:<port> in Docker)
  # capacity_bytes: 5368709120       # advertised capacity (default 5 GiB)
```

All config values can also be overridden via CLI flags (`-etcd` accepts a comma-separated list;
storage nodes also accept `-advertise` and `-capacity`; the metadata server accepts `-repair-interval`).

---

## Fault Tolerance

- **Node failure**: If a storage node goes offline, its etcd lease expires. The Metadata Server detects this during `DiscoverNodes()` and skips that node's address — the chunk is fetched from another live replica instead.
- **Re-replication**: Every `repair_interval` seconds the Metadata Server finds chunks with fewer live replicas than the replication factor, copies them (hash-verified) from a surviving replica to other live nodes, and records the new replicas. A second failure after repair therefore doesn't lose data.
- **Write failover**: If a storage node rejects or fails a chunk write, the next-best node is tried. If the replication factor still can't be met, the file is kept as `FAILED` (with whatever replicas were written) so `DELETE /files/:id` can reclaim the space.
- **Chunk corruption**: Storage nodes refuse chunks whose content doesn't match the declared hash. Each downloaded chunk is verified again against its SHA-256 hash; on mismatch the next replica is tried.
- **Crash safety**: Chunks are written to a temp file, fsynced and renamed into place, so a crash never leaves a half-written chunk under its final name. Stale temp files are removed on startup.
- **Deduplication-safe deletes**: Chunks are content-addressed and may be shared by several files. A chunk is removed from a node only when no remaining file references it there.
- **Replication factor**: With `replication_factor: 2`, every chunk is stored on 2 nodes, so the system tolerates 1 node failure without data loss.

### Known limitations
- The Metadata Server (SQLite) is a single point of failure.
- Chunk data on a node that is offline when its file is deleted is left behind (orphaned) — there is no orphan GC yet.
- There is no authentication; don't expose the ports publicly.
