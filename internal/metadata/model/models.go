package model

import (
	"time"
)

// File lifecycle states.
const (
	FileStatusPending = "PENDING"
	FileStatusActive  = "ACTIVE"
	FileStatusFailed  = "FAILED"
)

type File struct {
	ID        uint   `gorm:"primaryKey" json:"id"`
	Filename  string `gorm:"not null" json:"filename"`
	SizeBytes int64  `gorm:"not null" json:"size_bytes"`
	Status    string `gorm:"not null" json:"status"` // PENDING, ACTIVE, FAILED
	// Checksum is the SHA-256 of the complete file content, computed while the
	// upload is streamed. Empty for files uploaded before checksums were recorded.
	Checksum  string    `json:"checksum"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Chunks    []Chunk   `gorm:"foreignKey:FileID;constraint:OnDelete:CASCADE" json:"chunks,omitempty"`
}

type Chunk struct {
	ID        uint           `gorm:"primaryKey" json:"id"`
	FileID    uint           `gorm:"not null;index" json:"file_id"`
	Sequence  int            `gorm:"not null" json:"sequence"`
	Hash      string         `gorm:"not null;index" json:"hash"`
	SizeBytes int64          `gorm:"not null" json:"size_bytes"`
	Replicas  []ChunkReplica `gorm:"foreignKey:ChunkID;constraint:OnDelete:CASCADE" json:"replicas,omitempty"`
}

type ChunkReplica struct {
	ID      uint   `gorm:"primaryKey" json:"id"`
	ChunkID uint   `gorm:"not null;index" json:"chunk_id"`
	NodeID  string `gorm:"not null;index" json:"node_id"`
}

type StorageNode struct {
	NodeID     string    `gorm:"primaryKey" json:"node_id"`
	Address    string    `gorm:"not null" json:"address"`
	TotalSpace int64     `gorm:"not null" json:"total_space"`
	FreeSpace  int64     `gorm:"not null" json:"free_space"`
	IsActive   bool      `gorm:"default:true" json:"is_active"`
	LastSeen   time.Time `json:"last_seen"`
}

// Integrity check outcomes for a whole file.
const (
	CheckVerified    = "VERIFIED"    // every replica matches and the file checksum matches
	CheckDegraded    = "DEGRADED"    // content intact, but some replicas are bad/unreachable or below the replication target
	CheckCorrupt     = "CORRUPT"     // a chunk has no intact replica and corrupt data was found, or the file checksum differs
	CheckUnavailable = "UNAVAILABLE" // a chunk has no reachable intact replica (missing or offline, no corruption seen)
	CheckFailed      = "FAILED"      // the check itself could not run (e.g. node discovery failed)
)

// Per-replica results of an integrity check.
const (
	ReplicaOK      = "OK"      // data read back and its SHA-256 matches the chunk hash
	ReplicaCorrupt = "CORRUPT" // data read back but its SHA-256 differs
	ReplicaMissing = "MISSING" // the node is up but does not have the chunk
	ReplicaOffline = "OFFLINE" // the node is not registered in etcd (stopped or lease expired)
	ReplicaError   = "ERROR"   // the node could not be read (unreachable, server error)
)

// IntegrityCheck is the stored result of verifying one file against its replicas.
type IntegrityCheck struct {
	ID               uint           `gorm:"primaryKey" json:"id"`
	FileID           uint           `gorm:"not null;index" json:"file_id"`
	Status           string         `gorm:"not null" json:"status"`
	ExpectedChecksum string         `json:"expected_checksum"`
	ComputedChecksum string         `json:"computed_checksum"`
	ChunksTotal      int            `json:"chunks_total"`
	ReplicasTotal    int            `json:"replicas_total"`
	ReplicasOK       int            `json:"replicas_ok"`
	Message          string         `json:"message"`
	DurationMs       int64          `json:"duration_ms"`
	CheckedAt        time.Time      `gorm:"index" json:"checked_at"`
	Replicas         []ReplicaCheck `gorm:"foreignKey:CheckID;constraint:OnDelete:CASCADE" json:"replicas,omitempty"`
}

// ReplicaCheck is the result for one chunk replica within an IntegrityCheck.
type ReplicaCheck struct {
	ID            uint   `gorm:"primaryKey" json:"id"`
	CheckID       uint   `gorm:"not null;index" json:"check_id"`
	ChunkSequence int    `json:"chunk_sequence"`
	ChunkHash     string `json:"chunk_hash"`
	NodeID        string `json:"node_id"`
	Status        string `gorm:"not null" json:"status"`
	Detail        string `json:"detail,omitempty"`
}

// Operation types and results recorded in the activity log.
const (
	OpUpload   = "UPLOAD"
	OpDownload = "DOWNLOAD"
	OpDelete   = "DELETE"
	OpVerify   = "VERIFY"
	OpRepair   = "REPAIR"

	OpSuccess = "SUCCESS"
	OpFailed  = "FAILED"
)

// Operation is one entry in the file activity log. For VERIFY and REPAIR
// operations, Result holds the resulting IntegrityCheck status instead of SUCCESS/FAILED.
type Operation struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	Type      string    `gorm:"not null;index" json:"type"`
	FileID    uint      `json:"file_id"`
	Filename  string    `json:"filename"`
	Result    string    `gorm:"not null" json:"result"`
	Detail    string    `json:"detail,omitempty"`
	SizeBytes int64     `json:"size_bytes"`
	CreatedAt time.Time `gorm:"index" json:"created_at"`
}

// All lists every model for auto-migration.
func All() []any {
	return []any{&File{}, &Chunk{}, &ChunkReplica{}, &StorageNode{}, &IntegrityCheck{}, &ReplicaCheck{}, &Operation{}}
}
