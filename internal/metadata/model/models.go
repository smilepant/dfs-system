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
	ID        uint      `gorm:"primaryKey" json:"id"`
	Filename  string    `gorm:"not null" json:"filename"`
	SizeBytes int64     `gorm:"not null" json:"size_bytes"`
	Status    string    `gorm:"not null" json:"status"` // PENDING, ACTIVE, FAILED
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
