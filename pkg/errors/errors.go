package errors

import "errors"

var (
	ErrNodeUnavailable   = errors.New("storage node is unavailable")
	ErrChunkCorrupted    = errors.New("chunk data is corrupted (SHA-256 hash mismatch)")
	ErrFileNotFound      = errors.New("file not found")
	ErrFileNotReady      = errors.New("file is not in ACTIVE state")
	ErrChunkNotFound     = errors.New("chunk not found")
	ErrInvalidChunkID    = errors.New("invalid chunk id (expected lowercase hex SHA-256)")
	ErrInsufficientSpace = errors.New("insufficient storage space on node")
	ErrNoActiveNodes     = errors.New("no active storage nodes available")
	ErrRepFactorUnmet    = errors.New("could not satisfy replication factor")
	ErrCheckInProgress   = errors.New("an integrity check or repair for this file is already running")
)
