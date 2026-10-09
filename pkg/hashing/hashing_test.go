package hashing

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestComputeSHA256(t *testing.T) {
	data := []byte("hello world")

	// Compute expected hash directly using stdlib to avoid hardcoding errors
	h := sha256.Sum256(data)
	expectedHash := hex.EncodeToString(h[:])

	hash := ComputeSHA256(data)
	assert.Equal(t, expectedHash, hash)
	assert.Len(t, hash, 64) // SHA-256 is always 64 hex characters
}

func TestComputeSHA256Deterministic(t *testing.T) {
	data := []byte("chunk data content")
	hash1 := ComputeSHA256(data)
	hash2 := ComputeSHA256(data)
	assert.Equal(t, hash1, hash2, "Same data must always produce the same hash")
}

func TestIsValidSHA256(t *testing.T) {
	assert.True(t, IsValidSHA256(ComputeSHA256([]byte("x"))))
	assert.False(t, IsValidSHA256(""))
	assert.False(t, IsValidSHA256("dummyhash123"))
	assert.False(t, IsValidSHA256("/../../../../../../../../../../../../../../../../../../etc/passwd"))
	assert.False(t, IsValidSHA256("ABCDEF0000000000000000000000000000000000000000000000000000000000"))
}

func TestComputeSHA256DifferentData(t *testing.T) {
	hash1 := ComputeSHA256([]byte("data-a"))
	hash2 := ComputeSHA256([]byte("data-b"))
	assert.NotEqual(t, hash1, hash2, "Different data must produce different hashes")
}
