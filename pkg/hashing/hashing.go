package hashing

import (
	"crypto/sha256"
	"encoding/hex"
)

// ComputeSHA256 calculates the SHA-256 hash of a byte slice and returns it as a hex string.
func ComputeSHA256(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
