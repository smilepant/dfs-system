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

// IsValidSHA256 reports whether s is a lowercase hex-encoded SHA-256 digest.
// Chunk IDs are used to build on-disk paths, so anything else must be rejected.
func IsValidSHA256(s string) bool {
	if len(s) != sha256.Size*2 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
