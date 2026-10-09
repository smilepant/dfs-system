package service

import (
	"os"
	"testing"

	"dfs-system/pkg/errors"
	"dfs-system/pkg/hashing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestStorageService(t *testing.T) {
	fs := afero.NewMemMapFs()
	root := "/test-data"

	store, err := NewStorageService(fs, root, 0)
	assert.NoError(t, err)
	assert.NotNil(t, store)

	// Save chunk
	data := []byte("chunk bytes content")
	hash := hashing.ComputeSHA256(data)
	err = store.SaveChunk(hash, data)
	assert.NoError(t, err)

	// Saving the same chunk again is idempotent and does not double-count usage
	assert.NoError(t, store.SaveChunk(hash, data))

	// Read chunk
	readData, err := store.GetChunk(hash)
	assert.NoError(t, err)
	assert.Equal(t, data, readData)

	// Read non-existent chunk
	_, err = store.GetChunk(hashing.ComputeSHA256([]byte("nonexistent")))
	assert.Error(t, err)
	assert.True(t, os.IsNotExist(err))

	// Get stats
	total, free, err := store.GetStats()
	assert.NoError(t, err)
	assert.Equal(t, DefaultCapacity, total)
	assert.Equal(t, DefaultCapacity-int64(len(data)), free)

	// Delete chunk frees space and is idempotent
	assert.NoError(t, store.DeleteChunk(hash))
	assert.NoError(t, store.DeleteChunk(hash))
	_, free, _ = store.GetStats()
	assert.Equal(t, DefaultCapacity, free)
}

func TestStorageServiceRejectsBadInput(t *testing.T) {
	fs := afero.NewMemMapFs()
	store, err := NewStorageService(fs, "/root/data", 0)
	assert.NoError(t, err)

	// Path traversal attempts are rejected before touching the filesystem
	err = store.SaveChunk("/../../evil", []byte("x"))
	assert.ErrorIs(t, err, errors.ErrInvalidChunkID)
	_, err = store.GetChunk("../../etc/passwd")
	assert.ErrorIs(t, err, errors.ErrInvalidChunkID)
	exists, _ := afero.Exists(fs, "/evil")
	assert.False(t, exists)

	// Data that does not match its declared hash is rejected
	err = store.SaveChunk(hashing.ComputeSHA256([]byte("a")), []byte("b"))
	assert.ErrorIs(t, err, errors.ErrChunkCorrupted)
}

func TestStorageServiceCapacityAndRecovery(t *testing.T) {
	fs := afero.NewMemMapFs()
	root := "/data"

	store, err := NewStorageService(fs, root, 10)
	assert.NoError(t, err)

	small := []byte("12345678")
	assert.NoError(t, store.SaveChunk(hashing.ComputeSHA256(small), small))

	big := []byte("abcdefgh")
	err = store.SaveChunk(hashing.ComputeSHA256(big), big)
	assert.ErrorIs(t, err, errors.ErrInsufficientSpace)

	// A restart recomputes usage from disk and discards stale temp files
	assert.NoError(t, afero.WriteFile(fs, root+"/tmp_leftover", []byte("junk"), 0644))
	restarted, err := NewStorageService(fs, root, 10)
	assert.NoError(t, err)
	_, free, _ := restarted.GetStats()
	assert.Equal(t, int64(2), free)
	exists, _ := afero.Exists(fs, root+"/tmp_leftover")
	assert.False(t, exists)
}
