package service

import (
	"os"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
)

func TestStorageService(t *testing.T) {
	fs := afero.NewMemMapFs()
	root := "/test-data"

	store, err := NewStorageService(fs, root)
	assert.NoError(t, err)
	assert.NotNil(t, store)

	// Save chunk
	hash := "dummyhash123"
	data := []byte("chunk bytes content")
	err = store.SaveChunk(hash, data)
	assert.NoError(t, err)

	// Read chunk
	readData, err := store.GetChunk(hash)
	assert.NoError(t, err)
	assert.Equal(t, data, readData)

	// Read non-existent chunk
	_, err = store.GetChunk("nonexistent")
	assert.Error(t, err)
	assert.True(t, os.IsNotExist(err))

	// Get stats
	total, free, err := store.GetStats()
	assert.NoError(t, err)
	assert.Equal(t, int64(5*1024*1024*1024), total)
	assert.Equal(t, int64(5*1024*1024*1024-len(data)), free)
}
