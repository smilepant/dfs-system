package service

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"dfs-system/pkg/errors"
	"dfs-system/pkg/hashing"

	"github.com/spf13/afero"
)

// DefaultCapacity is the advertised capacity when none is configured (5 GB).
const DefaultCapacity = int64(5 * 1024 * 1024 * 1024)

const (
	chunkPrefix = "chunk_"
	tmpPrefix   = "tmp_"
)

type StorageService struct {
	fs          afero.Fs
	storageRoot string
	capacity    int64
	used        atomic.Int64
	writeMu     sync.Mutex // serializes the space check + rename so capacity cannot be overshot
}

// NewStorageService creates a new storage service under the given root directory.
// Leftover temp files from interrupted writes are removed and current usage is
// computed once, so later stats lookups do not need to walk the directory.
func NewStorageService(fs afero.Fs, storageRoot string, capacity int64) (*StorageService, error) {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	if err := fs.MkdirAll(storageRoot, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage root: %w", err)
	}

	s := &StorageService{fs: fs, storageRoot: storageRoot, capacity: capacity}

	entries, err := afero.ReadDir(fs, storageRoot)
	if err != nil {
		return nil, fmt.Errorf("failed to scan storage root: %w", err)
	}
	var used int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.HasPrefix(e.Name(), tmpPrefix) {
			_ = fs.Remove(filepath.Join(storageRoot, e.Name()))
			continue
		}
		used += e.Size()
	}
	s.used.Store(used)
	return s, nil
}

func (s *StorageService) chunkPath(hash string) (string, error) {
	if !hashing.IsValidSHA256(hash) {
		return "", errors.ErrInvalidChunkID
	}
	return filepath.Join(s.storageRoot, chunkPrefix+hash), nil
}

// SaveChunk verifies that data matches hash and stores it atomically
// (temp file + rename). Chunks are content-addressed, so storing an
// already-present chunk is a no-op.
func (s *StorageService) SaveChunk(hash string, data []byte) error {
	filePath, err := s.chunkPath(hash)
	if err != nil {
		return err
	}
	if hashing.ComputeSHA256(data) != hash {
		return errors.ErrChunkCorrupted
	}
	if info, err := s.fs.Stat(filePath); err == nil && info.Size() == int64(len(data)) {
		return nil
	}

	tmp, err := afero.TempFile(s.fs, s.storageRoot, tmpPrefix+hash+"_")
	if err != nil {
		return fmt.Errorf("failed to create temp chunk file: %w", err)
	}
	tmpName := tmp.Name()
	_, werr := tmp.Write(data)
	if werr == nil {
		werr = tmp.Sync()
	}
	if cerr := tmp.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		_ = s.fs.Remove(tmpName)
		return fmt.Errorf("failed to write chunk file: %w", werr)
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	var replaced int64
	if info, err := s.fs.Stat(filePath); err == nil {
		replaced = info.Size()
	}
	if s.used.Load()-replaced+int64(len(data)) > s.capacity {
		_ = s.fs.Remove(tmpName)
		return errors.ErrInsufficientSpace
	}
	if err := s.fs.Rename(tmpName, filePath); err != nil {
		_ = s.fs.Remove(tmpName)
		return fmt.Errorf("failed to commit chunk file: %w", err)
	}
	s.used.Add(int64(len(data)) - replaced)
	return nil
}

// OpenChunk opens a stored chunk for streaming. The caller must close the file.
func (s *StorageService) OpenChunk(hash string) (afero.File, os.FileInfo, error) {
	filePath, err := s.chunkPath(hash)
	if err != nil {
		return nil, nil, err
	}
	file, err := s.fs.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, os.ErrNotExist
		}
		return nil, nil, fmt.Errorf("failed to open chunk file: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to stat chunk file: %w", err)
	}
	return file, info, nil
}

// GetChunk reads a chunk by its hash from disk.
func (s *StorageService) GetChunk(hash string) ([]byte, error) {
	file, _, err := s.OpenChunk(hash)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk file: %w", err)
	}
	return data, nil
}

// DeleteChunk removes a chunk from disk. Deleting a missing chunk is not an error.
func (s *StorageService) DeleteChunk(hash string) error {
	filePath, err := s.chunkPath(hash)
	if err != nil {
		return err
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	info, err := s.fs.Stat(filePath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to stat chunk file: %w", err)
	}
	if err := s.fs.Remove(filePath); err != nil {
		return fmt.Errorf("failed to delete chunk file: %w", err)
	}
	s.used.Add(-info.Size())
	return nil
}

// GetStats returns the configured capacity and the remaining free space.
func (s *StorageService) GetStats() (total int64, free int64, err error) {
	total = s.capacity
	free = total - s.used.Load()
	if free < 0 {
		free = 0
	}
	return total, free, nil
}
