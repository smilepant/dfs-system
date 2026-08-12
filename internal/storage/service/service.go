package service

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/afero"
)

type StorageService struct {
	fs          afero.Fs
	storageRoot string
}

// NewStorageService creates a new storage service under the given root directory.
func NewStorageService(fs afero.Fs, storageRoot string) (*StorageService, error) {
	err := fs.MkdirAll(storageRoot, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage root: %w", err)
	}
	return &StorageService{
		fs:          fs,
		storageRoot: storageRoot,
	}, nil
}

// SaveChunk writes the bytes of a chunk on disk with its SHA-256 hash.
func (s *StorageService) SaveChunk(hash string, data []byte) error {
	filePath := filepath.Join(s.storageRoot, "chunk_"+hash)
	file, err := s.fs.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create chunk file: %w", err)
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return fmt.Errorf("failed to write chunk file: %w", err)
	}
	return nil
}

// GetChunk reads a chunk by its hash from disk.
func (s *StorageService) GetChunk(hash string) ([]byte, error) {
	filePath := filepath.Join(s.storageRoot, "chunk_"+hash)
	file, err := s.fs.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("failed to open chunk file: %w", err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read chunk file: %w", err)
	}
	return data, nil
}

// GetStats returns mock total capacity (5GB) and calculated free space.
func (s *StorageService) GetStats() (total int64, free int64, err error) {
	total = int64(5 * 1024 * 1024 * 1024) // Mock 5 GB capacity

	var used int64
	err = afero.Walk(s.fs, s.storageRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			used += info.Size()
		}
		return nil
	})
	if err != nil {
		return 0, 0, fmt.Errorf("failed to calculate storage stats: %w", err)
	}

	free = total - used
	if free < 0 {
		free = 0
	}
	return total, free, nil
}
