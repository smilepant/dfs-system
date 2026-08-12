package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"dfs-system/internal/common"
	"dfs-system/internal/coordinator"
	"dfs-system/internal/metadata/model"
	"dfs-system/internal/metadata/repository"
	"dfs-system/pkg/chunking"
	"dfs-system/pkg/errors"
	"dfs-system/pkg/hashing"

	"go.uber.org/zap"
)

// NodeHealthResult is the aggregated health status for one storage node.
type NodeHealthResult struct {
	NodeID     string `json:"node_id"`
	Address    string `json:"address"`
	Online     bool   `json:"online"`
	TotalSpace int64  `json:"total_space"`
	FreeSpace  int64  `json:"free_space"`
	Error      string `json:"error,omitempty"`
}

type MetadataService struct {
	repo       *repository.Repository
	etcdCoord  *coordinator.Coordinator
	repFactor  int
	httpClient *http.Client
	logger     *zap.Logger
}

// NewMetadataService creates a new metadata server service.
func NewMetadataService(repo *repository.Repository, etcdCoord *coordinator.Coordinator, repFactor int, logger *zap.Logger) *MetadataService {
	return &MetadataService{
		repo:      repo,
		etcdCoord: etcdCoord,
		repFactor: repFactor,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		logger: logger,
	}
}

// GetActiveNodes retrieves active storage nodes from the etcd registry.
func (s *MetadataService) GetActiveNodes(ctx context.Context) ([]common.StorageNodeInfo, error) {
	nodes, err := s.etcdCoord.DiscoverNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to discover active nodes in etcd: %w", err)
	}
	return nodes, nil
}

// UploadFile splits a file into 4MB chunks, distributes replicas to active storage nodes, and registers metadata in GORM.
func (s *MetadataService) UploadFile(ctx context.Context, filename string, r io.Reader, sizeBytes int64) (*model.File, error) {
	s.logger.Info("Starting file upload", zap.String("filename", filename), zap.Int64("size", sizeBytes))

	activeNodes, err := s.GetActiveNodes(ctx)
	if err != nil {
		return nil, err
	}
	if len(activeNodes) == 0 {
		return nil, errors.ErrNoActiveNodes
	}

	actualRepFactor := s.repFactor
	if len(activeNodes) < actualRepFactor {
		s.logger.Warn("Active nodes count is less than target replication factor",
			zap.Int("active_nodes", len(activeNodes)),
			zap.Int("target_rep_factor", s.repFactor),
		)
		actualRepFactor = len(activeNodes)
	}

	chunkDataList, err := chunking.Split(r, chunking.DefaultChunkSize)
	if err != nil {
		return nil, fmt.Errorf("failed to split file: %w", err)
	}

	dbFile := &model.File{
		Filename:  filename,
		SizeBytes: sizeBytes,
		Status:    "PENDING",
	}
	if err := s.repo.CreateFile(dbFile); err != nil {
		return nil, fmt.Errorf("failed to create file record in DB: %w", err)
	}

	dbChunks := make([]model.Chunk, len(chunkDataList))

	for seq, chunkData := range chunkDataList {
		hash := hashing.ComputeSHA256(chunkData)
		s.logger.Info("Processing chunk", zap.Int("sequence", seq), zap.String("hash", hash), zap.Int("size", len(chunkData)))

		sort.Slice(activeNodes, func(i, j int) bool {
			return activeNodes[i].FreeSpace > activeNodes[j].FreeSpace
		})

		selectedNodes := make([]common.StorageNodeInfo, 0, actualRepFactor)
		for _, node := range activeNodes {
			if len(selectedNodes) == actualRepFactor {
				break
			}
			selectedNodes = append(selectedNodes, node)
		}

		if len(selectedNodes) < actualRepFactor {
			s.repo.UpdateFileStatus(dbFile.ID, "FAILED")
			return nil, errors.ErrRepFactorUnmet
		}

		var wg sync.WaitGroup
		errsChan := make(chan error, len(selectedNodes))
		replicas := make([]model.ChunkReplica, len(selectedNodes))

		for idx, nodeInfo := range selectedNodes {
			wg.Add(1)
			go func(i int, node common.StorageNodeInfo) {
				defer wg.Done()
				err := s.uploadChunkToNode(ctx, node.Address, hash, chunkData)
				if err != nil {
					s.logger.Error("Failed to upload chunk to storage node",
						zap.String("node_id", node.NodeID),
						zap.String("address", node.Address),
						zap.Error(err),
					)
					errsChan <- err
					return
				}
				replicas[i] = model.ChunkReplica{
					NodeID: node.NodeID,
				}
			}(idx, nodeInfo)
		}

		wg.Wait()
		close(errsChan)

		if len(errsChan) > 0 {
			s.repo.UpdateFileStatus(dbFile.ID, "FAILED")
			return nil, fmt.Errorf("failed to upload chunk sequence %d to all replicas", seq)
		}

		dbChunks[seq] = model.Chunk{
			Sequence:  seq,
			Hash:      hash,
			SizeBytes: int64(len(chunkData)),
			Replicas:  replicas,
		}
	}

	dbFile.Chunks = dbChunks
	dbFile.Status = "ACTIVE"
	if err := s.repo.SaveFileWithAssociations(dbFile); err != nil {
		return nil, fmt.Errorf("failed to save file chunks in DB: %w", err)
	}

	s.logger.Info("File upload completed successfully", zap.Uint("file_id", dbFile.ID))
	return dbFile, nil
}

func (s *MetadataService) uploadChunkToNode(ctx context.Context, address, hash string, data []byte) error {
	url := fmt.Sprintf("http://%s/chunk/upload?hash=%s", address, hash)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("node returned status code %d", resp.StatusCode)
	}
	return nil
}

// DownloadFile gathers chunks from active replicas, verifies SHA-256 hashes, and writes the stream to target writer.
func (s *MetadataService) DownloadFile(ctx context.Context, fileID uint, w io.Writer) error {
	s.logger.Info("Starting file download", zap.Uint("file_id", fileID))

	file, err := s.repo.GetFile(fileID)
	if err != nil {
		return errors.ErrFileNotFound
	}

	activeNodes, err := s.GetActiveNodes(ctx)
	if err != nil {
		return err
	}
	activeNodeMap := make(map[string]string)
	for _, n := range activeNodes {
		activeNodeMap[n.NodeID] = n.Address
	}

	chunks := make([][]byte, len(file.Chunks))

	for _, chunk := range file.Chunks {
		chunkData, err := s.fetchChunkFromReplicas(ctx, chunk, activeNodeMap)
		if err != nil {
			return fmt.Errorf("failed to fetch chunk seq %d: %w", chunk.Sequence, err)
		}
		chunks[chunk.Sequence] = chunkData
	}

	err = chunking.Merge(w, chunks)
	if err != nil {
		return fmt.Errorf("failed to merge chunks: %w", err)
	}

	s.logger.Info("File download completed successfully", zap.Uint("file_id", fileID))
	return nil
}

func (s *MetadataService) fetchChunkFromReplicas(ctx context.Context, chunk model.Chunk, activeNodeMap map[string]string) ([]byte, error) {
	var lastErr error
	for _, replica := range chunk.Replicas {
		address, ok := activeNodeMap[replica.NodeID]
		if !ok {
			s.logger.Warn("Storage node replica is offline according to etcd", zap.String("node_id", replica.NodeID))
			lastErr = errors.ErrNodeUnavailable
			continue
		}

		s.logger.Info("Fetching chunk from replica", zap.String("node_id", replica.NodeID), zap.String("address", address))
		data, err := s.downloadChunkFromNode(ctx, address, chunk.Hash)
		if err != nil {
			s.logger.Error("Failed to download chunk from replica", zap.String("node_id", replica.NodeID), zap.Error(err))
			lastErr = err
			continue
		}

		downloadedHash := hashing.ComputeSHA256(data)
		if downloadedHash != chunk.Hash {
			s.logger.Error("Chunk corruption detected", zap.String("node_id", replica.NodeID), zap.String("expected", chunk.Hash), zap.String("actual", downloadedHash))
			lastErr = errors.ErrChunkCorrupted
			continue
		}

		return data, nil
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, fmt.Errorf("no replica nodes available to fetch chunk")
}

func (s *MetadataService) downloadChunkFromNode(ctx context.Context, address, hash string) ([]byte, error) {
	url := fmt.Sprintf("http://%s/chunk/%s", address, hash)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, errors.ErrChunkNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("node returned status %d", resp.StatusCode)
	}

	return io.ReadAll(resp.Body)
}

// ListFiles lists all files in GORM.
func (s *MetadataService) ListFiles() ([]model.File, error) {
	return s.repo.GetAllFiles()
}

// GetFileMetadata retrieves file metadata by ID.
func (s *MetadataService) GetFileMetadata(fileID uint) (*model.File, error) {
	return s.repo.GetFile(fileID)
}

// RegisterNode saves storage node configuration in GORM.
func (s *MetadataService) RegisterNode(node *model.StorageNode) error {
	return s.repo.RegisterNode(node)
}

// GetAllNodeHealth concurrently pings every registered storage node and returns their health status.
func (s *MetadataService) GetAllNodeHealth(ctx context.Context) []NodeHealthResult {
	nodes, err := s.repo.GetAllNodes()
	if err != nil || len(nodes) == 0 {
		return []NodeHealthResult{}
	}

	results := make([]NodeHealthResult, len(nodes))
	var wg sync.WaitGroup
	healthClient := &http.Client{Timeout: 2 * time.Second}

	for i, node := range nodes {
		wg.Add(1)
		go func(idx int, n model.StorageNode) {
			defer wg.Done()
			result := NodeHealthResult{
				NodeID:  n.NodeID,
				Address: n.Address,
			}
			url := fmt.Sprintf("http://%s/health", n.Address)
			req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
			if err != nil {
				result.Error = err.Error()
				results[idx] = result
				return
			}
			resp, err := healthClient.Do(req)
			if err != nil {
				result.Online = false
				result.Error = "unreachable"
				results[idx] = result
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				result.Online = true
				var payload map[string]interface{}
				if json.NewDecoder(resp.Body).Decode(&payload) == nil {
					if v, ok := payload["total_space"].(float64); ok {
						result.TotalSpace = int64(v)
					}
					if v, ok := payload["free_space"].(float64); ok {
						result.FreeSpace = int64(v)
					}
				}
			}
			results[idx] = result
		}(i, node)
	}
	wg.Wait()
	return results
}
