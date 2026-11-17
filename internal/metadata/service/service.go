package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"dfs-system/internal/common"
	"dfs-system/internal/metadata/model"
	"dfs-system/internal/metadata/repository"
	"dfs-system/pkg/chunking"
	"dfs-system/pkg/errors"
	"dfs-system/pkg/hashing"

	"go.uber.org/zap"
)

// downloadPrefetch is how many chunks may be fetched ahead of the one being written.
const downloadPrefetch = 2

// NodeHealthResult is the aggregated health status for one storage node.
type NodeHealthResult struct {
	NodeID     string `json:"node_id"`
	Address    string `json:"address"`
	Online     bool   `json:"online"`
	TotalSpace int64  `json:"total_space"`
	FreeSpace  int64  `json:"free_space"`
	Error      string `json:"error,omitempty"`
	// InCluster reports whether the node currently holds a live etcd lease, i.e.
	// whether uploads, downloads and repair will use it. Nil if etcd could not be queried.
	InCluster *bool `json:"in_cluster"`
	// ReplicaCount is the number of chunk replicas the metadata DB places on this node.
	ReplicaCount int64 `json:"replica_count"`
}

// NodeDiscoverer lists the storage nodes that are currently alive.
// It is satisfied by *coordinator.Coordinator (etcd) and by test fakes.
type NodeDiscoverer interface {
	DiscoverNodes(ctx context.Context) ([]common.StorageNodeInfo, error)
}

type MetadataService struct {
	repo       *repository.Repository
	discoverer NodeDiscoverer
	repFactor  int
	httpClient *http.Client
	logger     *zap.Logger

	// Chunks are content-addressed and shared between files. Uploads and
	// repairs hold the read lock while they place chunk data that the DB does
	// not reference yet; deletes hold the write lock while removing chunk data,
	// so a delete can never remove a chunk an in-flight upload is about to use.
	mutationMu sync.RWMutex

	// checking holds the IDs of files with an integrity check in progress.
	checking sync.Map
}

// NewMetadataService creates a new metadata server service.
func NewMetadataService(repo *repository.Repository, discoverer NodeDiscoverer, repFactor int, logger *zap.Logger) *MetadataService {
	return &MetadataService{
		repo:       repo,
		discoverer: discoverer,
		repFactor:  repFactor,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: logger,
	}
}

// GetActiveNodes retrieves active storage nodes from the etcd registry.
func (s *MetadataService) GetActiveNodes(ctx context.Context) ([]common.StorageNodeInfo, error) {
	nodes, err := s.discoverer.DiscoverNodes(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to discover active nodes in etcd: %w", err)
	}
	return nodes, nil
}

// liveAddresses returns live nodes and their addresses indexed by node ID.
func (s *MetadataService) liveAddresses(ctx context.Context) (map[string]string, []common.StorageNodeInfo, error) {
	nodes, err := s.GetActiveNodes(ctx)
	if err != nil {
		return nil, nil, err
	}
	addrs := make(map[string]string, len(nodes))
	for _, n := range nodes {
		addrs[n.NodeID] = n.Address
	}
	return addrs, nodes, nil
}

// placement tracks projected free space per node while chunks are being placed,
// so consecutive chunks spread across nodes instead of all landing on whichever
// nodes had the most free space at the start.
type placement struct {
	nodes []common.StorageNodeInfo
}

// newPlacement copies node capacity data to track space during replica placement.
func newPlacement(nodes []common.StorageNodeInfo) *placement {
	return &placement{nodes: append([]common.StorageNodeInfo(nil), nodes...)}
}

// candidates returns nodes with room for size bytes, excluding the given node IDs,
// ordered by most projected free space first.
func (p *placement) candidates(size int64, exclude map[string]bool) []common.StorageNodeInfo {
	out := make([]common.StorageNodeInfo, 0, len(p.nodes))
	for _, n := range p.nodes {
		if exclude[n.NodeID] || n.FreeSpace < size {
			continue
		}
		out = append(out, n)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].FreeSpace != out[j].FreeSpace {
			return out[i].FreeSpace > out[j].FreeSpace
		}
		return out[i].NodeID < out[j].NodeID
	})
	return out
}

// record subtracts newly stored replicas from each node's projected free space.
func (p *placement) record(replicas []model.ChunkReplica, size int64) {
	for _, r := range replicas {
		for i := range p.nodes {
			if p.nodes[i].NodeID == r.NodeID {
				p.nodes[i].FreeSpace -= size
			}
		}
	}
}

// replicateChunk stores data on up to `want` distinct candidate nodes. Nodes are
// tried in order; when one fails, the next candidate takes its place. It returns
// every replica that was successfully written, plus ErrRepFactorUnmet if fewer
// than `want` succeeded.
func (s *MetadataService) replicateChunk(ctx context.Context, hash string, data []byte, candidates []common.StorageNodeInfo, want int) ([]model.ChunkReplica, error) {
	replicas := make([]model.ChunkReplica, 0, want)
	next := 0
	for len(replicas) < want && next < len(candidates) {
		batch := candidates[next:min(next+want-len(replicas), len(candidates))]
		next += len(batch)

		errs := make([]error, len(batch))
		var wg sync.WaitGroup
		for i, node := range batch {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = s.uploadChunkToNode(ctx, node.Address, hash, data)
			}()
		}
		wg.Wait()

		for i, err := range errs {
			if err != nil {
				s.logger.Warn("Failed to upload chunk to storage node, trying another",
					zap.String("node_id", batch[i].NodeID),
					zap.String("address", batch[i].Address),
					zap.String("hash", hash),
					zap.Error(err),
				)
				continue
			}
			replicas = append(replicas, model.ChunkReplica{NodeID: batch[i].NodeID})
		}
	}
	if len(replicas) < want {
		return replicas, fmt.Errorf("%w: stored %d of %d replicas", errors.ErrRepFactorUnmet, len(replicas), want)
	}
	return replicas, nil
}

// UploadFile streams r in fixed-size chunks, replicates each chunk to active
// storage nodes, and records metadata. Only one chunk is held in memory at a time.
// If the upload fails part-way, the file is kept with status FAILED together with
// the replicas that were written, so deleting it reclaims that space.
func (s *MetadataService) UploadFile(ctx context.Context, filename string, r io.Reader) (*model.File, error) {
	s.logger.Info("Starting file upload", zap.String("filename", filename))

	activeNodes, err := s.GetActiveNodes(ctx)
	if err == nil && len(activeNodes) == 0 {
		err = errors.ErrNoActiveNodes
	}
	if err != nil {
		s.recordOperation(model.OpUpload, 0, filename, model.OpFailed, err.Error(), 0)
		return nil, err
	}

	want := s.repFactor
	if len(activeNodes) < want {
		s.logger.Warn("Active nodes count is less than target replication factor",
			zap.Int("active_nodes", len(activeNodes)),
			zap.Int("target_rep_factor", s.repFactor),
		)
		want = len(activeNodes)
	}

	s.mutationMu.RLock()
	defer s.mutationMu.RUnlock()

	dbFile := &model.File{
		Filename: filename,
		Status:   model.FileStatusPending,
	}
	if err := s.repo.CreateFile(dbFile); err != nil {
		return nil, fmt.Errorf("failed to create file record in DB: %w", err)
	}

	fileHash := sha256.New()
	place := newPlacement(activeNodes)
	reader := chunking.NewReader(r, chunking.DefaultChunkSize)
	var uploadErr error

	for seq := 0; ; seq++ {
		data, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			uploadErr = fmt.Errorf("failed to read uploaded file: %w", err)
			break
		}

		fileHash.Write(data)
		hash := hashing.ComputeSHA256(data)
		size := int64(len(data))
		s.logger.Debug("Processing chunk", zap.Int("sequence", seq), zap.String("hash", hash), zap.Int64("size", size))

		replicas, err := s.replicateChunk(ctx, hash, data, place.candidates(size, nil), want)
		place.record(replicas, size)
		if len(replicas) > 0 {
			dbFile.Chunks = append(dbFile.Chunks, model.Chunk{
				Sequence:  seq,
				Hash:      hash,
				SizeBytes: size,
				Replicas:  replicas,
			})
		}
		dbFile.SizeBytes += size
		if err != nil {
			uploadErr = fmt.Errorf("chunk %d: %w", seq, err)
			break
		}
	}

	dbFile.Status = model.FileStatusActive
	if uploadErr != nil {
		dbFile.Status = model.FileStatusFailed
	} else {
		dbFile.Checksum = hex.EncodeToString(fileHash.Sum(nil))
	}
	if err := s.repo.SaveFileWithAssociations(dbFile); err != nil {
		_ = s.repo.UpdateFileStatus(dbFile.ID, model.FileStatusFailed)
		s.recordOperation(model.OpUpload, dbFile.ID, filename, model.OpFailed, "failed to save file metadata", dbFile.SizeBytes)
		return nil, fmt.Errorf("failed to save file chunks in DB: %w", err)
	}
	if uploadErr != nil {
		s.logger.Error("File upload failed", zap.Uint("file_id", dbFile.ID), zap.Error(uploadErr))
		s.recordOperation(model.OpUpload, dbFile.ID, filename, model.OpFailed, uploadErr.Error(), dbFile.SizeBytes)
		return nil, uploadErr
	}
	s.recordOperation(model.OpUpload, dbFile.ID, filename, model.OpSuccess,
		fmt.Sprintf("%d chunk(s), %d replica(s) each", len(dbFile.Chunks), want), dbFile.SizeBytes)

	s.logger.Info("File upload completed successfully",
		zap.Uint("file_id", dbFile.ID),
		zap.Int64("size", dbFile.SizeBytes),
		zap.Int("chunks", len(dbFile.Chunks)),
	)
	return dbFile, nil
}

// uploadChunkToNode sends chunk data and its expected hash to one storage node.
func (s *MetadataService) uploadChunkToNode(ctx context.Context, address, hash string, data []byte) error {
	url := fmt.Sprintf("http://%s/chunk/upload?hash=%s", address, hash)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
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

// DownloadFile looks up a file and streams its content to w.
func (s *MetadataService) DownloadFile(ctx context.Context, fileID uint, w io.Writer) error {
	file, err := s.repo.GetFile(fileID)
	if err != nil {
		return err
	}
	return s.StreamFile(ctx, file, w)
}

// StreamFile fetches chunks from live replicas, verifies their SHA-256 hashes and
// writes them to w in order. The next few chunks are prefetched while the current
// one is written, so memory use stays bounded regardless of file size. Nothing is
// written to w if the first chunk cannot be fetched.
func (s *MetadataService) StreamFile(ctx context.Context, file *model.File, w io.Writer) (err error) {
	if file.Status != model.FileStatusActive {
		return errors.ErrFileNotReady
	}
	s.logger.Info("Starting file download", zap.Uint("file_id", file.ID))
	defer func() {
		if err != nil {
			s.recordOperation(model.OpDownload, file.ID, file.Filename, model.OpFailed, err.Error(), file.SizeBytes)
		} else {
			s.recordOperation(model.OpDownload, file.ID, file.Filename, model.OpSuccess, "", file.SizeBytes)
		}
	}()

	activeNodeMap, _, err := s.liveAddresses(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type fetched struct {
		data []byte
		err  error
	}
	results := make(chan fetched, downloadPrefetch)
	go func() {
		defer close(results)
		for _, chunk := range file.Chunks {
			data, err := s.fetchChunkFromReplicas(ctx, chunk, activeNodeMap)
			if err != nil {
				err = fmt.Errorf("failed to fetch chunk seq %d: %w", chunk.Sequence, err)
			}
			select {
			case results <- fetched{data, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for range file.Chunks {
		res, ok := <-results
		if !ok {
			return ctx.Err()
		}
		if res.err != nil {
			return res.err
		}
		if _, err := w.Write(res.data); err != nil {
			return fmt.Errorf("failed to write chunk to client: %w", err)
		}
	}

	s.logger.Info("File download completed successfully", zap.Uint("file_id", file.ID))
	return nil
}

// fetchChunkFromReplicas tries live replicas until it finds data matching the chunk hash.
func (s *MetadataService) fetchChunkFromReplicas(ctx context.Context, chunk model.Chunk, activeNodeMap map[string]string) ([]byte, error) {
	var lastErr error
	for _, replica := range chunk.Replicas {
		address, ok := activeNodeMap[replica.NodeID]
		if !ok {
			s.logger.Debug("Storage node replica is offline according to etcd", zap.String("node_id", replica.NodeID))
			lastErr = errors.ErrNodeUnavailable
			continue
		}

		data, err := s.downloadChunkFromNode(ctx, address, chunk.Hash)
		if err != nil {
			s.logger.Warn("Failed to download chunk from replica", zap.String("node_id", replica.NodeID), zap.Error(err))
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

// nodeStatusError is returned when a storage node answers with an unexpected HTTP status.
type nodeStatusError struct{ code int }

// Error describes the unexpected HTTP status returned by a storage node.
func (e *nodeStatusError) Error() string { return fmt.Sprintf("node returned status %d", e.code) }

// downloadChunkFromNode fetches a chunk with a bounded response size.
func (s *MetadataService) downloadChunkFromNode(ctx context.Context, address, hash string) ([]byte, error) {
	url := fmt.Sprintf("http://%s/chunk/%s", address, hash)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
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
		return nil, &nodeStatusError{resp.StatusCode}
	}

	return io.ReadAll(io.LimitReader(resp.Body, chunking.DefaultChunkSize+1))
}

// deleteChunkFromNode requests chunk removal, accepting an already missing chunk.
func (s *MetadataService) deleteChunkFromNode(ctx context.Context, address, hash string) error {
	url := fmt.Sprintf("http://%s/chunk/%s", address, hash)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("node returned status %d", resp.StatusCode)
	}
	return nil
}

// DeleteFile removes a file's metadata, then deletes its chunk data from storage
// nodes where no other file still references the same chunk. Chunk data on nodes
// that are offline is left behind and logged.
func (s *MetadataService) DeleteFile(ctx context.Context, fileID uint) error {
	s.mutationMu.Lock()
	defer s.mutationMu.Unlock()

	file, err := s.repo.DeleteFile(fileID)
	if err != nil {
		return err
	}
	s.logger.Info("File metadata deleted", zap.Uint("file_id", fileID), zap.String("filename", file.Filename))
	s.recordOperation(model.OpDelete, fileID, file.Filename, model.OpSuccess, "", file.SizeBytes)

	activeNodeMap, _, err := s.liveAddresses(ctx)
	if err != nil {
		s.logger.Warn("Could not discover nodes; chunk data left on storage nodes", zap.Uint("file_id", fileID), zap.Error(err))
		return nil
	}

	type location struct{ hash, nodeID string }
	seen := make(map[location]bool)
	for _, chunk := range file.Chunks {
		for _, replica := range chunk.Replicas {
			loc := location{chunk.Hash, replica.NodeID}
			if seen[loc] {
				continue
			}
			seen[loc] = true

			refs, err := s.repo.CountReplicaRefs(chunk.Hash, replica.NodeID)
			if err != nil || refs > 0 {
				continue // still used by another file (or unknown): keep it
			}
			address, ok := activeNodeMap[replica.NodeID]
			if !ok {
				s.logger.Warn("Node offline; orphaned chunk left behind", zap.String("node_id", replica.NodeID), zap.String("hash", chunk.Hash))
				continue
			}
			if err := s.deleteChunkFromNode(ctx, address, chunk.Hash); err != nil {
				s.logger.Warn("Failed to delete chunk from node", zap.String("node_id", replica.NodeID), zap.String("hash", chunk.Hash), zap.Error(err))
			}
		}
	}
	return nil
}

// RepairOnce restores the replication factor of every chunk belonging to an
// ACTIVE file: chunks with fewer live replicas than targeted are copied from a
// healthy replica (hash-verified) to additional live nodes. It returns the
// number of chunks that gained at least one replica.
func (s *MetadataService) RepairOnce(ctx context.Context) (int, error) {
	s.mutationMu.RLock()
	defer s.mutationMu.RUnlock()

	activeNodeMap, nodes, err := s.liveAddresses(ctx)
	if err != nil {
		return 0, err
	}
	if len(nodes) == 0 {
		return 0, nil
	}
	target := min(s.repFactor, len(nodes))

	chunks, err := s.repo.GetActiveChunks()
	if err != nil {
		return 0, fmt.Errorf("failed to list chunks: %w", err)
	}

	place := newPlacement(nodes)
	repaired := 0
	for _, chunk := range chunks {
		if ctx.Err() != nil {
			return repaired, ctx.Err()
		}

		holders := make(map[string]bool)
		for _, r := range chunk.Replicas {
			if _, ok := activeNodeMap[r.NodeID]; ok {
				holders[r.NodeID] = true
			}
		}
		missing := target - len(holders)
		if missing <= 0 {
			continue
		}
		if len(holders) == 0 {
			s.logger.Error("Chunk has no live replicas; cannot repair until a holder returns",
				zap.Uint("chunk_id", chunk.ID), zap.String("hash", chunk.Hash))
			continue
		}

		data, err := s.fetchChunkFromReplicas(ctx, chunk, activeNodeMap)
		if err != nil {
			s.logger.Warn("Repair: could not read chunk from any live replica", zap.Uint("chunk_id", chunk.ID), zap.Error(err))
			continue
		}

		replicas, err := s.replicateChunk(ctx, chunk.Hash, data, place.candidates(chunk.SizeBytes, holders), missing)
		place.record(replicas, chunk.SizeBytes)
		if err != nil {
			s.logger.Warn("Repair: chunk still under-replicated", zap.Uint("chunk_id", chunk.ID), zap.Error(err))
		}
		if len(replicas) == 0 {
			continue
		}
		if err := s.repo.AddReplicas(chunk.ID, replicas); err != nil {
			s.logger.Error("Repair: failed to record new replicas", zap.Uint("chunk_id", chunk.ID), zap.Error(err))
			continue
		}
		repaired++
	}
	return repaired, nil
}

// RunRepairLoop calls RepairOnce every interval until ctx is cancelled.
func (s *MetadataService) RunRepairLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := s.RepairOnce(ctx)
			if err != nil && !stderrors.Is(err, context.Canceled) {
				s.logger.Warn("Replica repair pass failed", zap.Error(err))
			} else if n > 0 {
				s.logger.Info("Replica repair pass restored chunks", zap.Int("chunks", n))
			}
		}
	}
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

	replicaCounts, err := s.repo.CountReplicasByNode()
	if err != nil {
		s.logger.Warn("Could not count replicas per node", zap.Error(err))
	}
	live, _, liveErr := s.liveAddresses(ctx)
	if liveErr != nil {
		s.logger.Warn("Could not query etcd for live nodes", zap.Error(liveErr))
	}

	results := make([]NodeHealthResult, len(nodes))
	var wg sync.WaitGroup
	healthClient := &http.Client{Timeout: 2 * time.Second}

	for i, node := range nodes {
		wg.Add(1)
		go func(idx int, n model.StorageNode) {
			defer wg.Done()
			result := NodeHealthResult{
				NodeID:       n.NodeID,
				Address:      n.Address,
				ReplicaCount: replicaCounts[n.NodeID],
			}
			if liveErr == nil {
				_, ok := live[n.NodeID]
				result.InCluster = &ok
			}
			defer func() { results[idx] = result }()

			url := fmt.Sprintf("http://%s/health", n.Address)
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				result.Error = err.Error()
				return
			}
			resp, err := healthClient.Do(req)
			if err != nil {
				result.Error = "unreachable"
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				result.Error = fmt.Sprintf("status %d", resp.StatusCode)
				return
			}
			result.Online = true
			var payload struct {
				TotalSpace int64 `json:"total_space"`
				FreeSpace  int64 `json:"free_space"`
			}
			if json.NewDecoder(resp.Body).Decode(&payload) == nil {
				result.TotalSpace = payload.TotalSpace
				result.FreeSpace = payload.FreeSpace
			}
		}(i, node)
	}
	wg.Wait()
	return results
}
