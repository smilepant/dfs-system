package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	stderrors "errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"dfs-system/internal/metadata/model"
	"dfs-system/pkg/errors"
	"dfs-system/pkg/hashing"

	"go.uber.org/zap"
)

// checkHistoryLimit is how many past checks GetIntegrityReport returns.
const checkHistoryLimit = 10

// FileIntegrity summarises one file and its most recent integrity check.
type FileIntegrity struct {
	FileID    uint                  `json:"file_id"`
	Filename  string                `json:"filename"`
	SizeBytes int64                 `json:"size_bytes"`
	Status    string                `json:"status"`
	Checksum  string                `json:"checksum"`
	Chunks    int                   `json:"chunks"`
	Replicas  int                   `json:"replicas"`
	CreatedAt time.Time             `json:"created_at"`
	LastCheck *model.IntegrityCheck `json:"last_check"`
}

// IntegrityReport is a file's newest check (with per-replica results) plus older checks.
type IntegrityReport struct {
	File    FileIntegrity          `json:"file"`
	History []model.IntegrityCheck `json:"history"`
}

// ClusterStats is a snapshot of what the metadata server knows about the cluster.
type ClusterStats struct {
	ReplicationFactor int    `json:"replication_factor"`
	FilesTotal        int64  `json:"files_total"`
	FilesActive       int64  `json:"files_active"`
	FilesFailed       int64  `json:"files_failed"`
	FilesPending      int64  `json:"files_pending"`
	LogicalBytes      int64  `json:"logical_bytes"` // total size of ACTIVE files, before replication
	Chunks            int    `json:"chunks"`        // chunks belonging to ACTIVE files
	Replicas          int    `json:"replicas"`
	LiveNodes         int    `json:"live_nodes"`         // nodes with a live etcd lease
	UnderReplicated   int    `json:"under_replicated"`   // chunks with fewer live replicas than the target
	UnavailableChunks int    `json:"unavailable_chunks"` // chunks with no live replica at all
	DiscoveryError    string `json:"discovery_error,omitempty"`
	// IntegrityByStatus counts ACTIVE files by the result of their latest check;
	// files never checked are counted under "UNCHECKED".
	IntegrityByStatus map[string]int `json:"integrity_by_status"`
}

// recordOperation appends to the activity log. Failures are logged, never
// returned: the log must not cause the operation itself to fail.
func (s *MetadataService) recordOperation(opType string, fileID uint, filename, result, detail string, size int64) {
	op := &model.Operation{
		Type:      opType,
		FileID:    fileID,
		Filename:  filename,
		Result:    result,
		Detail:    detail,
		SizeBytes: size,
	}
	if err := s.repo.CreateOperation(op); err != nil {
		s.logger.Warn("Failed to record operation", zap.String("type", opType), zap.Uint("file_id", fileID), zap.Error(err))
	}
}

// ListOperations returns the newest activity log entries, optionally filtered by type.
func (s *MetadataService) ListOperations(opType string, limit int) ([]model.Operation, error) {
	return s.repo.ListOperations(opType, limit)
}

// VerifyFile reads every replica of every chunk of an ACTIVE file back from its
// storage node, checks each against the chunk's SHA-256, and recomputes the
// whole-file SHA-256 from intact replicas. The result is stored and returned.
//
// Problems with the cluster (offline nodes, missing or corrupt chunks) are
// reported in the returned check, not as an error; an error means the check
// could not be started (unknown file, file not ACTIVE, check already running).
func (s *MetadataService) VerifyFile(ctx context.Context, fileID uint) (*model.IntegrityCheck, error) {
	file, err := s.repo.GetFile(fileID)
	if err != nil {
		return nil, err
	}
	if file.Status != model.FileStatusActive {
		return nil, errors.ErrFileNotReady
	}
	if _, running := s.checking.LoadOrStore(fileID, struct{}{}); running {
		return nil, errors.ErrCheckInProgress
	}
	defer s.checking.Delete(fileID)

	// Keep deletes from removing chunk data mid-check, which would show up as
	// false MISSING results.
	s.mutationMu.RLock()
	check := s.runCheck(ctx, file)
	s.mutationMu.RUnlock()

	if err := s.repo.SaveIntegrityCheck(check); err != nil {
		return nil, fmt.Errorf("failed to save integrity check: %w", err)
	}
	s.recordOperation(model.OpVerify, file.ID, file.Filename, check.Status, check.Message, file.SizeBytes)
	s.logger.Info("Integrity check finished",
		zap.Uint("file_id", file.ID),
		zap.String("status", check.Status),
		zap.Int("replicas_ok", check.ReplicasOK),
		zap.Int("replicas_total", check.ReplicasTotal),
	)
	return check, nil
}

// runCheck checks replicas and the file checksum, then builds an integrity result.
func (s *MetadataService) runCheck(ctx context.Context, file *model.File) *model.IntegrityCheck {
	start := time.Now()
	check := &model.IntegrityCheck{
		FileID:           file.ID,
		ExpectedChecksum: file.Checksum,
		ChunksTotal:      len(file.Chunks),
		CheckedAt:        start,
	}
	defer func() { check.DurationMs = time.Since(start).Milliseconds() }()

	activeNodeMap, nodes, err := s.liveAddresses(ctx)
	if err != nil {
		check.Status = model.CheckFailed
		check.Message = "Could not query etcd for live storage nodes."
		s.logger.Warn("Integrity check could not discover nodes", zap.Uint("file_id", file.ID), zap.Error(err))
		return check
	}
	target := max(1, min(s.repFactor, len(nodes)))

	fileHash := sha256.New()
	var lostChunks, corruptLost, underReplicated int
	for _, chunk := range file.Chunks {
		results, intact := s.checkChunkReplicas(ctx, chunk, activeNodeMap)
		if ctx.Err() != nil {
			check.Replicas = nil
			check.ReplicasTotal, check.ReplicasOK = 0, 0
			check.Status = model.CheckFailed
			check.Message = "Check was cancelled before it completed."
			return check
		}

		ok, corrupt := 0, 0
		for _, r := range results {
			switch r.Status {
			case model.ReplicaOK:
				ok++
			case model.ReplicaCorrupt:
				corrupt++
			}
		}
		check.Replicas = append(check.Replicas, results...)
		check.ReplicasTotal += len(results)
		check.ReplicasOK += ok

		if intact == nil {
			lostChunks++
			if corrupt > 0 {
				corruptLost++
			}
			continue
		}
		if ok < target {
			underReplicated++
		}
		if lostChunks == 0 {
			fileHash.Write(intact)
		}
	}

	if lostChunks > 0 {
		check.Status = model.CheckUnavailable
		if corruptLost > 0 {
			check.Status = model.CheckCorrupt
		}
		check.Message = fmt.Sprintf("%d of %d chunk(s) have no intact replica on a reachable node; the file cannot be fully read.",
			lostChunks, len(file.Chunks))
		return check
	}

	check.ComputedChecksum = hex.EncodeToString(fileHash.Sum(nil))
	var notes []string
	switch {
	case file.Checksum == "":
		// Every chunk matched the hash recorded at upload, so the reassembled
		// content is the uploaded content and its checksum can be recorded now.
		if err := s.repo.SetFileChecksum(file.ID, check.ComputedChecksum); err != nil {
			s.logger.Warn("Failed to record backfilled checksum", zap.Uint("file_id", file.ID), zap.Error(err))
		} else {
			check.ExpectedChecksum = check.ComputedChecksum
			notes = append(notes, "No file checksum was recorded at upload; it was computed from hash-verified chunks and saved.")
		}
	case check.ComputedChecksum != file.Checksum:
		check.Status = model.CheckCorrupt
		check.Message = "Every chunk matches its hash, but the reassembled file does not match the checksum recorded at upload (chunk metadata is inconsistent)."
		return check
	}

	bad := check.ReplicasTotal - check.ReplicasOK
	if bad == 0 && underReplicated == 0 {
		check.Status = model.CheckVerified
		msg := fmt.Sprintf("All %d replica(s) of %d chunk(s) match their SHA-256; file checksum matches.", check.ReplicasTotal, len(file.Chunks))
		if len(file.Chunks) == 0 {
			msg = "File is empty and has no chunks; its checksum matches."
		}
		notes = append([]string{msg}, notes...)
	} else {
		check.Status = model.CheckDegraded
		msg := "File content is intact"
		if bad > 0 {
			msg += fmt.Sprintf("; %d of %d replica(s) failed verification", bad, check.ReplicasTotal)
		}
		if underReplicated > 0 {
			msg += fmt.Sprintf("; %d chunk(s) have fewer than %d intact replica(s)", underReplicated, target)
		}
		notes = append([]string{msg + "."}, notes...)
	}
	check.Message = strings.Join(notes, " ")
	return check
}

// checkChunkReplicas verifies every replica of a chunk concurrently. It returns
// one result per replica and the data of one intact replica (nil if none).
func (s *MetadataService) checkChunkReplicas(ctx context.Context, chunk model.Chunk, activeNodeMap map[string]string) ([]model.ReplicaCheck, []byte) {
	results := make([]model.ReplicaCheck, len(chunk.Replicas))
	data := make([][]byte, len(chunk.Replicas))
	var wg sync.WaitGroup
	for i, replica := range chunk.Replicas {
		results[i] = model.ReplicaCheck{
			ChunkSequence: chunk.Sequence,
			ChunkHash:     chunk.Hash,
			NodeID:        replica.NodeID,
		}
		address, ok := activeNodeMap[replica.NodeID]
		if !ok {
			results[i].Status = model.ReplicaOffline
			results[i].Detail = "Node has no live etcd lease (stopped or not sending heartbeats)."
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := s.downloadChunkFromNode(ctx, address, chunk.Hash)
			switch {
			case stderrors.Is(err, errors.ErrChunkNotFound):
				results[i].Status = model.ReplicaMissing
				results[i].Detail = "Node is up but does not have this chunk."
			case err != nil:
				results[i].Status = model.ReplicaError
				results[i].Detail = describeFetchError(err)
			default:
				actual := hashing.ComputeSHA256(got)
				if actual != chunk.Hash {
					results[i].Status = model.ReplicaCorrupt
					results[i].Detail = fmt.Sprintf("Content SHA-256 is %s…, expected %s….", actual[:12], chunk.Hash[:12])
					return
				}
				results[i].Status = model.ReplicaOK
				data[i] = got
			}
		}()
	}
	wg.Wait()

	for _, d := range data {
		if d != nil {
			return results, d
		}
	}
	return results, nil
}

// describeFetchError turns a chunk fetch error into a short message that does
// not leak request URLs or internal error text.
func describeFetchError(err error) string {
	var urlErr *url.Error
	var statusErr *nodeStatusError
	switch {
	case stderrors.As(err, &urlErr) && urlErr.Timeout():
		return "Node did not respond in time."
	case stderrors.As(err, &urlErr):
		return "Node could not be reached."
	case stderrors.As(err, &statusErr):
		return fmt.Sprintf("Node returned HTTP %d.", statusErr.code)
	default:
		return "Chunk could not be read from the node."
	}
}

// ListIntegrity returns every file with its latest integrity check.
func (s *MetadataService) ListIntegrity() ([]FileIntegrity, error) {
	files, err := s.repo.GetAllFiles()
	if err != nil {
		return nil, err
	}
	latest, err := s.repo.LatestChecks()
	if err != nil {
		return nil, err
	}
	out := make([]FileIntegrity, 0, len(files))
	for i := range files {
		fi := summarize(&files[i])
		if c, ok := latest[files[i].ID]; ok {
			fi.LastCheck = &c
		}
		out = append(out, fi)
	}
	return out, nil
}

// GetIntegrityReport returns a file's newest check with replica results, and its history.
func (s *MetadataService) GetIntegrityReport(fileID uint) (*IntegrityReport, error) {
	file, err := s.repo.GetFile(fileID)
	if err != nil {
		return nil, err
	}
	history, err := s.repo.GetCheckHistory(fileID, checkHistoryLimit)
	if err != nil {
		return nil, err
	}
	report := &IntegrityReport{File: summarize(file), History: history}
	if len(history) > 0 {
		report.File.LastCheck = &history[0]
	}
	return report, nil
}

// summarize collects file metadata and chunk/replica counts for integrity reports.
func summarize(f *model.File) FileIntegrity {
	fi := FileIntegrity{
		FileID:    f.ID,
		Filename:  f.Filename,
		SizeBytes: f.SizeBytes,
		Status:    f.Status,
		Checksum:  f.Checksum,
		Chunks:    len(f.Chunks),
		CreatedAt: f.CreatedAt,
	}
	for _, c := range f.Chunks {
		fi.Replicas += len(c.Replicas)
	}
	return fi
}

// Stats summarises files, replication health and integrity results.
func (s *MetadataService) Stats(ctx context.Context) (*ClusterStats, error) {
	stats := &ClusterStats{ReplicationFactor: s.repFactor, IntegrityByStatus: map[string]int{}}

	counts, err := s.repo.CountFilesByStatus()
	if err != nil {
		return nil, err
	}
	for _, c := range counts {
		stats.FilesTotal += c.Count
		switch c.Status {
		case model.FileStatusActive:
			stats.FilesActive = c.Count
			stats.LogicalBytes = c.Bytes
		case model.FileStatusFailed:
			stats.FilesFailed = c.Count
		case model.FileStatusPending:
			stats.FilesPending = c.Count
		}
	}

	chunks, err := s.repo.GetActiveChunks()
	if err != nil {
		return nil, err
	}
	stats.Chunks = len(chunks)
	for _, c := range chunks {
		stats.Replicas += len(c.Replicas)
	}

	live, nodes, err := s.liveAddresses(ctx)
	if err != nil {
		stats.DiscoveryError = "Could not query etcd for live storage nodes."
		s.logger.Warn("Stats: node discovery failed", zap.Error(err))
	} else {
		stats.LiveNodes = len(nodes)
		target := max(1, min(s.repFactor, len(nodes)))
		for _, c := range chunks {
			liveReplicas := 0
			for _, r := range c.Replicas {
				if _, ok := live[r.NodeID]; ok {
					liveReplicas++
				}
			}
			if liveReplicas == 0 {
				stats.UnavailableChunks++
			}
			if liveReplicas < target {
				stats.UnderReplicated++
			}
		}
	}

	latest, err := s.repo.LatestChecks()
	if err != nil {
		return nil, err
	}
	activeIDs, err := s.repo.ActiveFileIDs()
	if err != nil {
		return nil, err
	}
	for _, id := range activeIDs {
		if c, ok := latest[id]; ok {
			stats.IntegrityByStatus[c.Status]++
		} else {
			stats.IntegrityByStatus["UNCHECKED"]++
		}
	}
	return stats, nil
}
