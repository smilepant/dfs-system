package service

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"dfs-system/internal/metadata/model"
	"dfs-system/pkg/errors"

	"go.uber.org/zap"
)

// Repair actions taken on one replica.
const (
	RepairRewritten = "REWRITTEN" // corrupt/missing replica restored in place from an intact copy
	RepairAdded     = "ADDED"     // new replica placed on another node to meet the replication target
	RepairFailed    = "FAILED"    // a rewrite or placement was attempted and failed
	RepairSkipped   = "SKIPPED"   // nothing could be done (node unreachable, or no intact copy exists)
)

// RepairAction describes what repair did for one replica of one chunk.
type RepairAction struct {
	ChunkSequence int    `json:"chunk_sequence"`
	NodeID        string `json:"node_id,omitempty"`
	Action        string `json:"action"`
	Detail        string `json:"detail"`
}

// RepairResult is the outcome of RepairFile, including the check run afterwards.
type RepairResult struct {
	FileID              uint                  `json:"file_id"`
	ReplicasRewritten   int                   `json:"replicas_rewritten"`
	ReplicasAdded       int                   `json:"replicas_added"`
	ChunksUnrecoverable int                   `json:"chunks_unrecoverable"`
	Actions             []RepairAction        `json:"actions"`
	Summary             string                `json:"summary"`
	Check               *model.IntegrityCheck `json:"check"`
}

// RepairFile restores damaged replicas of an ACTIVE file from intact ones:
//   - CORRUPT or MISSING replicas on live nodes are rewritten in place (the
//     storage node replaces a copy whose content no longer matches its hash);
//   - chunks still below the replication target get new replicas on other live nodes.
//
// Every copy is made from data whose SHA-256 was just verified, and storage
// nodes verify it again on write. Replicas on offline nodes are left alone (the
// node may return with intact data). A chunk with no intact replica anywhere
// cannot be recovered. Afterwards the file is verified again and that check is stored.
func (s *MetadataService) RepairFile(ctx context.Context, fileID uint) (*RepairResult, error) {
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

	s.mutationMu.RLock()
	result, err := s.repairChunks(ctx, file)
	if err == nil {
		// Reload so the follow-up check sees the replicas added above.
		var reloaded *model.File
		if reloaded, err = s.repo.GetFile(fileID); err == nil {
			result.Check = s.runCheck(ctx, reloaded)
		}
	}
	s.mutationMu.RUnlock()
	if err != nil {
		s.recordOperation(model.OpRepair, file.ID, file.Filename, model.OpFailed, err.Error(), file.SizeBytes)
		return nil, err
	}

	if err := s.repo.SaveIntegrityCheck(result.Check); err != nil {
		return nil, fmt.Errorf("failed to save integrity check: %w", err)
	}
	result.Summary = summarizeRepair(result)
	s.recordOperation(model.OpRepair, file.ID, file.Filename, result.Check.Status, result.Summary, file.SizeBytes)
	s.logger.Info("File repair finished",
		zap.Uint("file_id", file.ID),
		zap.Int("rewritten", result.ReplicasRewritten),
		zap.Int("added", result.ReplicasAdded),
		zap.Int("unrecoverable_chunks", result.ChunksUnrecoverable),
		zap.String("status", result.Check.Status),
	)
	return result, nil
}

func (s *MetadataService) repairChunks(ctx context.Context, file *model.File) (*RepairResult, error) {
	activeNodeMap, nodes, err := s.liveAddresses(ctx)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, errors.ErrNoActiveNodes
	}
	target := max(1, min(s.repFactor, len(nodes)))
	result := &RepairResult{FileID: file.ID, Actions: []RepairAction{}}
	place := newPlacement(nodes)

	for _, chunk := range file.Chunks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		checks, data := s.checkChunkReplicas(ctx, chunk, activeNodeMap)
		if data == nil {
			result.ChunksUnrecoverable++
			result.Actions = append(result.Actions, RepairAction{
				ChunkSequence: chunk.Sequence,
				Action:        RepairSkipped,
				Detail:        "No intact replica is reachable; this chunk cannot be restored until a node holding a good copy returns.",
			})
			continue
		}

		intact := 0
		holders := make(map[string]bool, len(checks))
		var rewrite []model.ReplicaCheck
		for _, c := range checks {
			holders[c.NodeID] = true
			switch c.Status {
			case model.ReplicaOK:
				intact++
			case model.ReplicaCorrupt, model.ReplicaMissing:
				rewrite = append(rewrite, c)
			case model.ReplicaError:
				result.Actions = append(result.Actions, RepairAction{
					ChunkSequence: chunk.Sequence, NodeID: c.NodeID, Action: RepairSkipped,
					Detail: "Node could not be read, so it was not rewritten. " + c.Detail,
				})
			}
		}

		// Rewrite damaged copies in place, concurrently.
		errs := make([]error, len(rewrite))
		var wg sync.WaitGroup
		for i, c := range rewrite {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = s.uploadChunkToNode(ctx, activeNodeMap[c.NodeID], chunk.Hash, data)
			}()
		}
		wg.Wait()
		for i, c := range rewrite {
			action := RepairAction{ChunkSequence: chunk.Sequence, NodeID: c.NodeID}
			if errs[i] != nil {
				s.logger.Warn("Repair: rewriting replica failed", zap.String("node_id", c.NodeID), zap.String("hash", chunk.Hash), zap.Error(errs[i]))
				action.Action, action.Detail = RepairFailed, "Rewrite failed: "+describeFetchError(errs[i])
			} else {
				intact++
				result.ReplicasRewritten++
				action.Action, action.Detail = RepairRewritten, fmt.Sprintf("Replaced %s replica with a hash-verified copy.", strings.ToLower(c.Status))
			}
			result.Actions = append(result.Actions, action)
		}

		// Top up chunks still short of the target on nodes that don't hold them yet.
		if missing := target - intact; missing > 0 {
			added, err := s.replicateChunk(ctx, chunk.Hash, data, place.candidates(chunk.SizeBytes, holders), missing)
			place.record(added, chunk.SizeBytes)
			if len(added) > 0 {
				if dbErr := s.repo.AddReplicas(chunk.ID, added); dbErr != nil {
					return nil, fmt.Errorf("failed to record new replicas: %w", dbErr)
				}
			}
			for _, r := range added {
				result.ReplicasAdded++
				result.Actions = append(result.Actions, RepairAction{
					ChunkSequence: chunk.Sequence, NodeID: r.NodeID, Action: RepairAdded,
					Detail: "Placed a new hash-verified replica to restore the replication target.",
				})
			}
			if err != nil {
				result.Actions = append(result.Actions, RepairAction{
					ChunkSequence: chunk.Sequence, Action: RepairFailed,
					Detail: fmt.Sprintf("Still %d intact replica(s), target %d: no other live node accepted the chunk.", intact+len(added), target),
				})
			}
		}
	}
	return result, nil
}

func summarizeRepair(r *RepairResult) string {
	var parts []string
	if r.ReplicasRewritten > 0 {
		parts = append(parts, fmt.Sprintf("%d replica(s) rewritten", r.ReplicasRewritten))
	}
	if r.ReplicasAdded > 0 {
		parts = append(parts, fmt.Sprintf("%d replica(s) added", r.ReplicasAdded))
	}
	if r.ChunksUnrecoverable > 0 {
		parts = append(parts, fmt.Sprintf("%d chunk(s) unrecoverable", r.ChunksUnrecoverable))
	}
	if len(parts) == 0 {
		parts = append(parts, "nothing needed repair")
	}
	return strings.Join(parts, ", ") + "; file is now " + strings.ToLower(r.Check.Status) + "."
}
