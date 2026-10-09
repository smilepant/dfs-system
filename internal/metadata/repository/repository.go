package repository

import (
	"errors"
	"time"

	"dfs-system/internal/metadata/model"
	dfserrors "dfs-system/pkg/errors"

	"gorm.io/gorm"
)

type Repository struct {
	db *gorm.DB
}

// NewRepository creates a new Repository.
func NewRepository(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

// preloadChunks loads chunks in sequence order (required for reassembly) with their replicas.
func preloadChunks(db *gorm.DB) *gorm.DB {
	return db.Preload("Chunks", func(tx *gorm.DB) *gorm.DB {
		return tx.Order("sequence ASC")
	}).Preload("Chunks.Replicas")
}

// CreateFile saves a File metadata along with its Chunks to the database.
func (r *Repository) CreateFile(file *model.File) error {
	return r.db.Create(file).Error
}

// SaveFileWithAssociations saves/updates a File and all associated Chunks/Replicas in a single operation.
func (r *Repository) SaveFileWithAssociations(file *model.File) error {
	return r.db.Session(&gorm.Session{FullSaveAssociations: true}).Save(file).Error
}

// UpdateFileStatus updates the file's processing status (PENDING, ACTIVE, etc.).
func (r *Repository) UpdateFileStatus(id uint, status string) error {
	return r.db.Model(&model.File{}).Where("id = ?", id).Update("status", status).Error
}

// GetFile retrieves a file with preloaded chunks and chunk replicas.
// Returns errors.ErrFileNotFound if no such file exists.
func (r *Repository) GetFile(id uint) (*model.File, error) {
	var file model.File
	err := preloadChunks(r.db).First(&file, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, dfserrors.ErrFileNotFound
	}
	if err != nil {
		return nil, err
	}
	return &file, nil
}

// GetAllFiles returns all file metadata stored in the system, newest first.
func (r *Repository) GetAllFiles() ([]model.File, error) {
	var files []model.File
	err := preloadChunks(r.db).Order("id DESC").Find(&files).Error
	return files, err
}

// DeleteFile removes a file and all of its chunk and replica rows in one
// transaction, returning the deleted file (with chunks/replicas) so the caller
// can clean up chunk data on storage nodes.
func (r *Repository) DeleteFile(id uint) (*model.File, error) {
	var file model.File
	err := r.db.Transaction(func(tx *gorm.DB) error {
		if err := preloadChunks(tx).First(&file, id).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return dfserrors.ErrFileNotFound
			}
			return err
		}
		checkIDs := tx.Model(&model.IntegrityCheck{}).Select("id").Where("file_id = ?", id)
		if err := tx.Where("check_id IN (?)", checkIDs).Delete(&model.ReplicaCheck{}).Error; err != nil {
			return err
		}
		if err := tx.Where("file_id = ?", id).Delete(&model.IntegrityCheck{}).Error; err != nil {
			return err
		}
		chunkIDs := tx.Model(&model.Chunk{}).Select("id").Where("file_id = ?", id)
		if err := tx.Where("chunk_id IN (?)", chunkIDs).Delete(&model.ChunkReplica{}).Error; err != nil {
			return err
		}
		if err := tx.Where("file_id = ?", id).Delete(&model.Chunk{}).Error; err != nil {
			return err
		}
		return tx.Delete(&model.File{}, id).Error
	})
	if err != nil {
		return nil, err
	}
	return &file, nil
}

// CountReplicaRefs returns how many replica rows still reference a chunk hash on
// a given node. Chunks are content-addressed and shared across files, so chunk
// data may only be deleted from a node when this reaches zero.
func (r *Repository) CountReplicaRefs(hash, nodeID string) (int64, error) {
	var count int64
	err := r.db.Model(&model.ChunkReplica{}).
		Joins("JOIN chunks ON chunks.id = chunk_replicas.chunk_id").
		Where("chunks.hash = ? AND chunk_replicas.node_id = ?", hash, nodeID).
		Count(&count).Error
	return count, err
}

// GetActiveChunks returns every chunk belonging to an ACTIVE file, with replicas.
func (r *Repository) GetActiveChunks() ([]model.Chunk, error) {
	var chunks []model.Chunk
	err := r.db.Preload("Replicas").
		Joins("JOIN files ON files.id = chunks.file_id").
		Where("files.status = ?", model.FileStatusActive).
		Find(&chunks).Error
	return chunks, err
}

// AddReplicas records additional replica locations for an existing chunk.
func (r *Repository) AddReplicas(chunkID uint, replicas []model.ChunkReplica) error {
	if len(replicas) == 0 {
		return nil
	}
	for i := range replicas {
		replicas[i].ID = 0
		replicas[i].ChunkID = chunkID
	}
	return r.db.Create(&replicas).Error
}

// SetFileChecksum records the whole-file SHA-256 for a file that has none yet.
func (r *Repository) SetFileChecksum(id uint, checksum string) error {
	return r.db.Model(&model.File{}).Where("id = ? AND (checksum IS NULL OR checksum = '')", id).
		Update("checksum", checksum).Error
}

// ActiveFileIDs returns the IDs of all ACTIVE files.
func (r *Repository) ActiveFileIDs() ([]uint, error) {
	var ids []uint
	err := r.db.Model(&model.File{}).Where("status = ?", model.FileStatusActive).Pluck("id", &ids).Error
	return ids, err
}

// FileStatusCount is the number and total size of files in one status.
type FileStatusCount struct {
	Status string
	Count  int64
	Bytes  int64
}

// CountFilesByStatus returns file counts and total sizes grouped by status.
func (r *Repository) CountFilesByStatus() ([]FileStatusCount, error) {
	var rows []FileStatusCount
	err := r.db.Model(&model.File{}).
		Select("status, COUNT(*) AS count, COALESCE(SUM(size_bytes), 0) AS bytes").
		Group("status").Scan(&rows).Error
	return rows, err
}

// CountReplicasByNode returns how many chunk replica rows each node holds.
func (r *Repository) CountReplicasByNode() (map[string]int64, error) {
	var rows []struct {
		NodeID string
		Count  int64
	}
	err := r.db.Model(&model.ChunkReplica{}).
		Select("node_id, COUNT(*) AS count").Group("node_id").Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(rows))
	for _, row := range rows {
		out[row.NodeID] = row.Count
	}
	return out, nil
}

// maxChecksPerFile bounds how much integrity history is kept for each file.
const maxChecksPerFile = 20

// SaveIntegrityCheck stores a check with its replica results and prunes the
// file's oldest checks beyond maxChecksPerFile.
func (r *Repository) SaveIntegrityCheck(check *model.IntegrityCheck) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(check).Error; err != nil {
			return err
		}
		stale := tx.Model(&model.IntegrityCheck{}).Select("id").
			Where("file_id = ?", check.FileID).Order("id DESC").Offset(maxChecksPerFile).Limit(-1)
		var staleIDs []uint
		if err := stale.Pluck("id", &staleIDs).Error; err != nil {
			return err
		}
		if len(staleIDs) == 0 {
			return nil
		}
		if err := tx.Where("check_id IN ?", staleIDs).Delete(&model.ReplicaCheck{}).Error; err != nil {
			return err
		}
		return tx.Where("id IN ?", staleIDs).Delete(&model.IntegrityCheck{}).Error
	})
}

// LatestChecks returns the most recent integrity check (without replica rows)
// for every file that has been checked, keyed by file ID.
func (r *Repository) LatestChecks() (map[uint]model.IntegrityCheck, error) {
	var checks []model.IntegrityCheck
	latest := r.db.Model(&model.IntegrityCheck{}).Select("MAX(id)").Group("file_id")
	if err := r.db.Where("id IN (?)", latest).Find(&checks).Error; err != nil {
		return nil, err
	}
	out := make(map[uint]model.IntegrityCheck, len(checks))
	for _, c := range checks {
		out[c.FileID] = c
	}
	return out, nil
}

// GetCheckHistory returns a file's integrity checks, newest first. Only the
// newest check has its replica results loaded.
func (r *Repository) GetCheckHistory(fileID uint, limit int) ([]model.IntegrityCheck, error) {
	var checks []model.IntegrityCheck
	err := r.db.Where("file_id = ?", fileID).Order("id DESC").Limit(limit).Find(&checks).Error
	if err != nil || len(checks) == 0 {
		return checks, err
	}
	err = r.db.Where("check_id = ?", checks[0].ID).
		Order("chunk_sequence ASC, node_id ASC").Find(&checks[0].Replicas).Error
	return checks, err
}

// maxOperations bounds the size of the activity log.
const maxOperations = 1000

// CreateOperation appends an entry to the activity log, pruning the oldest
// entries beyond maxOperations.
func (r *Repository) CreateOperation(op *model.Operation) error {
	if err := r.db.Create(op).Error; err != nil {
		return err
	}
	if op.ID > maxOperations && op.ID%100 == 0 {
		return r.db.Where("id <= ?", op.ID-maxOperations).Delete(&model.Operation{}).Error
	}
	return nil
}

// ListOperations returns the newest activity log entries, optionally filtered by type.
func (r *Repository) ListOperations(opType string, limit int) ([]model.Operation, error) {
	var ops []model.Operation
	q := r.db.Order("id DESC").Limit(limit)
	if opType != "" {
		q = q.Where("type = ?", opType)
	}
	err := q.Find(&ops).Error
	return ops, err
}

// RegisterNode upserts storage node registry.
func (r *Repository) RegisterNode(node *model.StorageNode) error {
	node.LastSeen = time.Now()
	node.IsActive = true

	var existing model.StorageNode
	err := r.db.Where("node_id = ?", node.NodeID).First(&existing).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return r.db.Create(node).Error
	} else if err != nil {
		return err
	}

	existing.Address = node.Address
	existing.TotalSpace = node.TotalSpace
	existing.FreeSpace = node.FreeSpace
	existing.LastSeen = node.LastSeen
	existing.IsActive = node.IsActive
	return r.db.Save(&existing).Error
}

// GetNode gets a node from the database.
func (r *Repository) GetNode(nodeID string) (*model.StorageNode, error) {
	var node model.StorageNode
	err := r.db.First(&node, "node_id = ?", nodeID).Error
	if err != nil {
		return nil, err
	}
	return &node, nil
}

// GetAllNodes returns all storage nodes in the database.
func (r *Repository) GetAllNodes() ([]model.StorageNode, error) {
	var nodes []model.StorageNode
	err := r.db.Order("node_id ASC").Find(&nodes).Error
	return nodes, err
}

// UpdateNodeHeartbeat updates the storage node status.
func (r *Repository) UpdateNodeHeartbeat(nodeID string, freeSpace int64) error {
	return r.db.Model(&model.StorageNode{}).
		Where("node_id = ?", nodeID).
		Updates(map[string]interface{}{
			"free_space": freeSpace,
			"last_seen":  time.Now(),
			"is_active":  true,
		}).Error
}
