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
	err := r.db.Find(&nodes).Error
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
