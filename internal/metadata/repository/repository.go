package repository

import (
	"time"

	"dfs-system/internal/metadata/model"

	"gorm.io/gorm"
)

type Repository struct {
	db *gorm.DB
}

// NewRepository creates a new Repository.
func NewRepository(db *gorm.DB) *Repository {
	return &Repository{db: db}
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
func (r *Repository) GetFile(id uint) (*model.File, error) {
	var file model.File
	err := r.db.Preload("Chunks.Replicas").First(&file, id).Error
	if err != nil {
		return nil, err
	}
	return &file, nil
}

// GetAllFiles returns all file metadata stored in the system.
func (r *Repository) GetAllFiles() ([]model.File, error) {
	var files []model.File
	err := r.db.Preload("Chunks.Replicas").Find(&files).Error
	return files, err
}

// RegisterNode upserts storage node registry.
func (r *Repository) RegisterNode(node *model.StorageNode) error {
	node.LastSeen = time.Now()
	node.IsActive = true

	var existing model.StorageNode
	err := r.db.Where("node_id = ?", node.NodeID).First(&existing).Error
	if err == gorm.ErrRecordNotFound {
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
