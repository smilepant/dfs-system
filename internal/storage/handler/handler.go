package handler

import (
	"errors"
	"io"
	"net/http"
	"os"

	"dfs-system/internal/storage/service"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type StorageHandler struct {
	store  *service.StorageService
	nodeID string
	logger *zap.Logger
}

// NewStorageHandler creates a handler for storage endpoints.
func NewStorageHandler(store *service.StorageService, nodeID string, logger *zap.Logger) *StorageHandler {
	return &StorageHandler{
		store:  store,
		nodeID: nodeID,
		logger: logger,
	}
}

// UploadChunk handles POST /chunk/upload?hash=<hash>
func (h *StorageHandler) UploadChunk(c *gin.Context) {
	hash := c.Query("hash")
	if hash == "" {
		h.logger.Error("Upload chunk failed: hash parameter is missing")
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing hash query parameter"})
		return
	}

	data, err := io.ReadAll(c.Request.Body)
	if err != nil {
		h.logger.Error("Upload chunk failed: failed to read request body", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read body"})
		return
	}

	err = h.store.SaveChunk(hash, data)
	if err != nil {
		h.logger.Error("Upload chunk failed: failed to save chunk", zap.String("hash", hash), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	h.logger.Info("Chunk uploaded successfully", zap.String("node_id", h.nodeID), zap.String("hash", hash), zap.Int("size", len(data)))
	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "chunk stored"})
}

// DownloadChunk handles GET /chunk/:id
func (h *StorageHandler) DownloadChunk(c *gin.Context) {
	hash := c.Param("id")
	if hash == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing chunk id"})
		return
	}

	data, err := h.store.GetChunk(hash)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			c.JSON(http.StatusNotFound, gin.H{"error": "chunk not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Data(http.StatusOK, "application/octet-stream", data)
}

// Health handles GET /health
func (h *StorageHandler) Health(c *gin.Context) {
	total, free, err := h.store.GetStats()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"status": "down", "error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"node_id":     h.nodeID,
		"status":      "UP",
		"total_space": total,
		"free_space":  free,
	})
}
