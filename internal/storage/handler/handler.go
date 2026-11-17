package handler

import (
	"errors"
	"io"
	"net/http"
	"os"

	"dfs-system/internal/storage/service"
	"dfs-system/pkg/chunking"
	dfserrors "dfs-system/pkg/errors"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// maxChunkBody bounds a single chunk upload so a client cannot exhaust node memory.
const maxChunkBody = chunking.DefaultChunkSize

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

// writeStoreError converts chunk storage errors into HTTP error responses.
func (h *StorageHandler) writeStoreError(c *gin.Context, hash string, err error) {
	switch {
	case errors.Is(err, dfserrors.ErrInvalidChunkID):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	case errors.Is(err, dfserrors.ErrChunkCorrupted):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	case errors.Is(err, dfserrors.ErrInsufficientSpace):
		c.JSON(http.StatusInsufficientStorage, gin.H{"error": err.Error()})
	case errors.Is(err, os.ErrNotExist):
		c.JSON(http.StatusNotFound, gin.H{"error": "chunk not found"})
	default:
		h.logger.Error("Chunk operation failed", zap.String("hash", hash), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	}
}

// UploadChunk reads a bounded upload and stores it after checking its hash.
func (h *StorageHandler) UploadChunk(c *gin.Context) {
	hash := c.Query("hash")
	if hash == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing hash query parameter"})
		return
	}

	data, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, maxChunkBody))
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "chunk exceeds maximum size"})
			return
		}
		h.logger.Error("Upload chunk failed: failed to read request body", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": "failed to read body"})
		return
	}

	if err := h.store.SaveChunk(hash, data); err != nil {
		h.writeStoreError(c, hash, err)
		return
	}

	h.logger.Debug("Chunk stored", zap.String("node_id", h.nodeID), zap.String("hash", hash), zap.Int("size", len(data)))
	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "chunk stored"})
}

// DownloadChunk streams a stored chunk to the client.
func (h *StorageHandler) DownloadChunk(c *gin.Context) {
	hash := c.Param("id")
	file, info, err := h.store.OpenChunk(hash)
	if err != nil {
		h.writeStoreError(c, hash, err)
		return
	}
	defer file.Close()

	c.Header("Content-Type", "application/octet-stream")
	http.ServeContent(c.Writer, c.Request, "", info.ModTime(), file)
}

// DeleteChunk removes a chunk identified by its hash.
func (h *StorageHandler) DeleteChunk(c *gin.Context) {
	hash := c.Param("id")
	if err := h.store.DeleteChunk(hash); err != nil {
		h.writeStoreError(c, hash, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "chunk deleted"})
}

// Health reports the node's ID, capacity, and remaining free space.
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
