package handler

import (
	"fmt"
	"net/http"
	"strconv"

	"dfs-system/internal/common"
	"dfs-system/internal/metadata/model"
	"dfs-system/internal/metadata/service"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type MetadataHandler struct {
	svc    *service.MetadataService
	logger *zap.Logger
}

// NewMetadataHandler creates a new MetadataHandler.
func NewMetadataHandler(svc *service.MetadataService, logger *zap.Logger) *MetadataHandler {
	return &MetadataHandler{
		svc:    svc,
		logger: logger,
	}
}

// Upload handles POST /upload
func (h *MetadataHandler) Upload(c *gin.Context) {
	fileHeader, err := c.FormFile("file")
	if err != nil {
		h.logger.Error("Upload failed: missing file parameter", zap.Error(err))
		c.JSON(http.StatusBadRequest, gin.H{"error": "missing file parameter in form-data"})
		return
	}

	fileReader, err := fileHeader.Open()
	if err != nil {
		h.logger.Error("Upload failed: cannot open file header reader", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to open uploaded file"})
		return
	}
	defer fileReader.Close()

	fileModel, err := h.svc.UploadFile(c.Request.Context(), fileHeader.Filename, fileReader, fileHeader.Size)
	if err != nil {
		h.logger.Error("Upload failed: service error", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, fileModel)
}

// Download handles GET /download/:fileId
func (h *MetadataHandler) Download(c *gin.Context) {
	fileIDStr := c.Param("fileId")
	fileID, err := strconv.ParseUint(fileIDStr, 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid file id"})
		return
	}

	fileMeta, err := h.svc.GetFileMetadata(uint(fileID))
	if err != nil {
		h.logger.Error("Download failed: file metadata not found", zap.Uint("file_id", uint(fileID)), zap.Error(err))
		c.JSON(http.StatusNotFound, gin.H{"error": "file not found"})
		return
	}

	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=%s", fileMeta.Filename))
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Length", strconv.FormatInt(fileMeta.SizeBytes, 10))

	err = h.svc.DownloadFile(c.Request.Context(), uint(fileID), c.Writer)
	if err != nil {
		h.logger.Error("Download failed: streaming error", zap.Uint("file_id", uint(fileID)), zap.Error(err))
		return
	}
}

// ListFiles handles GET /files
func (h *MetadataHandler) ListFiles(c *gin.Context) {
	files, err := h.svc.ListFiles()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, files)
}

// RegisterNode handles POST /nodes/register
func (h *MetadataHandler) RegisterNode(c *gin.Context) {
	var req common.NodeRegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	nodeModel := &model.StorageNode{
		NodeID:     req.NodeID,
		Address:    req.Address,
		TotalSpace: req.TotalSpace,
		FreeSpace:  req.FreeSpace,
	}

	err := h.svc.RegisterNode(nodeModel)
	if err != nil {
		h.logger.Error("Node registration failed in GORM DB", zap.String("node_id", req.NodeID), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	h.logger.Info("Storage node registered in GORM DB successfully", zap.String("node_id", req.NodeID), zap.String("address", req.Address))
	c.JSON(http.StatusOK, common.NodeRegisterResponse{
		Status:  "success",
		Message: "node registered successfully in GORM DB",
	})
}

// NodesHealth handles GET /nodes/health — proxies health checks to all registered storage nodes.
func (h *MetadataHandler) NodesHealth(c *gin.Context) {
	results := h.svc.GetAllNodeHealth(c.Request.Context())
	c.JSON(http.StatusOK, results)
}
