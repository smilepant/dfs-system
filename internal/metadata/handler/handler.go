package handler

import (
	"context"
	"errors"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"dfs-system/internal/common"
	"dfs-system/internal/metadata/model"
	"dfs-system/internal/metadata/service"
	dfserrors "dfs-system/pkg/errors"

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

// statusFor maps domain errors to HTTP status codes.
func statusFor(err error) int {
	switch {
	case errors.Is(err, dfserrors.ErrFileNotFound):
		return http.StatusNotFound
	case errors.Is(err, dfserrors.ErrFileNotReady),
		errors.Is(err, dfserrors.ErrCheckInProgress):
		return http.StatusConflict
	case errors.Is(err, dfserrors.ErrNoActiveNodes),
		errors.Is(err, dfserrors.ErrRepFactorUnmet),
		errors.Is(err, dfserrors.ErrNodeUnavailable),
		errors.Is(err, dfserrors.ErrChunkNotFound),
		errors.Is(err, dfserrors.ErrChunkCorrupted):
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func parseFileID(c *gin.Context) (uint, bool) {
	fileID, err := strconv.ParseUint(c.Param("fileId"), 10, 32)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid file id"})
		return 0, false
	}
	return uint(fileID), true
}

// Upload handles POST /upload
func (h *MetadataHandler) Upload(c *gin.Context) {
	fileHeader, err := c.FormFile("file")
	if err != nil {
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

	fileModel, err := h.svc.UploadFile(c.Request.Context(), fileHeader.Filename, fileReader)
	if err != nil {
		h.logger.Error("Upload failed: service error", zap.Error(err))
		c.JSON(statusFor(err), gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, fileModel)
}

// Download handles GET /download/:fileId
func (h *MetadataHandler) Download(c *gin.Context) {
	fileID, ok := parseFileID(c)
	if !ok {
		return
	}

	fileMeta, err := h.svc.GetFileMetadata(fileID)
	if err != nil {
		c.JSON(statusFor(err), gin.H{"error": err.Error()})
		return
	}
	if fileMeta.Status != model.FileStatusActive {
		c.JSON(http.StatusConflict, gin.H{"error": dfserrors.ErrFileNotReady.Error(), "status": fileMeta.Status})
		return
	}

	header := c.Writer.Header()
	header.Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": fileMeta.Filename}))
	header.Set("Content-Type", "application/octet-stream")
	header.Set("Content-Length", strconv.FormatInt(fileMeta.SizeBytes, 10))
	if fileMeta.Checksum != "" {
		header.Set("X-Checksum-SHA256", fileMeta.Checksum)
	}

	err = h.svc.StreamFile(c.Request.Context(), fileMeta, c.Writer)
	if err == nil {
		return
	}
	h.logger.Error("Download failed", zap.Uint("file_id", fileID), zap.Error(err))
	if !c.Writer.Written() {
		// Nothing sent yet: replace the file headers with a proper error response.
		header.Del("Content-Disposition")
		header.Del("Content-Length")
		header.Del("Content-Type")
		header.Del("X-Checksum-SHA256")
		c.JSON(statusFor(err), gin.H{"error": err.Error()})
	}
	// Otherwise the body is shorter than Content-Length; net/http closes the
	// connection so the client sees a truncated transfer, not a corrupt file.
}

// GetFile handles GET /files/:fileId
func (h *MetadataHandler) GetFile(c *gin.Context) {
	fileID, ok := parseFileID(c)
	if !ok {
		return
	}
	file, err := h.svc.GetFileMetadata(fileID)
	if err != nil {
		c.JSON(statusFor(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, file)
}

// DeleteFile handles DELETE /files/:fileId
func (h *MetadataHandler) DeleteFile(c *gin.Context) {
	fileID, ok := parseFileID(c)
	if !ok {
		return
	}
	if err := h.svc.DeleteFile(c.Request.Context(), fileID); err != nil {
		if !errors.Is(err, dfserrors.ErrFileNotFound) {
			h.logger.Error("Delete failed", zap.Uint("file_id", fileID), zap.Error(err))
		}
		c.JSON(statusFor(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "success", "message": "file deleted"})
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

	h.logger.Info("Storage node registered", zap.String("node_id", req.NodeID), zap.String("address", req.Address))
	c.JSON(http.StatusOK, common.NodeRegisterResponse{
		Status:  "success",
		Message: "node registered successfully",
	})
}

// NodesHealth handles GET /nodes/health — proxies health checks to all registered storage nodes.
func (h *MetadataHandler) NodesHealth(c *gin.Context) {
	results := h.svc.GetAllNodeHealth(c.Request.Context())
	c.JSON(http.StatusOK, results)
}

// verifyTimeout bounds a single integrity check.
const verifyTimeout = 10 * time.Minute

// VerifyFile handles POST /files/:fileId/verify. The check keeps running if the
// client disconnects, so its result is always recorded.
func (h *MetadataHandler) VerifyFile(c *gin.Context) {
	fileID, ok := parseFileID(c)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), verifyTimeout)
	defer cancel()

	check, err := h.svc.VerifyFile(ctx, fileID)
	if err != nil {
		if statusFor(err) == http.StatusInternalServerError {
			h.logger.Error("Integrity check failed", zap.Uint("file_id", fileID), zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "integrity check could not be completed"})
			return
		}
		c.JSON(statusFor(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, check)
}

// RepairFile handles POST /files/:fileId/repair. Like VerifyFile, it keeps
// running if the client disconnects so a half-finished repair is never abandoned.
func (h *MetadataHandler) RepairFile(c *gin.Context) {
	fileID, ok := parseFileID(c)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), verifyTimeout)
	defer cancel()

	result, err := h.svc.RepairFile(ctx, fileID)
	if err != nil {
		if statusFor(err) == http.StatusInternalServerError {
			h.logger.Error("Repair failed", zap.Uint("file_id", fileID), zap.Error(err))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "repair could not be completed"})
			return
		}
		c.JSON(statusFor(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, result)
}

// FileIntegrity handles GET /files/:fileId/integrity
func (h *MetadataHandler) FileIntegrity(c *gin.Context) {
	fileID, ok := parseFileID(c)
	if !ok {
		return
	}
	report, err := h.svc.GetIntegrityReport(fileID)
	if err != nil {
		c.JSON(statusFor(err), gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, report)
}

// ListIntegrity handles GET /integrity
func (h *MetadataHandler) ListIntegrity(c *gin.Context) {
	files, err := h.svc.ListIntegrity()
	if err != nil {
		h.logger.Error("Listing integrity failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load integrity status"})
		return
	}
	c.JSON(http.StatusOK, files)
}

// Stats handles GET /stats
func (h *MetadataHandler) Stats(c *gin.Context) {
	stats, err := h.svc.Stats(c.Request.Context())
	if err != nil {
		h.logger.Error("Computing stats failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to compute cluster stats"})
		return
	}
	c.JSON(http.StatusOK, stats)
}

var operationTypes = map[string]bool{
	model.OpUpload: true, model.OpDownload: true, model.OpDelete: true, model.OpVerify: true, model.OpRepair: true,
}

// ListOperations handles GET /operations?type=UPLOAD&limit=50
func (h *MetadataHandler) ListOperations(c *gin.Context) {
	opType := strings.ToUpper(c.Query("type"))
	if opType != "" && !operationTypes[opType] {
		c.JSON(http.StatusBadRequest, gin.H{"error": "type must be one of UPLOAD, DOWNLOAD, DELETE, VERIFY, REPAIR"})
		return
	}
	limit := 50
	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "limit must be between 1 and 500"})
			return
		}
		limit = n
	}
	ops, err := h.svc.ListOperations(opType, limit)
	if err != nil {
		h.logger.Error("Listing operations failed", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load activity"})
		return
	}
	c.JSON(http.StatusOK, ops)
}
