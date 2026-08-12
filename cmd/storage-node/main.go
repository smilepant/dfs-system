package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"dfs-system/internal/common/config"
	"dfs-system/internal/common/logger"
	"dfs-system/internal/coordinator"
	"dfs-system/internal/storage/handler"
	"dfs-system/internal/storage/node"
	"dfs-system/internal/storage/service"

	"github.com/gin-gonic/gin"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

func corsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept")
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}
func main() {
	configPath := flag.String("config", "configs/config.yaml", "Path to config file")
	nodeID := flag.String("id", "", "Overriding Storage Node ID")
	port := flag.Int("port", 0, "Overriding port")
	storageDir := flag.String("dir", "", "Overriding storage directory")
	etcdEndpoints := flag.String("etcd", "", "Comma-separated etcd endpoints")
	metadataURL := flag.String("metadata-url", "", "Overriding metadata server URL")
	flag.Parse()

	// 1. Initialize Logger
	log, err := logger.InitLogger(true) // dev mode = true
	if err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Sync()

	// 2. Load Config
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Warn("Failed to load config file, using CLI flags / defaults", zap.String("path", *configPath), zap.Error(err))
		cfg = &config.Config{}
	}

	// Override config with flags if provided
	if *nodeID != "" {
		cfg.Storage.NodeID = *nodeID
	}
	if *port != 0 {
		cfg.Storage.Port = *port
	}
	if *storageDir != "" {
		cfg.Storage.StorageRoot = *storageDir
	}
	if *etcdEndpoints != "" {
		cfg.Storage.EtcdEndpoints = []string{*etcdEndpoints}
	}
	if *metadataURL != "" {
		cfg.Storage.MetadataServerURL = *metadataURL
	}

	// Set defaults if config is empty
	if cfg.Storage.NodeID == "" {
		cfg.Storage.NodeID = "storage-node-1"
	}
	if cfg.Storage.Port == 0 {
		cfg.Storage.Port = 8081
	}
	if cfg.Storage.StorageRoot == "" {
		cfg.Storage.StorageRoot = "./data-node-1"
	}
	if len(cfg.Storage.EtcdEndpoints) == 0 {
		cfg.Storage.EtcdEndpoints = []string{"localhost:2379"}
	}
	if cfg.Storage.MetadataServerURL == "" {
		cfg.Storage.MetadataServerURL = "http://localhost:8000"
	}
	if cfg.Storage.HeartbeatInterval == 0 {
		cfg.Storage.HeartbeatInterval = 5
	}

	log.Info("Starting Storage Node...",
		zap.String("node_id", cfg.Storage.NodeID),
		zap.Int("port", cfg.Storage.Port),
		zap.String("storage_root", cfg.Storage.StorageRoot),
	)

	// 3. Initialize Storage Service
	fs := afero.NewOsFs()
	store, err := service.NewStorageService(fs, cfg.Storage.StorageRoot)
	if err != nil {
		log.Fatal("Failed to initialize storage service", zap.Error(err))
	}

	// 4. Initialize etcd Coordinator
	etcdCoord, err := coordinator.NewCoordinator(cfg.Storage.EtcdEndpoints)
	if err != nil {
		log.Fatal("Failed to initialize etcd coordinator", zap.Error(err))
	}
	defer etcdCoord.Close()

	// 5. Initialize & Start Worker
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Determine address registration format
	nodeAddress := fmt.Sprintf("%s:%d", cfg.Storage.NodeID, cfg.Storage.Port)
	if os.Getenv("RUNNING_IN_DOCKER") != "true" {
		nodeAddress = fmt.Sprintf("localhost:%d", cfg.Storage.Port)
	}

	worker := node.NewWorker(
		cfg.Storage.NodeID,
		nodeAddress,
		cfg.Storage.MetadataServerURL,
		cfg.Storage.HeartbeatInterval,
		store,
		etcdCoord,
		log,
	)
	worker.Start(ctx)

	// 6. Setup Gin Router
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	// CORS
	r.Use(corsMiddleware())

	// Logger middleware for Gin
	r.Use(func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery
		c.Next()
		log.Info("Request Log",
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("query", query),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("duration", time.Since(start)),
		)
	})

	h := handler.NewStorageHandler(store, cfg.Storage.NodeID, log)

	r.POST("/chunk/upload", h.UploadChunk)
	r.GET("/chunk/:id", h.DownloadChunk)
	r.GET("/health", h.Health)

	// 7. Start HTTP Server
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Storage.Port),
		Handler: r,
	}

	go func() {
		log.Info("HTTP server listening", zap.Int("port", cfg.Storage.Port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info("Shutting down Storage Node...")

	cancel() // Stop the background worker

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Fatal("Server forced to shutdown", zap.Error(err))
	}

	log.Info("Storage Node stopped.")
}
