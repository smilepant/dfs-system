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
	"dfs-system/internal/metadata/handler"
	"dfs-system/internal/metadata/model"
	"dfs-system/internal/metadata/repository"
	"dfs-system/internal/metadata/service"

	"github.com/glebarez/sqlite"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// corsMiddleware adds CORS headers so the web dashboard can call the API from any origin.
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
	port := flag.Int("port", 0, "Overriding port")
	dbPath := flag.String("db", "", "Overriding database path")
	etcdEndpoints := flag.String("etcd", "", "Comma-separated etcd endpoints")
	repFactor := flag.Int("rep", 0, "Overriding replication factor")
	webDir := flag.String("web", "./web", "Path to web dashboard directory")
	flag.Parse()

	// 1. Logger
	log, err := logger.InitLogger(true)
	if err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Sync()

	// 2. Load config
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Warn("Config file not found, using defaults / CLI flags", zap.Error(err))
		cfg = &config.Config{}
	}

	if *port != 0          { cfg.Metadata.Port = *port }
	if *dbPath != ""       { cfg.Metadata.DBPath = *dbPath }
	if *etcdEndpoints != "" { cfg.Metadata.EtcdEndpoints = []string{*etcdEndpoints} }
	if *repFactor != 0     { cfg.Metadata.ReplicationFactor = *repFactor }

	// Defaults
	if cfg.Metadata.Port == 0               { cfg.Metadata.Port = 8000 }
	if cfg.Metadata.DBPath == ""            { cfg.Metadata.DBPath = "./metadata.db" }
	if len(cfg.Metadata.EtcdEndpoints) == 0 { cfg.Metadata.EtcdEndpoints = []string{"localhost:2379"} }
	if cfg.Metadata.ReplicationFactor == 0  { cfg.Metadata.ReplicationFactor = 2 }

	log.Info("Starting Metadata Server",
		zap.Int("port", cfg.Metadata.Port),
		zap.String("db", cfg.Metadata.DBPath),
		zap.Strings("etcd", cfg.Metadata.EtcdEndpoints),
		zap.Int("rep_factor", cfg.Metadata.ReplicationFactor),
	)

	// 3. Database + auto-migrate
	gormDB, err := gorm.Open(sqlite.Open(cfg.Metadata.DBPath), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to open database", zap.Error(err))
	}
	if err := gormDB.AutoMigrate(&model.File{}, &model.Chunk{}, &model.ChunkReplica{}, &model.StorageNode{}); err != nil {
		log.Fatal("Database migration failed", zap.Error(err))
	}
	log.Info("Database migrated")

	repo := repository.NewRepository(gormDB)

	// 4. etcd coordinator — retry until available
	var etcdCoord *coordinator.Coordinator
	for attempt := 1; attempt <= 10; attempt++ {
		etcdCoord, err = coordinator.NewCoordinator(cfg.Metadata.EtcdEndpoints)
		if err == nil {
			break
		}
		log.Warn("etcd not ready, retrying…", zap.Int("attempt", attempt), zap.Error(err))
		time.Sleep(2 * time.Second)
	}
	if etcdCoord == nil {
		log.Fatal("Could not connect to etcd after retries")
	}
	defer etcdCoord.Close()
	log.Info("Connected to etcd", zap.Strings("endpoints", cfg.Metadata.EtcdEndpoints))

	// 5. Service + handler
	svc := service.NewMetadataService(repo, etcdCoord, cfg.Metadata.ReplicationFactor, log)
	h := handler.NewMetadataHandler(svc, log)

	// 6. Gin router
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(corsMiddleware())

	// Zap request logger
	r.Use(func(c *gin.Context) {
		start := time.Now()
		c.Next()
		log.Info("HTTP",
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("latency", time.Since(start)),
		)
	})

	// API routes
	r.POST("/upload", h.Upload)
	r.GET("/download/:fileId", h.Download)
	r.GET("/files", h.ListFiles)
	r.POST("/nodes/register", h.RegisterNode)
	r.GET("/nodes/health", h.NodesHealth)

	// Serve web dashboard — available at /app/
	r.Static("/app", *webDir)
	r.GET("/", func(c *gin.Context) { c.Redirect(http.StatusFound, "/app/") })

	// 7. HTTP server
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Metadata.Port),
		Handler: r,
	}

	go func() {
		log.Info("Listening", zap.Int("port", cfg.Metadata.Port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("Server error", zap.Error(err))
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info("Shutting down Metadata Server…")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatal("Forced shutdown", zap.Error(err))
	}
	log.Info("Metadata Server stopped.")
}
