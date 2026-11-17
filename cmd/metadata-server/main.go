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
	"dfs-system/internal/common/middleware"
	"dfs-system/internal/coordinator"
	"dfs-system/internal/metadata/handler"
	"dfs-system/internal/metadata/model"
	"dfs-system/internal/metadata/repository"
	"dfs-system/internal/metadata/service"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// main configures the metadata API, database, node discovery, and background repair.
func main() {
	configPath := flag.String("config", "configs/config.yaml", "Path to config file")
	port := flag.Int("port", 0, "Overriding port")
	dbPath := flag.String("db", "", "Overriding database path")
	etcdEndpoints := flag.String("etcd", "", "Comma-separated etcd endpoints")
	repFactor := flag.Int("rep", 0, "Overriding replication factor")
	repairInterval := flag.Int("repair-interval", 0, "Seconds between re-replication passes (negative disables)")
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

	if *port != 0 {
		cfg.Metadata.Port = *port
	}
	if *dbPath != "" {
		cfg.Metadata.DBPath = *dbPath
	}
	if *etcdEndpoints != "" {
		cfg.Metadata.EtcdEndpoints = config.SplitList(*etcdEndpoints)
	}
	if *repFactor != 0 {
		cfg.Metadata.ReplicationFactor = *repFactor
	}
	if *repairInterval != 0 {
		cfg.Metadata.RepairInterval = *repairInterval
	}

	// Defaults
	if cfg.Metadata.Port == 0 {
		cfg.Metadata.Port = 8000
	}
	if cfg.Metadata.DBPath == "" {
		cfg.Metadata.DBPath = "./metadata.db"
	}
	if len(cfg.Metadata.EtcdEndpoints) == 0 {
		cfg.Metadata.EtcdEndpoints = []string{"localhost:2379"}
	}
	if cfg.Metadata.ReplicationFactor == 0 {
		cfg.Metadata.ReplicationFactor = 2
	}
	if cfg.Metadata.RepairInterval == 0 {
		cfg.Metadata.RepairInterval = 30
	}

	log.Info("Starting Metadata Server",
		zap.Int("port", cfg.Metadata.Port),
		zap.String("db", cfg.Metadata.DBPath),
		zap.Strings("etcd", cfg.Metadata.EtcdEndpoints),
		zap.Int("rep_factor", cfg.Metadata.ReplicationFactor),
		zap.Int("repair_interval", cfg.Metadata.RepairInterval),
	)

	// 3. Database + auto-migrate
	gormDB, err := gorm.Open(sqlite.Open(cfg.Metadata.DBPath), &gorm.Config{})
	if err != nil {
		log.Fatal("Failed to open database", zap.Error(err))
	}
	// SQLite allows a single writer; one pooled connection avoids "database is locked"
	// errors under concurrent uploads, and WAL keeps reads from blocking on writes.
	sqlDB, err := gormDB.DB()
	if err != nil {
		log.Fatal("Failed to access database handle", zap.Error(err))
	}
	sqlDB.SetMaxOpenConns(1)
	if err := gormDB.Exec("PRAGMA journal_mode=WAL").Error; err != nil {
		log.Warn("Could not enable WAL mode", zap.Error(err))
	}
	if err := gormDB.AutoMigrate(model.All()...); err != nil {
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if cfg.Metadata.RepairInterval > 0 {
		go svc.RunRepairLoop(ctx, time.Duration(cfg.Metadata.RepairInterval)*time.Second)
	}

	// 6. Gin router
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(middleware.CORS())
	r.Use(middleware.RequestLogger(log))

	// API routes
	r.POST("/upload", h.Upload)
	r.GET("/download/:fileId", h.Download)
	r.GET("/files", h.ListFiles)
	r.GET("/files/:fileId", h.GetFile)
	r.DELETE("/files/:fileId", h.DeleteFile)
	r.POST("/nodes/register", h.RegisterNode)
	r.GET("/nodes/health", h.NodesHealth)
	r.POST("/files/:fileId/verify", h.VerifyFile)
	r.POST("/files/:fileId/repair", h.RepairFile)
	r.GET("/files/:fileId/integrity", h.FileIntegrity)
	r.GET("/integrity", h.ListIntegrity)
	r.GET("/stats", h.Stats)
	r.GET("/operations", h.ListOperations)

	// Serve web dashboard — available at /app/
	r.Static("/app", *webDir)
	r.GET("/", func(c *gin.Context) { c.Redirect(http.StatusFound, "/app/") })

	// 7. HTTP server
	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Metadata.Port),
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
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
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("Forced shutdown", zap.Error(err))
	}
	log.Info("Metadata Server stopped.")
}
