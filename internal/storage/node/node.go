package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"dfs-system/internal/common"
	"dfs-system/internal/coordinator"
	"dfs-system/internal/storage/service"

	"go.uber.org/zap"
)

type Worker struct {
	nodeID      string
	address     string
	metadataURL string
	interval    int
	store       *service.StorageService
	etcdCoord   *coordinator.Coordinator
	logger      *zap.Logger
}

// NewWorker initializes a storage node worker.
func NewWorker(nodeID string, address string, metadataURL string, interval int, store *service.StorageService, etcdCoord *coordinator.Coordinator, logger *zap.Logger) *Worker {
	return &Worker{
		nodeID:      nodeID,
		address:     address,
		metadataURL: metadataURL,
		interval:    interval,
		store:       store,
		etcdCoord:   etcdCoord,
		logger:      logger,
	}
}

// Start spawns the background worker.
func (w *Worker) Start(ctx context.Context) {
	go w.runRegistrationAndHeartbeatLoop(ctx)
}

func (w *Worker) runRegistrationAndHeartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(w.interval) * time.Second)
	defer ticker.Stop()

	registeredWithMeta := false

	for {
		total, free, err := w.store.GetStats()
		if err != nil {
			w.logger.Error("Worker failed to get storage stats", zap.Error(err))
			total = 0
			free = 0
		}

		nodeInfo := common.StorageNodeInfo{
			NodeID:     w.nodeID,
			Address:    w.address,
			TotalSpace: total,
			FreeSpace:  free,
		}

		w.logger.Info("Attempting etcd node registration", zap.String("node_id", w.nodeID), zap.String("address", w.address))
		leaseID, kaChan, err := w.etcdCoord.RegisterNode(ctx, nodeInfo, int64(w.interval*3))
		if err != nil {
			w.logger.Error("Failed to register with etcd. Retrying in interval...", zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				continue
			}
		}

		w.logger.Info("Registered successfully with etcd", zap.String("node_id", w.nodeID), zap.Int64("lease_id", int64(leaseID)))

		if !registeredWithMeta {
			w.logger.Info("Registering with Metadata Server", zap.String("url", w.metadataURL))
			err = w.registerWithMetadataServer(nodeInfo)
			if err != nil {
				w.logger.Error("Failed to register with Metadata Server. Will retry...", zap.Error(err))
			} else {
				w.logger.Info("Registered successfully with Metadata Server")
				registeredWithMeta = true
			}
		}

		kaLoopCtx, cancel := context.WithCancel(ctx)

		go func() {
			defer cancel()
			for {
				select {
				case <-kaLoopCtx.Done():
					return
				case resp, ok := <-kaChan:
					if !ok || resp == nil {
						w.logger.Warn("etcd keep-alive channel closed. Re-registering...")
						return
					}
				}
			}
		}()

		select {
		case <-ctx.Done():
			cancel()
			return
		case <-kaLoopCtx.Done():
			// Lease expired or channel closed, re-loop
		}

		registeredWithMeta = false
		w.logger.Warn("Heartbeat lost. Restarting registration in 2s...")
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

func (w *Worker) registerWithMetadataServer(info common.StorageNodeInfo) error {
	url := fmt.Sprintf("%s/nodes/register", w.metadataURL)
	payload := common.NodeRegisterRequest{
		NodeID:     info.NodeID,
		Address:    info.Address,
		TotalSpace: info.TotalSpace,
		FreeSpace:  info.FreeSpace,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("metadata server returned status %d", resp.StatusCode)
	}

	return nil
}
