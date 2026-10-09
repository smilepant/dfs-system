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

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type Worker struct {
	nodeID      string
	address     string
	metadataURL string
	interval    int
	store       *service.StorageService
	etcdCoord   *coordinator.Coordinator
	httpClient  *http.Client
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
		httpClient:  &http.Client{Timeout: 5 * time.Second},
		logger:      logger,
	}
}

// Start spawns the background worker.
func (w *Worker) Start(ctx context.Context) {
	go w.runRegistrationAndHeartbeatLoop(ctx)
}

func (w *Worker) nodeInfo() common.StorageNodeInfo {
	total, free, err := w.store.GetStats()
	if err != nil {
		w.logger.Error("Worker failed to get storage stats", zap.Error(err))
	}
	return common.StorageNodeInfo{
		NodeID:     w.nodeID,
		Address:    w.address,
		TotalSpace: total,
		FreeSpace:  free,
	}
}

func (w *Worker) runRegistrationAndHeartbeatLoop(ctx context.Context) {
	period := time.Duration(w.interval) * time.Second

	for {
		w.logger.Info("Attempting etcd node registration", zap.String("node_id", w.nodeID), zap.String("address", w.address))
		leaseID, kaChan, err := w.etcdCoord.RegisterNode(ctx, w.nodeInfo(), int64(w.interval*3))
		if err != nil {
			w.logger.Error("Failed to register with etcd, retrying", zap.Duration("in", period), zap.Error(err))
			select {
			case <-ctx.Done():
				return
			case <-time.After(period):
				continue
			}
		}
		w.logger.Info("Registered successfully with etcd", zap.String("node_id", w.nodeID), zap.Int64("lease_id", int64(leaseID)))

		w.heartbeat(ctx, leaseID, kaChan)
		if ctx.Err() != nil {
			return
		}

		w.logger.Warn("Heartbeat lost. Restarting registration in 2s...")
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

// heartbeat runs while the etcd lease is alive: it drains keep-alive responses,
// republishes current free space every interval so placement decisions stay
// accurate, and retries metadata-server registration until it succeeds.
// It returns when the lease is lost or ctx is cancelled.
func (w *Worker) heartbeat(ctx context.Context, leaseID clientv3.LeaseID, kaChan <-chan *clientv3.LeaseKeepAliveResponse) {
	ticker := time.NewTicker(time.Duration(w.interval) * time.Second)
	defer ticker.Stop()

	registeredWithMeta := w.tryRegisterWithMetadataServer()

	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-kaChan:
			if !ok || resp == nil {
				w.logger.Warn("etcd keep-alive channel closed")
				return
			}
		case <-ticker.C:
			info := w.nodeInfo()
			if err := w.etcdCoord.UpdateNode(ctx, info, leaseID); err != nil {
				w.logger.Warn("Failed to refresh node stats in etcd", zap.Error(err))
			}
			if !registeredWithMeta {
				registeredWithMeta = w.tryRegisterWithMetadataServer()
			}
		}
	}
}

func (w *Worker) tryRegisterWithMetadataServer() bool {
	if err := w.registerWithMetadataServer(w.nodeInfo()); err != nil {
		w.logger.Warn("Failed to register with Metadata Server, will retry", zap.String("url", w.metadataURL), zap.Error(err))
		return false
	}
	w.logger.Info("Registered successfully with Metadata Server", zap.String("url", w.metadataURL))
	return true
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

	resp, err := w.httpClient.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("metadata server returned status %d", resp.StatusCode)
	}

	return nil
}
