package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"dfs-system/internal/common"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type Coordinator struct {
	cli *clientv3.Client
}

// NewCoordinator creates a new etcd client wrapper.
func NewCoordinator(endpoints []string) (*Coordinator, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &Coordinator{cli: cli}, nil
}

// Close closes the underlying etcd client.
func (c *Coordinator) Close() error {
	return c.cli.Close()
}

// RegisterNode registers a storage node's details in etcd under a lease (TTL).
// It returns the lease ID, a keep-alive response channel, and any error encountered.
func (c *Coordinator) RegisterNode(ctx context.Context, info common.StorageNodeInfo, ttl int64) (clientv3.LeaseID, <-chan *clientv3.LeaseKeepAliveResponse, error) {
	resp, err := c.cli.Grant(ctx, ttl)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to grant lease: %w", err)
	}
	leaseID := resp.ID

	key := fmt.Sprintf("/nodes/%s", info.NodeID)
	val, err := json.Marshal(info)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to marshal node info: %w", err)
	}

	_, err = c.cli.Put(ctx, key, string(val), clientv3.WithLease(leaseID))
	if err != nil {
		return 0, nil, fmt.Errorf("failed to put node register key: %w", err)
	}

	kaChan, err := c.cli.KeepAlive(ctx, leaseID)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to setup keep-alive channel: %w", err)
	}

	return leaseID, kaChan, nil
}

// DiscoverNodes fetches all active storage nodes from the etcd key-value store.
func (c *Coordinator) DiscoverNodes(ctx context.Context) ([]common.StorageNodeInfo, error) {
	resp, err := c.cli.Get(ctx, "/nodes/", clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve active nodes: %w", err)
	}

	var nodes []common.StorageNodeInfo
	for _, kv := range resp.Kvs {
		var info common.StorageNodeInfo
		if err := json.Unmarshal(kv.Value, &info); err == nil {
			nodes = append(nodes, info)
		}
	}
	return nodes, nil
}
