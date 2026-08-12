package integration

import (
	"context"
	"testing"
	"time"

	"dfs-system/internal/common"
	"dfs-system/internal/coordinator"
	"dfs-system/internal/metadata/model"
	"dfs-system/internal/metadata/repository"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

func TestEtcdCoordination(t *testing.T) {
	// Try to connect to local etcd (default during development)
	coord, err := coordinator.NewCoordinator([]string{"localhost:2379"})
	if err != nil {
		t.Skip("Skipping etcd integration test: etcd not running on localhost:2379")
		return
	}
	defer coord.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Verify we can ping etcd (DiscoverNodes should return empty or list)
	_, err = coord.DiscoverNodes(ctx)
	if err != nil {
		t.Skipf("Skipping etcd integration test: failed to query etcd: %v", err)
		return
	}

	// Register a mock node
	nodeInfo := common.StorageNodeInfo{
		NodeID:     "test-node",
		Address:    "localhost:9999",
		TotalSpace: 1000,
		FreeSpace:  800,
	}

	leaseID, _, err := coord.RegisterNode(ctx, nodeInfo, 5)
	assert.NoError(t, err)
	assert.NotZero(t, leaseID)

	// Discover nodes
	nodes, err := coord.DiscoverNodes(ctx)
	assert.NoError(t, err)
	assert.NotEmpty(t, nodes)

	found := false
	for _, n := range nodes {
		if n.NodeID == "test-node" {
			found = true
			assert.Equal(t, "localhost:9999", n.Address)
		}
	}
	assert.True(t, found)
}

func TestMetadataRepository(t *testing.T) {
	// Use in-memory SQLite DB for testing repository
	gormDB, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{})
	assert.NoError(t, err)

	err = gormDB.AutoMigrate(&model.File{}, &model.Chunk{}, &model.ChunkReplica{}, &model.StorageNode{})
	assert.NoError(t, err)

	repo := repository.NewRepository(gormDB)

	// Test node register
	node := &model.StorageNode{
		NodeID:     "node-123",
		Address:    "localhost:8081",
		TotalSpace: 100,
		FreeSpace:  90,
	}
	err = repo.RegisterNode(node)
	assert.NoError(t, err)

	retrievedNode, err := repo.GetNode("node-123")
	assert.NoError(t, err)
	assert.Equal(t, "localhost:8081", retrievedNode.Address)

	// Test file creation
	file := &model.File{
		Filename:  "testfile.txt",
		SizeBytes: 15,
		Status:    "PENDING",
	}
	err = repo.CreateFile(file)
	assert.NoError(t, err)
	assert.NotZero(t, file.ID)

	// Update status
	err = repo.UpdateFileStatus(file.ID, "ACTIVE")
	assert.NoError(t, err)

	retrievedFile, err := repo.GetFile(file.ID)
	assert.NoError(t, err)
	assert.Equal(t, "ACTIVE", retrievedFile.Status)
}
