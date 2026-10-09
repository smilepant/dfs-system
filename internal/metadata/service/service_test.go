package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"dfs-system/internal/common"
	"dfs-system/internal/metadata/model"
	"dfs-system/internal/metadata/repository"
	storagehandler "dfs-system/internal/storage/handler"
	storageservice "dfs-system/internal/storage/service"
	"dfs-system/pkg/chunking"
	"dfs-system/pkg/errors"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// fakeDiscoverer stands in for etcd; tests toggle nodes on and off.
type fakeDiscoverer struct {
	mu    sync.Mutex
	nodes map[string]common.StorageNodeInfo
	down  map[string]bool
}

func (f *fakeDiscoverer) DiscoverNodes(context.Context) ([]common.StorageNodeInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []common.StorageNodeInfo
	for id, n := range f.nodes {
		if !f.down[id] {
			out = append(out, n)
		}
	}
	return out, nil
}

func (f *fakeDiscoverer) setDown(id string, down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down[id] = down
}

type testNode struct {
	id     string
	store  *storageservice.StorageService
	fs     afero.Fs
	failUp bool // reject chunk uploads with 500
}

type cluster struct {
	svc   *MetadataService
	repo  *repository.Repository
	disc  *fakeDiscoverer
	nodes map[string]*testNode
}

func newCluster(t *testing.T, nodeCount, repFactor int, failing ...string) *cluster {
	t.Helper()
	gin.SetMode(gin.TestMode)

	db, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, _ := db.DB()
	sqlDB.SetMaxOpenConns(1)
	require.NoError(t, db.AutoMigrate(&model.File{}, &model.Chunk{}, &model.ChunkReplica{}, &model.StorageNode{}))
	repo := repository.NewRepository(db)

	c := &cluster{
		repo:  repo,
		disc:  &fakeDiscoverer{nodes: map[string]common.StorageNodeInfo{}, down: map[string]bool{}},
		nodes: map[string]*testNode{},
	}

	for i := 1; i <= nodeCount; i++ {
		id := fmt.Sprintf("node-%d", i)
		fs := afero.NewMemMapFs()
		store, err := storageservice.NewStorageService(fs, "/data", 0)
		require.NoError(t, err)
		tn := &testNode{id: id, store: store, fs: fs}
		for _, f := range failing {
			tn.failUp = tn.failUp || f == id
		}
		c.nodes[id] = tn

		h := storagehandler.NewStorageHandler(store, id, zap.NewNop())
		r := gin.New()
		r.POST("/chunk/upload", func(ctx *gin.Context) {
			if tn.failUp {
				ctx.AbortWithStatus(http.StatusInternalServerError)
				return
			}
			h.UploadChunk(ctx)
		})
		r.GET("/chunk/:id", h.DownloadChunk)
		r.DELETE("/chunk/:id", h.DeleteChunk)
		srv := httptest.NewServer(r)
		t.Cleanup(srv.Close)

		total, free, _ := store.GetStats()
		c.disc.nodes[id] = common.StorageNodeInfo{
			NodeID:     id,
			Address:    strings.TrimPrefix(srv.URL, "http://"),
			TotalSpace: total,
			FreeSpace:  free,
		}
	}

	c.svc = NewMetadataService(repo, c.disc, repFactor, zap.NewNop())
	return c
}

func randomData(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

func (c *cluster) download(t *testing.T, id uint) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, c.svc.DownloadFile(context.Background(), id, &buf))
	return buf.Bytes()
}

func (c *cluster) chunkCount(t *testing.T, nodeID string) int {
	t.Helper()
	entries, err := afero.ReadDir(c.nodes[nodeID].fs, "/data")
	require.NoError(t, err)
	return len(entries)
}

func TestUploadDownloadRoundTripSpreadsChunks(t *testing.T) {
	c := newCluster(t, 3, 2)
	data := randomData(t, 2*chunking.DefaultChunkSize+123) // 3 chunks

	file, err := c.svc.UploadFile(context.Background(), "big.bin", bytes.NewReader(data))
	require.NoError(t, err)
	assert.Equal(t, model.FileStatusActive, file.Status)
	assert.Equal(t, int64(len(data)), file.SizeBytes)
	require.Len(t, file.Chunks, 3)

	for _, ch := range file.Chunks {
		require.Len(t, ch.Replicas, 2)
		assert.NotEqual(t, ch.Replicas[0].NodeID, ch.Replicas[1].NodeID)
	}
	// 6 replicas over 3 equally-empty nodes should land 2 per node.
	for id := range c.nodes {
		assert.Equal(t, 2, c.chunkCount(t, id), "node %s", id)
	}

	assert.Equal(t, data, c.download(t, file.ID))
}

func TestUploadFallsBackWhenNodeRejectsChunk(t *testing.T) {
	c := newCluster(t, 3, 2, "node-1")
	data := randomData(t, 1000)

	file, err := c.svc.UploadFile(context.Background(), "a.txt", bytes.NewReader(data))
	require.NoError(t, err)
	require.Len(t, file.Chunks[0].Replicas, 2)
	for _, r := range file.Chunks[0].Replicas {
		assert.NotEqual(t, "node-1", r.NodeID)
	}
}

func TestUploadFailureMarksFileFailed(t *testing.T) {
	c := newCluster(t, 2, 2, "node-1")

	_, err := c.svc.UploadFile(context.Background(), "a.txt", bytes.NewReader([]byte("hello")))
	assert.ErrorIs(t, err, errors.ErrRepFactorUnmet)

	files, err := c.svc.ListFiles()
	require.NoError(t, err)
	require.Len(t, files, 1)
	assert.Equal(t, model.FileStatusFailed, files[0].Status)

	err = c.svc.DownloadFile(context.Background(), files[0].ID, &bytes.Buffer{})
	assert.ErrorIs(t, err, errors.ErrFileNotReady)

	// Deleting the failed file reclaims the replica that did get written.
	require.NoError(t, c.svc.DeleteFile(context.Background(), files[0].ID))
	assert.Equal(t, 0, c.chunkCount(t, "node-2"))
}

func TestDownloadSurvivesNodeLossAndCorruption(t *testing.T) {
	c := newCluster(t, 3, 2)
	data := randomData(t, chunking.DefaultChunkSize+10)

	file, err := c.svc.UploadFile(context.Background(), "a.bin", bytes.NewReader(data))
	require.NoError(t, err)

	// Corrupt the first replica of chunk 0 in place; the second replica must be used.
	ch := file.Chunks[0]
	corrupted := c.nodes[ch.Replicas[0].NodeID]
	require.NoError(t, afero.WriteFile(corrupted.fs, "/data/chunk_"+ch.Hash, []byte("garbage"), 0644))
	assert.Equal(t, data, c.download(t, file.ID))

	// Take down the first replica holder of chunk 1; the other holder must serve it.
	c.disc.setDown(file.Chunks[1].Replicas[0].NodeID, true)
	assert.Equal(t, data, c.download(t, file.ID))
}

func TestRepairRestoresReplicationFactor(t *testing.T) {
	c := newCluster(t, 3, 2)
	data := randomData(t, 2*chunking.DefaultChunkSize)

	file, err := c.svc.UploadFile(context.Background(), "a.bin", bytes.NewReader(data))
	require.NoError(t, err)

	c.disc.setDown("node-1", true)
	repaired, err := c.svc.RepairOnce(context.Background())
	require.NoError(t, err)
	assert.Greater(t, repaired, 0)

	// Every chunk now has 2 replicas on live nodes.
	file, err = c.repo.GetFile(file.ID)
	require.NoError(t, err)
	for _, ch := range file.Chunks {
		live := 0
		for _, r := range ch.Replicas {
			if r.NodeID != "node-1" {
				live++
			}
		}
		assert.Equal(t, 2, live, "chunk %d", ch.Sequence)
	}

	// A second pass has nothing to do, and the data survives losing another node.
	repaired, err = c.svc.RepairOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, repaired)
	c.disc.setDown("node-2", true)
	assert.Equal(t, data, c.download(t, file.ID))
}

func TestDeleteKeepsChunksSharedWithOtherFiles(t *testing.T) {
	c := newCluster(t, 2, 2)
	data := randomData(t, 5000)

	first, err := c.svc.UploadFile(context.Background(), "one.txt", bytes.NewReader(data))
	require.NoError(t, err)
	second, err := c.svc.UploadFile(context.Background(), "two.txt", bytes.NewReader(data))
	require.NoError(t, err)

	require.NoError(t, c.svc.DeleteFile(context.Background(), first.ID))
	_, err = c.svc.GetFileMetadata(first.ID)
	assert.ErrorIs(t, err, errors.ErrFileNotFound)
	assert.Equal(t, data, c.download(t, second.ID))

	require.NoError(t, c.svc.DeleteFile(context.Background(), second.ID))
	for id := range c.nodes {
		assert.Equal(t, 0, c.chunkCount(t, id), "node %s", id)
	}

	assert.ErrorIs(t, c.svc.DeleteFile(context.Background(), second.ID), errors.ErrFileNotFound)
}

func TestUploadEmptyFile(t *testing.T) {
	c := newCluster(t, 2, 2)
	file, err := c.svc.UploadFile(context.Background(), "empty", bytes.NewReader(nil))
	require.NoError(t, err)
	assert.Empty(t, file.Chunks)
	assert.Empty(t, c.download(t, file.ID))
}

func TestUploadNoNodes(t *testing.T) {
	c := newCluster(t, 1, 2)
	c.disc.setDown("node-1", true)
	_, err := c.svc.UploadFile(context.Background(), "a", bytes.NewReader([]byte("x")))
	assert.ErrorIs(t, err, errors.ErrNoActiveNodes)
}
