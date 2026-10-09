package service

import (
	"bytes"
	"context"
	"testing"

	"dfs-system/internal/metadata/model"
	"dfs-system/pkg/chunking"
	"dfs-system/pkg/errors"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (c *cluster) repair(t *testing.T, id uint) *RepairResult {
	t.Helper()
	res, err := c.svc.RepairFile(context.Background(), id)
	require.NoError(t, err)
	return res
}

func (c *cluster) chunkOnDisk(t *testing.T, nodeID, hash string) []byte {
	t.Helper()
	data, err := afero.ReadFile(c.nodes[nodeID].fs, "/data/chunk_"+hash)
	require.NoError(t, err)
	return data
}

func TestRepairRewritesCorruptReplicaOfSameSize(t *testing.T) {
	c := newCluster(t, 3, 2)
	data := randomData(t, chunking.DefaultChunkSize+100)
	file := c.upload(t, "a.bin", data)

	ch := file.Chunks[0]
	bad := ch.Replicas[0].NodeID
	good := c.chunkOnDisk(t, bad, ch.Hash)
	tampered := append([]byte(nil), good...)
	tampered[0] ^= 0xff // same size, different content
	require.NoError(t, afero.WriteFile(c.nodes[bad].fs, "/data/chunk_"+ch.Hash, tampered, 0644))
	require.Equal(t, model.CheckDegraded, c.verify(t, file.ID).Status)

	res := c.repair(t, file.ID)
	assert.Equal(t, 1, res.ReplicasRewritten)
	assert.Equal(t, 0, res.ReplicasAdded)
	assert.Equal(t, model.CheckVerified, res.Check.Status, res.Check.Message)
	assert.Equal(t, good, c.chunkOnDisk(t, bad, ch.Hash), "corrupt copy replaced on disk")
	assert.Contains(t, res.Summary, "1 replica(s) rewritten")

	report, err := c.svc.GetIntegrityReport(file.ID)
	require.NoError(t, err)
	assert.Equal(t, model.CheckVerified, report.File.LastCheck.Status, "post-repair check is stored")
	assert.Equal(t, data, c.download(t, file.ID))
}

func TestRepairRestoresMissingReplica(t *testing.T) {
	c := newCluster(t, 2, 2)
	file := c.upload(t, "a.bin", randomData(t, 1000))
	ch := file.Chunks[0]
	require.NoError(t, c.nodes[ch.Replicas[1].NodeID].fs.Remove("/data/chunk_"+ch.Hash))

	res := c.repair(t, file.ID)
	assert.Equal(t, 1, res.ReplicasRewritten)
	assert.Equal(t, model.CheckVerified, res.Check.Status)
}

func TestRepairAddsReplicaWhenHolderOffline(t *testing.T) {
	c := newCluster(t, 3, 2)
	file := c.upload(t, "a.bin", randomData(t, 1000))
	ch := file.Chunks[0]
	offline := ch.Replicas[0].NodeID
	c.disc.setDown(offline, true)

	res := c.repair(t, file.ID)
	assert.Equal(t, 0, res.ReplicasRewritten)
	assert.Equal(t, 1, res.ReplicasAdded)
	// Redundancy is restored, but the offline replica still can't be checked, so the
	// result stays DEGRADED until that node returns.
	assert.Equal(t, model.CheckDegraded, res.Check.Status)
	assert.Equal(t, 2, res.Check.ReplicasOK)

	c.disc.setDown(offline, false)
	assert.Equal(t, model.CheckVerified, c.verify(t, file.ID).Status)
}

func TestRepairFixesUnderReplicatedChunk(t *testing.T) {
	c := newCluster(t, 2, 2)
	c.disc.setDown("node-2", true)
	file := c.upload(t, "a.bin", randomData(t, 1000))
	c.disc.setDown("node-2", false)

	res := c.repair(t, file.ID)
	assert.Equal(t, 1, res.ReplicasAdded)
	assert.Equal(t, model.CheckVerified, res.Check.Status)
}

func TestRepairCannotRecoverChunkWithoutIntactCopy(t *testing.T) {
	c := newCluster(t, 2, 2)
	file := c.upload(t, "a.bin", randomData(t, 1000))
	ch := file.Chunks[0]
	for _, r := range ch.Replicas {
		require.NoError(t, afero.WriteFile(c.nodes[r.NodeID].fs, "/data/chunk_"+ch.Hash, []byte("tampered"), 0644))
	}

	res := c.repair(t, file.ID)
	assert.Equal(t, 1, res.ChunksUnrecoverable)
	assert.Equal(t, 0, res.ReplicasRewritten)
	assert.Equal(t, model.CheckCorrupt, res.Check.Status)
	assert.Equal(t, []byte("tampered"), c.chunkOnDisk(t, ch.Replicas[0].NodeID, ch.Hash), "bad data is never copied")
}

func TestRepairHealthyFileIsNoOp(t *testing.T) {
	c := newCluster(t, 3, 2)
	file := c.upload(t, "a.bin", randomData(t, 1000))
	res := c.repair(t, file.ID)
	assert.Equal(t, 0, res.ReplicasRewritten+res.ReplicasAdded)
	assert.Equal(t, model.CheckVerified, res.Check.Status)
	assert.Contains(t, res.Summary, "nothing needed repair")

	ops, err := c.svc.ListOperations(model.OpRepair, 10)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	assert.Equal(t, model.CheckVerified, ops[0].Result)
}

func TestRepairRejectsUnknownAndFailedFiles(t *testing.T) {
	c := newCluster(t, 2, 2, "node-1")
	_, err := c.svc.RepairFile(context.Background(), 42)
	assert.ErrorIs(t, err, errors.ErrFileNotFound)

	_, err = c.svc.UploadFile(context.Background(), "a", bytes.NewReader([]byte("x")))
	require.Error(t, err)
	files, err := c.svc.ListFiles()
	require.NoError(t, err)
	_, err = c.svc.RepairFile(context.Background(), files[0].ID)
	assert.ErrorIs(t, err, errors.ErrFileNotReady)
}
