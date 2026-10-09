package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"dfs-system/internal/metadata/model"
	"dfs-system/pkg/chunking"
	"dfs-system/pkg/errors"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (c *cluster) upload(t *testing.T, name string, data []byte) *model.File {
	t.Helper()
	file, err := c.svc.UploadFile(context.Background(), name, bytes.NewReader(data))
	require.NoError(t, err)
	return file
}

func (c *cluster) verify(t *testing.T, id uint) *model.IntegrityCheck {
	t.Helper()
	check, err := c.svc.VerifyFile(context.Background(), id)
	require.NoError(t, err)
	return check
}

func (c *cluster) setChecksum(id uint, sum string) error {
	return c.db.Model(&model.File{}).Where("id = ?", id).Update("checksum", sum).Error
}

func replicaStatuses(check *model.IntegrityCheck) map[string]int {
	out := map[string]int{}
	for _, r := range check.Replicas {
		out[r.Status]++
	}
	return out
}

func TestUploadRecordsFileChecksum(t *testing.T) {
	c := newCluster(t, 3, 2)
	data := randomData(t, chunking.DefaultChunkSize+500)
	file := c.upload(t, "a.bin", data)
	assert.Equal(t, sha256Hex(data), file.Checksum)

	stored, err := c.repo.GetFile(file.ID)
	require.NoError(t, err)
	assert.Equal(t, sha256Hex(data), stored.Checksum)
}

func TestVerifyHealthyFile(t *testing.T) {
	c := newCluster(t, 3, 2)
	data := randomData(t, 2*chunking.DefaultChunkSize+10) // 3 chunks x 2 replicas
	file := c.upload(t, "a.bin", data)

	check := c.verify(t, file.ID)
	assert.Equal(t, model.CheckVerified, check.Status)
	assert.Equal(t, 3, check.ChunksTotal)
	assert.Equal(t, 6, check.ReplicasTotal)
	assert.Equal(t, 6, check.ReplicasOK)
	assert.Equal(t, sha256Hex(data), check.ComputedChecksum)
	assert.Equal(t, sha256Hex(data), check.ExpectedChecksum)
	assert.Equal(t, map[string]int{model.ReplicaOK: 6}, replicaStatuses(check))
}

func TestVerifyDetectsSingleCorruptReplica(t *testing.T) {
	c := newCluster(t, 3, 2)
	data := randomData(t, 1000)
	file := c.upload(t, "a.bin", data)

	ch := file.Chunks[0]
	bad := ch.Replicas[0].NodeID
	require.NoError(t, afero.WriteFile(c.nodes[bad].fs, "/data/chunk_"+ch.Hash, []byte("tampered"), 0644))

	check := c.verify(t, file.ID)
	assert.Equal(t, model.CheckDegraded, check.Status, check.Message)
	assert.Equal(t, sha256Hex(data), check.ComputedChecksum)
	for _, r := range check.Replicas {
		if r.NodeID == bad {
			assert.Equal(t, model.ReplicaCorrupt, r.Status)
		} else {
			assert.Equal(t, model.ReplicaOK, r.Status)
		}
	}
}

func TestVerifyAllReplicasCorrupt(t *testing.T) {
	c := newCluster(t, 2, 2)
	file := c.upload(t, "a.bin", randomData(t, 1000))

	ch := file.Chunks[0]
	for _, r := range ch.Replicas {
		require.NoError(t, afero.WriteFile(c.nodes[r.NodeID].fs, "/data/chunk_"+ch.Hash, []byte("tampered"), 0644))
	}

	check := c.verify(t, file.ID)
	assert.Equal(t, model.CheckCorrupt, check.Status)
	assert.Empty(t, check.ComputedChecksum)
	assert.Equal(t, 0, check.ReplicasOK)
}

func TestVerifyMissingAndOfflineReplicas(t *testing.T) {
	c := newCluster(t, 3, 2)
	data := randomData(t, 1000)
	file := c.upload(t, "a.bin", data)
	ch := file.Chunks[0]

	// One holder lost the chunk on disk: the file is still readable, but degraded.
	missing := ch.Replicas[0].NodeID
	require.NoError(t, c.nodes[missing].fs.Remove("/data/chunk_"+ch.Hash))
	check := c.verify(t, file.ID)
	assert.Equal(t, model.CheckDegraded, check.Status)
	assert.Equal(t, 1, replicaStatuses(check)[model.ReplicaMissing])

	// The other holder goes offline too: no intact replica is reachable.
	offline := ch.Replicas[1].NodeID
	c.disc.setDown(offline, true)
	check = c.verify(t, file.ID)
	assert.Equal(t, model.CheckUnavailable, check.Status)
	assert.Equal(t, map[string]int{model.ReplicaMissing: 1, model.ReplicaOffline: 1}, replicaStatuses(check))
}

func TestVerifyReportsUnreachableNode(t *testing.T) {
	c := newCluster(t, 2, 2)
	file := c.upload(t, "a.bin", randomData(t, 1000))

	// etcd still lists the node, but nothing answers at its address.
	info := c.disc.nodes["node-1"]
	info.Address = "127.0.0.1:1"
	c.disc.nodes["node-1"] = info

	check := c.verify(t, file.ID)
	assert.Equal(t, model.CheckDegraded, check.Status)
	statuses := replicaStatuses(check)
	assert.Equal(t, 1, statuses[model.ReplicaError])
	assert.Equal(t, 1, statuses[model.ReplicaOK])
	for _, r := range check.Replicas {
		if r.Status == model.ReplicaError {
			assert.Equal(t, "Node could not be reached.", r.Detail)
		}
	}
}

func TestVerifyDetectsFileChecksumMismatch(t *testing.T) {
	c := newCluster(t, 2, 2)
	file := c.upload(t, "a.bin", randomData(t, 1000))
	require.NoError(t, c.setChecksum(file.ID, sha256Hex([]byte("something else"))))

	check := c.verify(t, file.ID)
	assert.Equal(t, model.CheckCorrupt, check.Status)
	assert.Equal(t, 2, check.ReplicasOK, "chunks themselves are intact")
}

func TestVerifyBackfillsMissingChecksum(t *testing.T) {
	c := newCluster(t, 2, 2)
	data := randomData(t, 1000)
	file := c.upload(t, "legacy.bin", data)
	require.NoError(t, c.setChecksum(file.ID, ""))

	check := c.verify(t, file.ID)
	assert.Equal(t, model.CheckVerified, check.Status)
	assert.Contains(t, check.Message, "computed from hash-verified chunks")

	stored, err := c.repo.GetFile(file.ID)
	require.NoError(t, err)
	assert.Equal(t, sha256Hex(data), stored.Checksum)
}

func TestVerifyFlagsUnderReplication(t *testing.T) {
	c := newCluster(t, 2, 2)
	c.disc.setDown("node-2", true)
	file := c.upload(t, "a.bin", randomData(t, 1000)) // only 1 replica possible
	c.disc.setDown("node-2", false)

	check := c.verify(t, file.ID)
	assert.Equal(t, model.CheckDegraded, check.Status)
	assert.Contains(t, check.Message, "fewer than 2 intact replica")
}

func TestVerifyEmptyFile(t *testing.T) {
	c := newCluster(t, 2, 2)
	file := c.upload(t, "empty", nil)
	check := c.verify(t, file.ID)
	assert.Equal(t, model.CheckVerified, check.Status)
	assert.Equal(t, sha256Hex(nil), check.ComputedChecksum)
}

func TestVerifyRejectsUnknownAndFailedFiles(t *testing.T) {
	c := newCluster(t, 2, 2, "node-1")
	_, err := c.svc.VerifyFile(context.Background(), 999)
	assert.ErrorIs(t, err, errors.ErrFileNotFound)

	_, err = c.svc.UploadFile(context.Background(), "a", bytes.NewReader([]byte("x")))
	require.Error(t, err)
	files, err := c.svc.ListFiles()
	require.NoError(t, err)
	_, err = c.svc.VerifyFile(context.Background(), files[0].ID)
	assert.ErrorIs(t, err, errors.ErrFileNotReady)
}

func TestIntegrityReportHistoryAndDelete(t *testing.T) {
	c := newCluster(t, 2, 2)
	file := c.upload(t, "a.bin", randomData(t, 1000))
	c.verify(t, file.ID)
	c.disc.setDown("node-1", true)
	c.verify(t, file.ID)

	report, err := c.svc.GetIntegrityReport(file.ID)
	require.NoError(t, err)
	require.Len(t, report.History, 2)
	assert.Equal(t, model.CheckDegraded, report.History[0].Status, "newest first")
	assert.Len(t, report.History[0].Replicas, 2, "newest check includes replica results")
	assert.Empty(t, report.History[1].Replicas)
	assert.Equal(t, model.CheckDegraded, report.File.LastCheck.Status)

	list, err := c.svc.ListIntegrity()
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, model.CheckDegraded, list[0].LastCheck.Status)

	require.NoError(t, c.svc.DeleteFile(context.Background(), file.ID))
	_, err = c.svc.GetIntegrityReport(file.ID)
	assert.ErrorIs(t, err, errors.ErrFileNotFound)
	latest, err := c.repo.LatestChecks()
	require.NoError(t, err)
	assert.Empty(t, latest, "checks are removed with the file")
}

func TestStatsAndOperations(t *testing.T) {
	c := newCluster(t, 3, 2)
	data := randomData(t, chunking.DefaultChunkSize+1) // 2 chunks
	file := c.upload(t, "a.bin", data)
	c.download(t, file.ID)
	c.verify(t, file.ID)

	stats, err := c.svc.Stats(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.FilesActive)
	assert.Equal(t, int64(len(data)), stats.LogicalBytes)
	assert.Equal(t, 2, stats.Chunks)
	assert.Equal(t, 4, stats.Replicas)
	assert.Equal(t, 3, stats.LiveNodes)
	assert.Equal(t, 0, stats.UnderReplicated)
	assert.Equal(t, map[string]int{model.CheckVerified: 1}, stats.IntegrityByStatus)

	// Take down a holder of chunk 0: that chunk is now under-replicated.
	c.disc.setDown(file.Chunks[0].Replicas[0].NodeID, true)
	stats, err = c.svc.Stats(context.Background())
	require.NoError(t, err)
	assert.GreaterOrEqual(t, stats.UnderReplicated, 1)
	assert.Equal(t, 0, stats.UnavailableChunks)

	ops, err := c.svc.ListOperations("", 10)
	require.NoError(t, err)
	var types []string
	for _, op := range ops {
		types = append(types, op.Type+":"+op.Result)
	}
	assert.Equal(t, []string{"VERIFY:VERIFIED", "DOWNLOAD:SUCCESS", "UPLOAD:SUCCESS"}, types)

	uploads, err := c.svc.ListOperations(model.OpUpload, 10)
	require.NoError(t, err)
	assert.Len(t, uploads, 1)
}
