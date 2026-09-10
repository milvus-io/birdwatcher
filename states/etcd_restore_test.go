package states

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

// restoreTestKV is a minimal kv.MetaKV fake that records MultiSave calls and
// can be told to fail whenever a batch contains a given sentinel key. Tying
// the failure to batch content (rather than call count) keeps the test
// deterministic despite the concurrent workers racing to pull batches off
// the channel.
type restoreTestKV struct {
	kv.MetaKV

	failKey string

	mu             sync.Mutex
	saved          map[string]string
	multiSaveCalls int
}

func newRestoreTestKV(failKey string) *restoreTestKV {
	return &restoreTestKV{failKey: failKey, saved: make(map[string]string)}
}

func (k *restoreTestKV) MultiSave(ctx context.Context, keys, values []string) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	k.multiSaveCalls++
	for _, key := range keys {
		if k.failKey != "" && key == k.failKey {
			return fmt.Errorf("simulated MultiSave failure for key %s", key)
		}
	}
	for i, key := range keys {
		k.saved[key] = values[i]
	}
	return nil
}

// buildEtcdBackupV2Stream writes `entries` KeyDataPair records in the format
// restoreEtcdFromBackV2 expects: an 8-byte little-endian length prefix
// followed by the marshaled proto, terminated by a zero-length stopper.
func buildEtcdBackupV2Stream(t *testing.T, entries int) (*bytes.Buffer, *models.PartHeader) {
	t.Helper()

	buf := &bytes.Buffer{}
	w := bufio.NewWriter(buf)
	for i := 0; i < entries; i++ {
		entry := &commonpb.KeyDataPair{
			Key:  fmt.Sprintf("key-%d", i),
			Data: []byte(fmt.Sprintf("value-%d", i)),
		}
		bs, err := proto.Marshal(entry)
		require.NoError(t, err)
		writeBackupBytes(w, bs)
	}
	writeBackupBytes(w, nil) // stopper
	require.NoError(t, w.Flush())

	meta := map[string]string{
		"instance": "test-instance",
		"cnt":      fmt.Sprintf("%d", entries),
	}
	extra, err := json.Marshal(meta)
	require.NoError(t, err)

	return buf, &models.PartHeader{Extra: extra}
}

func TestRestoreEtcdFromBackV2Success(t *testing.T) {
	const entries = 25 // batchNum(10) * 2 full batches + 1 partial batch
	rd, ph := buildEtcdBackupV2Stream(t, entries)

	fake := newRestoreTestKV("")
	instance, err := restoreEtcdFromBackV2(fake, rd, ph)
	require.NoError(t, err)
	require.Equal(t, "test-instance", instance)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Len(t, fake.saved, entries)
	for i := 0; i < entries; i++ {
		require.Equal(t, fmt.Sprintf("value-%d", i), fake.saved[fmt.Sprintf("key-%d", i)])
	}
}

// TestRestoreEtcdFromBackV2PropagatesMultiSaveError reproduces issue #540:
// a MultiSave failure on one batch out of several must abort the restore and
// be surfaced as an error, never silently dropped by a last-write-wins race
// on a shared error variable. Run with -race to also confirm there is no
// data race on the error path.
func TestRestoreEtcdFromBackV2PropagatesMultiSaveError(t *testing.T) {
	const entries = 25 // 3 batches across 3 workers
	rd, ph := buildEtcdBackupV2Stream(t, entries)

	// key-10 is the first key of the second batch.
	fake := newRestoreTestKV("key-10")
	instance, err := restoreEtcdFromBackV2(fake, rd, ph)
	require.Error(t, err)
	require.Empty(t, instance)
}

func TestRestoreEtcdFromBackV2PropagatesMultiSaveErrorRepeated(t *testing.T) {
	// Repeat the race-prone path several times so a flaky last-write-wins
	// outcome (the failing batch happens to not be the last one to finish)
	// cannot pass by chance.
	for i := 0; i < 20; i++ {
		const entries = 25
		rd, ph := buildEtcdBackupV2Stream(t, entries)
		fake := newRestoreTestKV("key-10")
		instance, err := restoreEtcdFromBackV2(fake, rd, ph)
		require.Error(t, err)
		require.Empty(t, instance)
	}
}
