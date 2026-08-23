package kv

import (
	"bufio"
	"context"
	"os"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// fakeMetaKV is a minimal in-memory MetaKV used to verify FileAuditKV
// delegates to the wrapped client instead of swallowing calls.
type fakeMetaKV struct {
	data map[string]string

	multiSaveErr   error
	multiSaveCalls [][2][]string
}

func newFakeMetaKV() *fakeMetaKV {
	return &fakeMetaKV{data: map[string]string{}}
}

func (f *fakeMetaKV) Load(ctx context.Context, key string, opts ...LoadOption) (string, error) {
	v, ok := f.data[key]
	if !ok {
		return "", errors.New("key not found")
	}
	return v, nil
}

func (f *fakeMetaKV) LoadWithPrefix(ctx context.Context, key string, opts ...LoadOption) ([]string, []string, error) {
	return nil, nil, nil
}

func (f *fakeMetaKV) Save(ctx context.Context, key, value string) error {
	f.data[key] = value
	return nil
}

func (f *fakeMetaKV) MultiSave(ctx context.Context, keys, values []string) error {
	f.multiSaveCalls = append(f.multiSaveCalls, [2][]string{keys, values})
	if f.multiSaveErr != nil {
		return f.multiSaveErr
	}
	for i, key := range keys {
		f.data[key] = values[i]
	}
	return nil
}

func (f *fakeMetaKV) Remove(ctx context.Context, key string) error {
	delete(f.data, key)
	return nil
}

func (f *fakeMetaKV) RemoveWithPrefix(ctx context.Context, key string) error {
	return nil
}

func (f *fakeMetaKV) removeWithPrevKV(ctx context.Context, key string) (*mvccpb.KeyValue, error) {
	return nil, nil
}

func (f *fakeMetaKV) removeWithPrefixAndPrevKV(ctx context.Context, prefix string) ([]*mvccpb.KeyValue, error) {
	return nil, nil
}

func (f *fakeMetaKV) GetAllRootPath(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (f *fakeMetaKV) BackupKV(base, prefix string, w *bufio.Writer, ignoreRevision bool, batchSize int64) error {
	return nil
}

func (f *fakeMetaKV) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	return nil
}

func (f *fakeMetaKV) Close() {}

func newTestFileAuditKV(t *testing.T, cli MetaKV) *FileAuditKV {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "audit-*.log")
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	return NewFileAuditKV(cli, f)
}

func TestFileAuditKVMultiSave(t *testing.T) {
	t.Run("delegates and persists values", func(t *testing.T) {
		fake := newFakeMetaKV()
		audit := newTestFileAuditKV(t, fake)

		keys := []string{"k1", "k2"}
		values := []string{"v1", "v2"}
		err := audit.MultiSave(context.TODO(), keys, values)
		require.NoError(t, err)

		require.Len(t, fake.multiSaveCalls, 1)
		assert.Equal(t, keys, fake.multiSaveCalls[0][0])
		assert.Equal(t, values, fake.multiSaveCalls[0][1])
		assert.Equal(t, "v1", fake.data["k1"])
		assert.Equal(t, "v2", fake.data["k2"])
	})

	t.Run("propagates underlying error", func(t *testing.T) {
		fake := newFakeMetaKV()
		fake.multiSaveErr = errors.New("injected failure")
		audit := newTestFileAuditKV(t, fake)

		err := audit.MultiSave(context.TODO(), []string{"k1"}, []string{"v1"})
		assert.ErrorIs(t, err, fake.multiSaveErr)
		assert.Empty(t, fake.data)
	})

	t.Run("rejects mismatched keys and values", func(t *testing.T) {
		fake := newFakeMetaKV()
		audit := newTestFileAuditKV(t, fake)

		err := audit.MultiSave(context.TODO(), []string{"k1", "k2"}, []string{"v1"})
		assert.Error(t, err)
		assert.Empty(t, fake.multiSaveCalls)
	})
}
