package kv

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"os"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"

	"github.com/milvus-io/birdwatcher/models"
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

func newTestFileAuditKV(t *testing.T, cli MetaKV) (*FileAuditKV, string) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "audit-*.log")
	require.NoError(t, err)
	t.Cleanup(func() { f.Close() })
	return NewFileAuditKV(cli, f), f.Name()
}

// readAuditRecords replays the length-prefixed records written to an audit
// log file, decoding each as either an AuditHeader or a raw mvccpb.KeyValue
// depending on the caller's expected sequence.
func readAuditRecords(t *testing.T, path string) [][]byte {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	var records [][]byte
	for {
		lb := make([]byte, 8)
		_, err := io.ReadFull(f, lb)
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)

		n := binary.LittleEndian.Uint64(lb)
		data := make([]byte, n)
		if n > 0 {
			_, err = io.ReadFull(f, data)
			require.NoError(t, err)
		}
		records = append(records, data)
	}
	return records
}

func decodeAuditHeader(t *testing.T, data []byte) *models.AuditHeader {
	t.Helper()
	header := &models.AuditHeader{}
	require.NoError(t, proto.Unmarshal(data, header))
	return header
}

func decodeKeyValue(t *testing.T, data []byte) *mvccpb.KeyValue {
	t.Helper()
	kv := &mvccpb.KeyValue{}
	require.NoError(t, proto.Unmarshal(data, protoadapt.MessageV2Of(kv)))
	return kv
}

func TestFileAuditKVMultiSave(t *testing.T) {
	t.Run("delegates and persists values", func(t *testing.T) {
		fake := newFakeMetaKV()
		audit, _ := newTestFileAuditKV(t, fake)

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
		audit, _ := newTestFileAuditKV(t, fake)

		err := audit.MultiSave(context.TODO(), []string{"k1"}, []string{"v1"})
		assert.ErrorIs(t, err, fake.multiSaveErr)
		assert.Empty(t, fake.data)
	})

	t.Run("rejects mismatched keys and values", func(t *testing.T) {
		fake := newFakeMetaKV()
		audit, _ := newTestFileAuditKV(t, fake)

		err := audit.MultiSave(context.TODO(), []string{"k1", "k2"}, []string{"v1"})
		assert.Error(t, err)
		assert.Empty(t, fake.multiSaveCalls)
	})

	t.Run("writes OpPut/OpPutBefore/OpPutAfter headers and key/value records", func(t *testing.T) {
		fake := newFakeMetaKV()
		audit, path := newTestFileAuditKV(t, fake)

		keys := []string{"k1", "k2"}
		values := []string{"v1", "v2"}
		require.NoError(t, audit.MultiSave(context.TODO(), keys, values))

		records := readAuditRecords(t, path)
		require.Len(t, records, 5, "expected OpPut header, OpPutBefore header, 2 key/value records, OpPutAfter header")

		opPut := decodeAuditHeader(t, records[0])
		assert.EqualValues(t, models.AuditOpType_OpPut, opPut.GetOpType())
		assert.EqualValues(t, len(keys), opPut.GetEntriesNum())

		opPutBefore := decodeAuditHeader(t, records[1])
		assert.EqualValues(t, models.AuditOpType_OpPutBefore, opPutBefore.GetOpType())
		assert.EqualValues(t, len(keys), opPutBefore.GetEntriesNum())

		for i, key := range keys {
			kv := decodeKeyValue(t, records[2+i])
			assert.Equal(t, key, string(kv.Key))
			assert.Equal(t, values[i], string(kv.Value))
		}

		opPutAfter := decodeAuditHeader(t, records[4])
		assert.EqualValues(t, models.AuditOpType_OpPutAfter, opPutAfter.GetOpType())
		assert.EqualValues(t, len(keys), opPutAfter.GetEntriesNum())
	})

	t.Run("writes only OpPutAfter header when underlying save fails", func(t *testing.T) {
		fake := newFakeMetaKV()
		fake.multiSaveErr = errors.New("injected failure")
		audit, path := newTestFileAuditKV(t, fake)

		err := audit.MultiSave(context.TODO(), []string{"k1"}, []string{"v1"})
		require.Error(t, err)

		records := readAuditRecords(t, path)
		require.Len(t, records, 2, "expected OpPut header and OpPutAfter header only, no key/value records on failure")

		opPut := decodeAuditHeader(t, records[0])
		assert.EqualValues(t, models.AuditOpType_OpPut, opPut.GetOpType())

		opPutAfter := decodeAuditHeader(t, records[1])
		assert.EqualValues(t, models.AuditOpType_OpPutAfter, opPutAfter.GetOpType())
	})
}
