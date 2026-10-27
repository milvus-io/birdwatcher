package storage

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/birdwatcher/storage/manifest"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// Count actual HTTP GETs through the MinIO SDK, not only ReaderAt invocations.
// One-row Parquet groups reproduce the small-range request pattern.
func TestManifestSegmentReaderBuffersMinioRequests(t *testing.T) {
	const rows = 128
	var values [][]any
	for i := range rows {
		values = append(values, []any{int64(i), int64(1000 + i), int64(i % 17)})
	}
	data := scanTestParquet(t, []arrow.Field{{Name: "100", Type: arrow.PrimitiveTypes.Int64}, {Name: "1", Type: arrow.PrimitiveTypes.Int64}, {Name: "101", Type: arrow.PrimitiveTypes.Int64}}, values, 1)
	require.Less(t, len(data), common.DefaultReadBufferSize)
	var gets atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			gets.Add(1)
		}
		w.Header().Set("ETag", `"fixture"`)
		http.ServeContent(w, r, "data.parquet", time.Unix(1700000000, 0), bytes.NewReader(data))
	}))
	defer server.Close()
	newStore := func() oss.ObjectStore {
		client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{Secure: false, Region: "us-east-1", Creds: credentials.NewStaticV4("test-access", "test-secret", "")})
		require.NoError(t, err)
		return oss.NewMinioObjectStoreWithBucket(client, "bucket")
	}
	ctx := context.Background()
	// Baseline uses the previous direct Parquet read path.
	obj, err := newStore().Open(ctx, "data.parquet")
	require.NoError(t, err)
	pq, err := file.NewParquetReader(obj)
	require.NoError(t, err)
	ar, err := pqarrow.NewFileReader(pq, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	require.NoError(t, err)
	rr, err := ar.GetRecordReader(ctx, nil, nil)
	require.NoError(t, err)
	var baselineRows int64
	for rr.Next() {
		baselineRows += rr.Record().NumRows()
	}
	if err := rr.Err(); err != nil {
		require.ErrorIs(t, err, io.EOF)
	}
	rr.Release()
	require.NoError(t, pq.Close())
	baselineGETs := gets.Swap(0)
	require.EqualValues(t, rows, baselineRows)
	store := newStore()
	m := &manifest.Manifest{ColumnGroups: []manifest.ColumnGroup{{Columns: []string{"100", "1", "101"}, Format: "parquet", Files: []manifest.ColumnGroupFile{{Path: "data.parquet", EndIndex: rows}}}}}
	r, err := NewManifestSegmentReader(scanTestSegment(), m, scanTestFields(schemapb.DataType_Int64), func(key string) (common.ReadSeeker, error) { return store.Open(ctx, key) })
	require.NoError(t, err)
	defer r.Close()
	var bufferedRows int
	for {
		batch, _, err := r.Next(ctx)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		for row := range batch.Len() {
			pk, ok := DeserializeItem(batch.Column(100), schemapb.DataType_Int64, row)
			require.True(t, ok)
			require.EqualValues(t, bufferedRows, pk)
			value, ok := DeserializeItem(batch.Column(101), schemapb.DataType_Int64, row)
			require.True(t, ok)
			require.EqualValues(t, bufferedRows%17, value)
			bufferedRows++
		}
	}
	require.NoError(t, r.Close())
	bufferedGETs := gets.Load()
	require.Equal(t, rows, bufferedRows)
	require.EqualValues(t, 1, bufferedGETs)
	require.Greater(t, baselineGETs, bufferedGETs*10)
	t.Logf("rows=%d object_bytes=%d default_buffer=%d HTTP_GETs: direct=%d buffered=%d", rows, len(data), common.DefaultReadBufferSize, baselineGETs, bufferedGETs)
}
