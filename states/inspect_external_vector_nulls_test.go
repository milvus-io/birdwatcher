package states

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
)

type reopeningObjectStore struct {
	data  []byte
	opens atomic.Int64
}

func (s *reopeningObjectStore) Open(context.Context, string, ...oss.OpenOption) (storagecommon.ReadSeeker, error) {
	s.opens.Add(1)
	return bytes.NewReader(s.data), nil
}

func (*reopeningObjectStore) Stat(context.Context, string) (*models.FsStat, error) {
	return nil, nil
}

func (*reopeningObjectStore) List(context.Context, string, bool) (<-chan oss.ObjectInfo, error) {
	return nil, nil
}

func TestClassifyExternalVectorArray_ListNullKinds(t *testing.T) {
	builder := array.NewListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Float32)
	defer builder.Release()
	values := builder.ValueBuilder().(*array.Float32Builder)

	builder.Append(true)
	values.AppendValues([]float32{1, 2}, nil)
	builder.Append(true)
	values.AppendNulls(2)
	builder.Append(true)
	values.Append(3)
	values.AppendNull()
	builder.Append(false)
	builder.Append(true)
	values.Append(4)

	input := builder.NewArray()
	defer input.Release()

	got, err := classifyExternalVectorArray(input, schemapb.DataType_FloatVector, 2)
	if err != nil {
		t.Fatalf("classifyExternalVectorArray() error = %v", err)
	}
	want := externalVectorNullCounts{
		Rows:              5,
		ValidRows:         1,
		RowNullRows:       1,
		FullNullRows:      1,
		PartialNullRows:   1,
		InvalidLengthRows: 1,
	}
	if got != want {
		t.Fatalf("classifyExternalVectorArray() = %+v, want %+v", got, want)
	}
}

func TestClassifyExternalVectorArray_FixedSizeListNullKinds(t *testing.T) {
	builder := array.NewFixedSizeListBuilder(memory.DefaultAllocator, 2, arrow.PrimitiveTypes.Float32)
	defer builder.Release()
	values := builder.ValueBuilder().(*array.Float32Builder)

	builder.Append(true)
	values.AppendNulls(2)
	builder.Append(true)
	values.Append(1)
	values.AppendNull()
	builder.Append(false)
	values.AppendNulls(2)
	builder.Append(true)
	values.AppendValues([]float32{2, 3}, nil)

	input := builder.NewArray()
	defer input.Release()

	got, err := classifyExternalVectorArray(input, schemapb.DataType_FloatVector, 2)
	if err != nil {
		t.Fatalf("classifyExternalVectorArray() error = %v", err)
	}
	want := externalVectorNullCounts{
		Rows:            4,
		ValidRows:       1,
		RowNullRows:     1,
		FullNullRows:    1,
		PartialNullRows: 1,
	}
	if got != want {
		t.Fatalf("classifyExternalVectorArray() = %+v, want %+v", got, want)
	}
}

func TestClassifyExternalVectorArray_AdditionalArrowTypes(t *testing.T) {
	t.Run("large list", func(t *testing.T) {
		builder := array.NewLargeListBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Float32)
		defer builder.Release()
		values := builder.ValueBuilder().(*array.Float32Builder)

		builder.Append(true)
		values.AppendValues([]float32{1, 2}, nil)
		builder.Append(true)
		values.AppendNulls(2)

		input := builder.NewArray()
		defer input.Release()

		got, err := classifyExternalVectorArray(input, schemapb.DataType_FloatVector, 2)
		if err != nil {
			t.Fatalf("classifyExternalVectorArray() error = %v", err)
		}
		want := externalVectorNullCounts{Rows: 2, ValidRows: 1, FullNullRows: 1}
		if got != want {
			t.Fatalf("classifyExternalVectorArray() = %+v, want %+v", got, want)
		}
	})

	t.Run("list view", func(t *testing.T) {
		builder := array.NewListViewBuilder(memory.DefaultAllocator, arrow.PrimitiveTypes.Float32)
		defer builder.Release()
		values := builder.ValueBuilder().(*array.Float32Builder)

		builder.AppendWithSize(true, 2)
		values.AppendValues([]float32{1, 2}, nil)
		builder.AppendWithSize(true, 2)
		values.Append(3)
		values.AppendNull()

		input := builder.NewArray()
		defer input.Release()

		got, err := classifyExternalVectorArray(input, schemapb.DataType_FloatVector, 2)
		if err != nil {
			t.Fatalf("classifyExternalVectorArray() error = %v", err)
		}
		want := externalVectorNullCounts{Rows: 2, ValidRows: 1, PartialNullRows: 1}
		if got != want {
			t.Fatalf("classifyExternalVectorArray() = %+v, want %+v", got, want)
		}
	})

	t.Run("binary view", func(t *testing.T) {
		builder := array.NewBinaryViewBuilder(memory.DefaultAllocator)
		defer builder.Release()
		builder.Append([]byte{1, 2})
		builder.AppendNull()
		builder.Append([]byte{1})

		input := builder.NewArray()
		defer input.Release()

		got, err := classifyExternalVectorArray(input, schemapb.DataType_BinaryVector, 16)
		if err != nil {
			t.Fatalf("classifyExternalVectorArray() error = %v", err)
		}
		want := externalVectorNullCounts{
			Rows:              3,
			ValidRows:         1,
			RowNullRows:       1,
			InvalidLengthRows: 1,
		}
		if got != want {
			t.Fatalf("classifyExternalVectorArray() = %+v, want %+v", got, want)
		}
	})
}

func TestExpectedExternalVectorListLength(t *testing.T) {
	tests := []struct {
		name      string
		fieldType schemapb.DataType
		dim       int64
		childType arrow.DataType
		want      int64
	}{
		{name: "float semantic", fieldType: schemapb.DataType_FloatVector, dim: 8, childType: arrow.PrimitiveTypes.Float32, want: 8},
		{name: "float raw bytes", fieldType: schemapb.DataType_FloatVector, dim: 8, childType: arrow.PrimitiveTypes.Uint8, want: 32},
		{name: "float16 semantic", fieldType: schemapb.DataType_Float16Vector, dim: 8, childType: arrow.FixedWidthTypes.Float16, want: 8},
		{name: "float16 raw bytes", fieldType: schemapb.DataType_Float16Vector, dim: 8, childType: arrow.PrimitiveTypes.Uint8, want: 16},
		{name: "bfloat16 bytes", fieldType: schemapb.DataType_BFloat16Vector, dim: 8, childType: arrow.PrimitiveTypes.Uint8, want: 16},
		{name: "binary bytes", fieldType: schemapb.DataType_BinaryVector, dim: 16, childType: arrow.PrimitiveTypes.Uint8, want: 2},
		{name: "int8 semantic", fieldType: schemapb.DataType_Int8Vector, dim: 8, childType: arrow.PrimitiveTypes.Int8, want: 8},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := expectedExternalVectorListLength(test.fieldType, test.dim, test.childType)
			if err != nil {
				t.Fatalf("expectedExternalVectorListLength() error = %v", err)
			}
			if got != test.want {
				t.Fatalf("expectedExternalVectorListLength() = %d, want %d", got, test.want)
			}
		})
	}
}

func TestResolveExternalVectorField(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:       101,
				Name:          "embedding",
				DataType:      schemapb.DataType_FloatVector,
				ExternalField: "source_vector",
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "128"},
				},
			},
		},
	}
	field, externalField, dim, err := resolveExternalVectorField(schema, "embedding")
	if err != nil {
		t.Fatalf("resolveExternalVectorField() error = %v", err)
	}
	if field.GetFieldID() != 101 || externalField != "source_vector" || dim != 128 {
		t.Fatalf("resolveExternalVectorField() = field=%d external=%s dim=%d", field.GetFieldID(), externalField, dim)
	}
}

func TestContainsManifestColumn(t *testing.T) {
	columns := []string{"100", "101"}
	if !containsManifestColumn(columns, "vector", "101") {
		t.Fatal("containsManifestColumn() expected field ID match")
	}
	if containsManifestColumn(columns, "vector", "102") {
		t.Fatal("containsManifestColumn() unexpected match")
	}
}

func TestBuildExternalVectorScanIntervals(t *testing.T) {
	tests := []struct {
		name   string
		ranges []externalVectorSegmentRange
		want   []externalVectorScanInterval
	}{
		{
			name: "keeps disjoint ranges separate",
			ranges: []externalVectorSegmentRange{
				{StartIndex: 100, EndIndex: 200},
				{StartIndex: 1_000_000, EndIndex: 1_000_100},
			},
			want: []externalVectorScanInterval{
				{StartIndex: 100, EndIndex: 200, RangeIndexes: []int{0}},
				{StartIndex: 1_000_000, EndIndex: 1_000_100, RangeIndexes: []int{1}},
			},
		},
		{
			name: "merges overlapping and adjacent ranges",
			ranges: []externalVectorSegmentRange{
				{StartIndex: 20, EndIndex: 30},
				{StartIndex: 0, EndIndex: 10},
				{StartIndex: 8, EndIndex: 20},
				{StartIndex: 100, EndIndex: 110},
			},
			want: []externalVectorScanInterval{
				{StartIndex: 0, EndIndex: 30, RangeIndexes: []int{1, 2, 0}},
				{StartIndex: 100, EndIndex: 110, RangeIndexes: []int{3}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := buildExternalVectorScanIntervals(test.ranges)
			if err != nil {
				t.Fatalf("buildExternalVectorScanIntervals() error = %v", err)
			}
			if len(got) != len(test.want) {
				t.Fatalf("buildExternalVectorScanIntervals() = %+v, want %+v", got, test.want)
			}
			for i := range got {
				if got[i].StartIndex != test.want[i].StartIndex ||
					got[i].EndIndex != test.want[i].EndIndex ||
					!slices.Equal(got[i].RangeIndexes, test.want[i].RangeIndexes) {
					t.Fatalf("interval[%d] = %+v, want %+v", i, got[i], test.want[i])
				}
			}
		})
	}
}

func TestBuildExternalVectorScanIntervalsRejectsInvalidRange(t *testing.T) {
	_, err := buildExternalVectorScanIntervals([]externalVectorSegmentRange{{StartIndex: 10, EndIndex: 10}})
	if err == nil || !strings.Contains(err.Error(), "invalid external vector row range") {
		t.Fatalf("buildExternalVectorScanIntervals() error = %v", err)
	}
}

func TestInspectExternalVectorNullRangeProjectsVectorAcrossRowGroups(t *testing.T) {
	allocator := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32), Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()

	ids := builder.Field(0).(*array.Int64Builder)
	ids.AppendValues([]int64{10, 11, 12, 13, 14}, nil)
	vectors := builder.Field(1).(*array.ListBuilder)
	values := vectors.ValueBuilder().(*array.Float32Builder)

	vectors.Append(true)
	values.AppendValues([]float32{1, 2}, nil)
	vectors.Append(true)
	values.AppendNulls(2)
	vectors.Append(true)
	values.Append(3)
	values.AppendNull()
	vectors.Append(false)
	vectors.Append(true)
	values.AppendValues([]float32{4, 5}, nil)

	record := builder.NewRecord()
	defer record.Release()
	table := array.NewTableFromRecords(schema, []arrow.Record{record})
	defer table.Release()

	var parquetData bytes.Buffer
	if err := pqarrow.WriteTable(
		table,
		&parquetData,
		2,
		parquet.NewWriterProperties(),
		pqarrow.DefaultWriterProps(),
	); err != nil {
		t.Fatalf("WriteTable() error = %v", err)
	}

	objectResult := inspectExternalVectorNullObject(
		context.Background(),
		&singleObjectStore{object: bytes.NewReader(parquetData.Bytes())},
		externalVectorObjectJob{
			ObjectKey: "vectors.parquet",
			Ranges: []externalVectorSegmentRange{{
				SegmentID:  100,
				ObjectKey:  "vectors.parquet",
				StartIndex: 1,
				EndIndex:   4,
			}},
		},
		"vector",
		schemapb.DataType_FloatVector,
		2,
		2,
		true,
	)
	result := objectResult.Ranges[0]
	if result.InspectionError != "" {
		t.Fatalf("inspectExternalVectorNullObject() error = %s", result.InspectionError)
	}
	if result.Rows != 3 || result.ValidRows != 0 || result.RowNullRows != 1 ||
		result.FullNullRows != 1 || result.PartialNullRows != 1 || result.InvalidLengthRows != 0 {
		t.Fatalf("inspectExternalVectorNullObject() = %+v", result)
	}
}

func TestScanExternalVectorNullRangesGroupsObjectsAndSkipsCleanRowGroups(t *testing.T) {
	allocator := memory.DefaultAllocator
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "vector", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32), Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(allocator, schema)
	defer builder.Release()
	vectors := builder.Field(0).(*array.ListBuilder)
	values := vectors.ValueBuilder().(*array.Float32Builder)

	vectors.Append(true)
	values.AppendValues([]float32{1, 2}, nil)
	vectors.Append(true)
	values.AppendValues([]float32{3, 4}, nil)
	vectors.Append(true)
	values.AppendNulls(2)
	vectors.Append(true)
	values.Append(5)
	values.AppendNull()
	vectors.Append(false)
	vectors.Append(true)
	values.AppendValues([]float32{6, 7}, nil)

	record := builder.NewRecord()
	defer record.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.Record{record})
	defer tbl.Release()
	var parquetData bytes.Buffer
	if err := pqarrow.WriteTable(
		tbl,
		&parquetData,
		2,
		parquet.NewWriterProperties(),
		pqarrow.DefaultWriterProps(),
	); err != nil {
		t.Fatalf("WriteTable() error = %v", err)
	}

	ranges := []externalVectorSegmentRange{
		{SegmentID: 1, ObjectKey: "vectors.parquet", StartIndex: 0, EndIndex: 4},
		{SegmentID: 2, ObjectKey: "vectors.parquet", StartIndex: 1, EndIndex: 6},
	}
	t.Run("metadata fast path", func(t *testing.T) {
		store := &reopeningObjectStore{data: parquetData.Bytes()}
		var got []externalVectorObjectResult
		for result := range scanExternalVectorNullRanges(
			context.Background(), store, ranges, "vector",
			schemapb.DataType_FloatVector, 2, 4, 2, false,
		) {
			got = append(got, result)
		}
		if store.opens.Load() != 1 {
			t.Fatalf("object opens = %d, want 1", store.opens.Load())
		}
		if len(got) != 1 || got[0].RowGroupsSkipped != 1 || got[0].RowGroupsScanned != 2 {
			t.Fatalf("object result = %+v", got)
		}
		first, second := got[0].Ranges[0], got[0].Ranges[1]
		if first.Rows != 4 || first.MetadataNoNullRows != 2 || first.FullNullRows != 1 ||
			first.PartialNullRows != 1 || first.ValidRows != 0 {
			t.Fatalf("first range = %+v", first)
		}
		if second.Rows != 5 || second.MetadataNoNullRows != 1 || second.ValidRows != 1 ||
			second.RowNullRows != 1 || second.FullNullRows != 1 || second.PartialNullRows != 1 {
			t.Fatalf("second range = %+v", second)
		}
	})

	t.Run("exact path", func(t *testing.T) {
		store := &reopeningObjectStore{data: parquetData.Bytes()}
		result := <-scanExternalVectorNullRanges(
			context.Background(), store, ranges, "vector",
			schemapb.DataType_FloatVector, 2, 4, 2, true,
		)
		if store.opens.Load() != 1 || result.RowGroupsSkipped != 0 || result.RowGroupsScanned != 3 {
			t.Fatalf("opens=%d result=%+v", store.opens.Load(), result)
		}
		if result.Ranges[0].MetadataNoNullRows != 0 || result.Ranges[0].ValidRows != 2 {
			t.Fatalf("exact first range = %+v", result.Ranges[0])
		}
	})
}

func TestResolveExternalManifestLancePathPreservesFragmentID(t *testing.T) {
	location := externalSourceLocation{
		Scheme:   "s3",
		Host:     "s3.us-west-2.amazonaws.com",
		Bucket:   "bucket",
		RootPath: "dataset",
	}
	full := "s3://s3.us-west-2.amazonaws.com/bucket/dataset?fragment_id=12"
	got, err := resolveExternalManifestLancePath(location, full)
	if err != nil || got != full {
		t.Fatalf("resolve full path = %q, %v", got, err)
	}
	got, err = resolveExternalManifestLancePath(location, "dataset?fragment_id=13")
	if err != nil || got != "s3://s3.us-west-2.amazonaws.com/bucket/dataset?fragment_id=13" {
		t.Fatalf("resolve relative path = %q, %v", got, err)
	}
}

func TestRedactExternalVectorObjectKey(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "preserve numeric Lance fragment ID",
			raw:  "s3://endpoint/bucket/dataset?fragment_id=12",
			want: "s3://endpoint/bucket/dataset?fragment_id=12",
		},
		{
			name: "remove credentials and unrelated query",
			raw:  "s3://user:password@endpoint/bucket/dataset?fragment_id=12&token=secret#private",
			want: "s3://endpoint/bucket/dataset?fragment_id=12",
		},
		{
			name: "remove signed parquet query",
			raw:  "part.parquet?X-Amz-Signature=secret",
			want: "part.parquet",
		},
		{
			name: "drop invalid fragment ID",
			raw:  "dataset?fragment_id=secret",
			want: "dataset",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := redactExternalVectorObjectKey(test.raw); got != test.want {
				t.Fatalf("redactExternalVectorObjectKey() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSanitizeExternalVectorInspectionError(t *testing.T) {
	raw := "s3://user:password@endpoint/bucket/dataset?fragment_id=12&token=secret#private"
	message := sanitizeExternalVectorInspectionError(
		fmt.Errorf("open Lance reader for %s: access denied", raw),
		raw,
	)
	if strings.Contains(message, "password") || strings.Contains(message, "secret") ||
		strings.Contains(message, "token=") {
		t.Fatalf("inspection error contains credentials: %s", message)
	}
	if !strings.Contains(message, "s3://endpoint/bucket/dataset?fragment_id=12") {
		t.Fatalf("inspection error lost safe object identity: %s", message)
	}
}

func TestBuildLancePropertyValuesMinioUsesS3CompatibleBackend(t *testing.T) {
	values, err := buildLancePropertyValues(
		externalSourceLocation{
			Scheme:   "minio",
			Host:     "localhost:9000",
			Bucket:   "test-bucket",
			RootPath: "dataset",
		},
		externalSourceSpec{
			CloudProvider:  "minio",
			AccessKeyID:    "minio-ak",
			AccessKeyValue: "minio-sk",
		},
		1024,
	)
	if err != nil {
		t.Fatalf("buildLancePropertyValues() error = %v", err)
	}
	if _, ok := values["extfs.birdwatcher.cloud_provider"]; ok {
		t.Fatal("MinIO sentinel must not be forwarded to milvus-storage")
	}
	if values["extfs.birdwatcher.address"] != "localhost:9000" ||
		values["extfs.birdwatcher.use_ssl"] != "false" ||
		values["extfs.birdwatcher.use_virtual_host"] != "false" ||
		values["reader.metadata_cache.enable"] != "true" {
		t.Fatalf("unexpected MinIO Lance properties: %#v", values)
	}
}

func TestBuildLancePropertyValuesForwardsSupportedProvider(t *testing.T) {
	values, err := buildLancePropertyValues(
		externalSourceLocation{
			Scheme:   "s3",
			Host:     "s3.us-west-2.amazonaws.com",
			Bucket:   "test-bucket",
			RootPath: "dataset",
		},
		externalSourceSpec{
			CloudProvider:  "aws",
			Region:         "us-west-2",
			AccessKeyID:    "test-ak",
			AccessKeyValue: "test-sk",
		},
		1024,
	)
	if err != nil {
		t.Fatalf("buildLancePropertyValues() error = %v", err)
	}
	if values["extfs.birdwatcher.cloud_provider"] != "aws" {
		t.Fatalf("cloud provider = %q, want aws", values["extfs.birdwatcher.cloud_provider"])
	}
}

func TestBuildLancePropertyValuesForwardsAzureBroker(t *testing.T) {
	values, err := buildLancePropertyValues(
		externalSourceLocation{
			Scheme:   "azure",
			Host:     "core.windows.net",
			Bucket:   "container",
			RootPath: "dataset",
		},
		externalSourceSpec{
			CloudProvider:           "azure",
			Region:                  "westus3",
			AccessKeyID:             "storage-account",
			AzureClientID:           "client-id",
			AzureTenantID:           "tenant-id",
			AzureCredentialEndpoint: "https://broker.example.com/v1/credentials/assume-role",
			LoadFrequency:           3600,
		},
		1024,
	)
	if err != nil {
		t.Fatalf("buildLancePropertyValues() error = %v", err)
	}
	for key, want := range map[string]string{
		"extfs.birdwatcher.cloud_provider":            "azure",
		"extfs.birdwatcher.address":                   "core.windows.net",
		"extfs.birdwatcher.bucket_name":               "container",
		"extfs.birdwatcher.access_key_id":             "storage-account",
		"extfs.birdwatcher.azure_client_id":           "client-id",
		"extfs.birdwatcher.azure_tenant_id":           "tenant-id",
		"extfs.birdwatcher.azure_credential_endpoint": "https://broker.example.com/v1/credentials/assume-role",
		"extfs.birdwatcher.load_frequency":            "3600",
		"extfs.birdwatcher.use_iam":                   "false",
	} {
		if got := values[key]; got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
}

func TestRedactExternalVectorSource(t *testing.T) {
	if got := redactExternalVectorSource("s3://bucket/dataset"); got != "s3://bucket/dataset" {
		t.Fatalf("redactExternalVectorSource() = %q", got)
	}
	for _, source := range []string{
		"s3://bucket/dataset?token=secret",
		"s3://bucket/dataset#secret",
		"s3://user:password@bucket/dataset",
	} {
		if got := redactExternalVectorSource(source); got != "<redacted>" {
			t.Fatalf("redactExternalVectorSource(%q) = %q", source, got)
		}
	}
}

func TestSelectExternalCollectionSegments(t *testing.T) {
	externalSegment := &models.Segment{SegmentInfo: &datapb.SegmentInfo{
		ID:           1,
		ManifestPath: `{"ver":1,"base_path":"files"}`,
	}}
	regular := &models.Segment{SegmentInfo: &datapb.SegmentInfo{ID: 2}}

	got, err := selectExternalCollectionSegments([]*models.Segment{regular, externalSegment}, 0)
	if err != nil {
		t.Fatalf("selectExternalCollectionSegments() error = %v", err)
	}
	if len(got) != 1 || got[0].GetID() != externalSegment.GetID() {
		t.Fatalf("selectExternalCollectionSegments() = %v, want segment %d", got, externalSegment.GetID())
	}

	_, err = selectExternalCollectionSegments([]*models.Segment{regular}, regular.GetID())
	if err == nil || !strings.Contains(err.Error(), "not an external collection segment") {
		t.Fatalf("selectExternalCollectionSegments() error = %v, want explicit non-external error", err)
	}
}

func TestScanExternalVectorNullSegmentsRejectsNonExternalCollectionFirst(t *testing.T) {
	collection := models.NewCollection(&etcdpb.CollectionInfo{
		ID:     42,
		Schema: &schemapb.CollectionSchema{Name: "regular_collection"},
	}, "")

	_, err := (&InstanceState{}).scanExternalVectorNullSegments(
		context.Background(), collection, &ScanBinlogParams{})
	if err == nil || !strings.Contains(err.Error(),
		"collection 42 is not an external collection: external_source is empty") {
		t.Fatalf("scanExternalVectorNullSegments() error = %v", err)
	}
}
