package states

import (
	"bytes"
	"encoding/binary"
	"testing"

	binlogv1 "github.com/milvus-io/birdwatcher/storage/binlog/v1"
)

func TestValidateInspectParquetParam(t *testing.T) {
	tests := []struct {
		name    string
		param   InspectParquetParam
		wantErr bool
	}{
		{
			name:    "local file mode",
			param:   InspectParquetParam{FilePath: "/tmp/a.parquet"},
			wantErr: false,
		},
		{
			name:    "segment mode",
			param:   InspectParquetParam{SegmentID: 100},
			wantErr: false,
		},
		{
			name:    "segment auto storage version",
			param:   InspectParquetParam{SegmentID: 100, StorageVersion: "auto"},
			wantErr: false,
		},
		{
			name:    "segment storage version override",
			param:   InspectParquetParam{SegmentID: 100, StorageVersion: "2"},
			wantErr: false,
		},
		{
			name:    "external manifest mode",
			param:   InspectParquetParam{External: true, CollectionID: 1, ManifestSegmentID: 10},
			wantErr: false,
		},
		{
			name:    "external file mode",
			param:   InspectParquetParam{External: true, CollectionID: 1, ExternalFile: "a/b.parquet"},
			wantErr: false,
		},
		{
			name:    "missing selectors",
			param:   InspectParquetParam{},
			wantErr: true,
		},
		{
			name:    "conflicting local selectors",
			param:   InspectParquetParam{FilePath: "/tmp/a.parquet", SegmentID: 10},
			wantErr: true,
		},
		{
			name:    "local file rejects auto storage version",
			param:   InspectParquetParam{FilePath: "/tmp/a.parquet", StorageVersion: "auto"},
			wantErr: true,
		},
		{
			name:    "invalid storage version",
			param:   InspectParquetParam{SegmentID: 100, StorageVersion: "invalid"},
			wantErr: true,
		},
		{
			name:    "negative storage version",
			param:   InspectParquetParam{SegmentID: 100, StorageVersion: "-1"},
			wantErr: true,
		},
		{
			name:    "external missing collection",
			param:   InspectParquetParam{External: true, ManifestSegmentID: 10},
			wantErr: true,
		},
		{
			name:    "external missing target",
			param:   InspectParquetParam{External: true, CollectionID: 1},
			wantErr: true,
		},
		{
			name:    "external conflicting targets",
			param:   InspectParquetParam{External: true, CollectionID: 1, ManifestSegmentID: 10, ExternalFile: "a.parquet"},
			wantErr: true,
		},
		{
			name:    "external conflicts with segment",
			param:   InspectParquetParam{External: true, CollectionID: 1, ManifestSegmentID: 10, SegmentID: 20},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateInspectParquetParam(&tt.param)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateInspectParquetParam() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseInspectStorageVersionOption(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		wantMode    inspectStorageVersionMode
		wantVersion int64
		wantErr     bool
	}{
		{name: "empty uses meta", value: "", wantMode: inspectStorageVersionFromMeta},
		{name: "meta", value: "meta", wantMode: inspectStorageVersionFromMeta},
		{name: "auto case insensitive", value: " AUTO ", wantMode: inspectStorageVersionAuto},
		{name: "v1 compatibility value", value: "1", wantMode: inspectStorageVersionOverride, wantVersion: 1},
		{name: "v2", value: "2", wantMode: inspectStorageVersionOverride, wantVersion: 2},
		{name: "future manifest version", value: "4", wantMode: inspectStorageVersionOverride, wantVersion: 4},
		{name: "negative", value: "-1", wantErr: true},
		{name: "invalid", value: "v2", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseInspectStorageVersionOption(tt.value)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseInspectStorageVersionOption() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if got.mode != tt.wantMode || got.version != tt.wantVersion {
				t.Fatalf("parseInspectStorageVersionOption() = %#v, want mode=%v version=%d", got, tt.wantMode, tt.wantVersion)
			}
		})
	}
}

func TestDetectBinlogStorageVersion(t *testing.T) {
	v1Header := make([]byte, 4)
	binary.LittleEndian.PutUint32(v1Header, uint32(binlogv1.MagicNumberV1))

	tests := []struct {
		name    string
		data    []byte
		want    int64
		wantErr bool
	}{
		{name: "v1 wrapper", data: append(v1Header, 0x01), want: 0},
		{name: "raw parquet", data: []byte("PAR1payload"), want: 2},
		{name: "unknown magic", data: []byte("NOPE"), wantErr: true},
		{name: "short header", data: []byte("PAR"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := bytes.NewReader(tt.data)
			got, err := detectBinlogStorageVersion(reader)
			if (err != nil) != tt.wantErr {
				t.Fatalf("detectBinlogStorageVersion() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("detectBinlogStorageVersion() = %d, want %d", got, tt.want)
			}
			position, seekErr := reader.Seek(0, 1)
			if seekErr != nil {
				t.Fatalf("reader.Seek() error = %v", seekErr)
			}
			if position != 0 {
				t.Fatalf("detectBinlogStorageVersion() changed reader position to %d", position)
			}
		})
	}
}

func TestParseExternalSpec(t *testing.T) {
	raw := `{
		"format": "parquet",
		"extfs": {
			"cloud_provider": "aliyun",
			"external_id": "ext-123",
			"region": "cn-hangzhou",
			"role_arn": "acs:ram::1:role/demo",
			"use_ssl": true
		}
	}`

	spec, err := parseExternalSpec(raw)
	if err != nil {
		t.Fatalf("parseExternalSpec() error = %v", err)
	}
	if spec.Format != "parquet" {
		t.Fatalf("unexpected format: %s", spec.Format)
	}
	if spec.CloudProvider != "aliyun" {
		t.Fatalf("unexpected cloud provider: %s", spec.CloudProvider)
	}
	if spec.Region != "cn-hangzhou" {
		t.Fatalf("unexpected region: %s", spec.Region)
	}
	if spec.RoleARN != "acs:ram::1:role/demo" {
		t.Fatalf("unexpected role arn: %s", spec.RoleARN)
	}
	if spec.ExternalID != "ext-123" {
		t.Fatalf("unexpected external id: %s", spec.ExternalID)
	}
	if spec.UseSSL == nil || !*spec.UseSSL {
		t.Fatalf("expected use_ssl=true, got %#v", spec.UseSSL)
	}
}

func TestParseExternalSource(t *testing.T) {
	location, err := parseExternalSource("oss://oss-cn-hangzhou.aliyuncs.com/test-oss-0815/testlake/parquet")
	if err != nil {
		t.Fatalf("parseExternalSource() error = %v", err)
	}
	if location.Scheme != "oss" {
		t.Fatalf("unexpected scheme: %s", location.Scheme)
	}
	if location.Host != "oss-cn-hangzhou.aliyuncs.com" {
		t.Fatalf("unexpected host: %s", location.Host)
	}
	if location.Bucket != "test-oss-0815" {
		t.Fatalf("unexpected bucket: %s", location.Bucket)
	}
	if location.RootPath != "testlake/parquet" {
		t.Fatalf("unexpected root path: %s", location.RootPath)
	}
}

func TestResolveExternalObjectKey(t *testing.T) {
	location := externalSourceLocation{
		Host:     "oss-cn-hangzhou.aliyuncs.com",
		Bucket:   "test-oss-0815",
		RootPath: "testlake/parquet",
	}

	tests := []struct {
		name string
		file string
		want string
	}{
		{
			name: "relative path",
			file: "part-0001.parquet",
			want: "testlake/parquet/part-0001.parquet",
		},
		{
			name: "already rooted",
			file: "testlake/parquet/part-0002.parquet",
			want: "testlake/parquet/part-0002.parquet",
		},
		{
			name: "full uri",
			file: "oss://oss-cn-hangzhou.aliyuncs.com/test-oss-0815/testlake/parquet/part-0003.parquet",
			want: "testlake/parquet/part-0003.parquet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveExternalObjectKey(location, tt.file)
			if err != nil {
				t.Fatalf("resolveExternalObjectKey() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("resolveExternalObjectKey() = %s, want %s", got, tt.want)
			}
		})
	}
}
