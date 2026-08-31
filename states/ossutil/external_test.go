package ossutil

import (
	"context"
	"testing"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
)

// resolverTestStore is a minimal oss.ObjectStore stub for path-routing tests.
// It is never opened, only passed through by ManifestPathResolver.
type resolverTestStore struct{}

func (s *resolverTestStore) Open(ctx context.Context, key string, opts ...oss.OpenOption) (storagecommon.ReadSeeker, error) {
	return nil, nil
}

func (s *resolverTestStore) Stat(ctx context.Context, key string) (*models.FsStat, error) {
	return nil, nil
}

func (s *resolverTestStore) List(ctx context.Context, prefix string, recursive bool) (<-chan oss.ObjectInfo, error) {
	return nil, nil
}

func TestManifestPathResolverMixedManifest(t *testing.T) {
	location := ExternalSourceLocation{
		Host:     "oss-cn-hangzhou.aliyuncs.com",
		Bucket:   "test-oss-0815",
		RootPath: "testlake/parquet",
	}
	internalStore := &resolverTestStore{}
	externalStore := &resolverTestStore{}

	// manifestBasePath mirrors {rootPath}/files/insert_log/{coll}/{part}/{seg}
	resolver := NewManifestPathResolver(internalStore, "by-dev/files/insert_log/1/2/3", externalStore, location)

	tests := []struct {
		name      string
		file      string
		dirPrefix string
		wantStore *resolverTestStore
		wantKey   string
		wantBack  FileBackend
		wantErr   bool
	}{
		{
			name:      "external absolute uri",
			file:      "oss://oss-cn-hangzhou.aliyuncs.com/test-oss-0815/testlake/parquet/part-0001.parquet",
			dirPrefix: "_data",
			wantStore: externalStore,
			wantKey:   "testlake/parquet/part-0001.parquet",
			wantBack:  FileBackendExternal,
		},
		{
			name:      "external ROOT_PATH placeholder",
			file:      "ROOT_PATH/part-0002.parquet",
			dirPrefix: "_data",
			wantStore: externalStore,
			wantKey:   "testlake/parquet/part-0002.parquet",
			wantBack:  FileBackendExternal,
		},
		{
			name:      "internal function output file",
			file:      "abc-123.parquet",
			dirPrefix: "_data",
			wantStore: internalStore,
			wantKey:   "by-dev/files/insert_log/1/2/3/_data/abc-123.parquet",
			wantBack:  FileBackendInternal,
		},
		{
			name:      "internal bm25 stats",
			file:      "bm25.3/0",
			dirPrefix: "_stats",
			wantStore: internalStore,
			wantKey:   "by-dev/files/insert_log/1/2/3/_stats/bm25.3/0",
			wantBack:  FileBackendInternal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store, key, backend, err := resolver.Resolve(tt.file, tt.dirPrefix)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Resolve() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if store != tt.wantStore {
				t.Fatalf("Resolve() store mismatch: got %#v, want %#v", store, tt.wantStore)
			}
			if key != tt.wantKey {
				t.Fatalf("Resolve() key = %q, want %q", key, tt.wantKey)
			}
			if backend != tt.wantBack {
				t.Fatalf("Resolve() backend = %v, want %v", backend, tt.wantBack)
			}
		})
	}
}

func TestParseExternalSpecFullKeys(t *testing.T) {
	raw := `{
		"format": "parquet",
		"extfs": {
			"cloud_provider": "aws",
			"region": "us-east-1",
			"role_arn": "arn:aws:iam::1:role/r",
			"session_name": "birdwatcher",
			"external_id": "ext-123",
			"access_key_id": "AKIA",
			"access_key_value": "secret",
			"use_iam": false,
			"iam_endpoint": "https://sts.example.com",
			"bucket_name": "override-bucket",
			"storage_type": "remote",
			"use_ssl": true,
			"load_frequency": "3600"
		}
	}`

	spec, err := ParseExternalSpec(raw)
	if err != nil {
		t.Fatalf("ParseExternalSpec() error = %v", err)
	}
	if spec.Format != "parquet" {
		t.Fatalf("unexpected format: %s", spec.Format)
	}
	if spec.CloudProvider != "aws" {
		t.Fatalf("unexpected cloud provider: %s", spec.CloudProvider)
	}
	if spec.Region != "us-east-1" {
		t.Fatalf("unexpected region: %s", spec.Region)
	}
	if spec.RoleARN != "arn:aws:iam::1:role/r" {
		t.Fatalf("unexpected role arn: %s", spec.RoleARN)
	}
	if spec.RoleSessionName != "birdwatcher" {
		t.Fatalf("unexpected session name: %s", spec.RoleSessionName)
	}
	if spec.ExternalID != "ext-123" {
		t.Fatalf("unexpected external id: %s", spec.ExternalID)
	}
	if spec.AK != "AKIA" || spec.SK != "secret" {
		t.Fatalf("unexpected access keys: %q/%q", spec.AK, spec.SK)
	}
	if spec.UseIAM {
		t.Fatalf("expected use_iam=false")
	}
	if spec.IAMEndpoint != "https://sts.example.com" {
		t.Fatalf("unexpected iam endpoint: %s", spec.IAMEndpoint)
	}
	if spec.BucketName != "override-bucket" {
		t.Fatalf("unexpected bucket name: %s", spec.BucketName)
	}
	if spec.StorageType != "remote" {
		t.Fatalf("unexpected storage type: %s", spec.StorageType)
	}
	if spec.UseSSL == nil || !*spec.UseSSL {
		t.Fatalf("expected use_ssl=true, got %#v", spec.UseSSL)
	}
	if spec.LoadFrequency != 3600 {
		t.Fatalf("unexpected load frequency: %d", spec.LoadFrequency)
	}
}

func TestParseExternalSpecEmpty(t *testing.T) {
	spec, err := ParseExternalSpec("")
	if err != nil {
		t.Fatalf("ParseExternalSpec() error = %v", err)
	}
	if spec.Format != "" {
		t.Fatalf("unexpected format: %s", spec.Format)
	}
}

func TestParseExternalSpecRejectsUnsupportedFormat(t *testing.T) {
	raw := `{"format":"lance-table","extfs":{}}`
	if _, err := ParseExternalSpec(raw); err == nil {
		t.Fatalf("expected error for unsupported format")
	}
}

func TestParseExternalSource(t *testing.T) {
	location, err := ParseExternalSource("oss://oss-cn-hangzhou.aliyuncs.com/test-oss-0815/testlake/parquet")
	if err != nil {
		t.Fatalf("ParseExternalSource() error = %v", err)
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
	location := ExternalSourceLocation{
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
			got, err := ResolveExternalObjectKey(location, tt.file)
			if err != nil {
				t.Fatalf("ResolveExternalObjectKey() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("ResolveExternalObjectKey() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestInferCloudProviderFromScheme(t *testing.T) {
	tests := []struct {
		scheme string
		want   string
	}{
		{scheme: "oss", want: "aliyun"},
		{scheme: "s3", want: "aws"},
		{scheme: "aws", want: "aws"},
		{scheme: "minio", want: "aws"},
		{scheme: "gs", want: "gcp"},
		{scheme: "gcs", want: "gcp"},
		{scheme: "cos", want: "tencent"},
		{scheme: "obs", want: "huawei"},
		{scheme: "unknown", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.scheme, func(t *testing.T) {
			if got := InferCloudProviderFromScheme(tt.scheme); got != tt.want {
				t.Fatalf("InferCloudProviderFromScheme(%q) = %q, want %q", tt.scheme, got, tt.want)
			}
		})
	}
}

func TestDeriveEndpoint(t *testing.T) {
	tests := []struct {
		provider string
		region   string
		want     string
	}{
		{provider: "aws", region: "us-east-1", want: "https://s3.us-east-1.amazonaws.com"},
		{provider: "aws", region: "cn-north-1", want: "https://s3.cn-north-1.amazonaws.com.cn"},
		{provider: "aws", region: "", want: ""},
		{provider: "gcp", region: "", want: "https://storage.googleapis.com"},
		{provider: "aliyun", region: "cn-hangzhou", want: "https://oss-cn-hangzhou.aliyuncs.com"},
		{provider: "aliyun", region: "", want: ""},
		{provider: "tencent", region: "ap-guangzhou", want: "https://cos.ap-guangzhou.myqcloud.com"},
		{provider: "huawei", region: "cn-north-4", want: "https://obs.cn-north-4.myhuaweicloud.com"},
		{provider: "azure", region: "public", want: "core.windows.net"},
		{provider: "azure", region: "china", want: "core.chinacloudapi.cn"},
		{provider: "azure", region: "", want: ""},
		{provider: "unknown", region: "us-east-1", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.provider+"/"+tt.region, func(t *testing.T) {
			if got := DeriveEndpoint(tt.provider, tt.region); got != tt.want {
				t.Fatalf("DeriveEndpoint(%q, %q) = %q, want %q", tt.provider, tt.region, got, tt.want)
			}
		})
	}
}

func TestIsCloudEndpointHost(t *testing.T) {
	tests := []struct {
		host string
		want bool
	}{
		{host: "s3.us-east-1.amazonaws.com", want: true},
		{host: "storage.googleapis.com", want: true},
		{host: "oss-cn-hangzhou.aliyuncs.com", want: true},
		{host: "cos.ap-guangzhou.myqcloud.com", want: true},
		{host: "myacct.core.windows.net", want: true},
		{host: "my-bucket", want: false},
		{host: "localhost:9000", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			if got := IsCloudEndpointHost(tt.host); got != tt.want {
				t.Fatalf("IsCloudEndpointHost(%q) = %v, want %v", tt.host, got, tt.want)
			}
		})
	}
}

func TestResolveExternalSourceAWSForm(t *testing.T) {
	// s3://bucket/key with AWS cloud_provider + region: host is the bucket,
	// endpoint is derived.
	location, err := ResolveExternalSource("s3://my-bucket/root/file", ExternalSourceSpec{
		CloudProvider: "aws",
		Region:        "us-east-1",
	})
	if err != nil {
		t.Fatalf("ResolveExternalSource() error = %v", err)
	}
	if location.Form != LocationFormAWS {
		t.Fatalf("expected AWS form, got %v", location.Form)
	}
	if location.Bucket != "my-bucket" {
		t.Fatalf("unexpected bucket: %s", location.Bucket)
	}
	if location.RootPath != "root/file" {
		t.Fatalf("unexpected root path: %s", location.RootPath)
	}
	if location.Address != "https://s3.us-east-1.amazonaws.com" {
		t.Fatalf("unexpected address: %s", location.Address)
	}
}

func TestResolveExternalSourceMilvusForm(t *testing.T) {
	// oss://endpoint/bucket/key stays Milvus form regardless of spec.
	location, err := ResolveExternalSource("oss://oss-cn-hangzhou.aliyuncs.com/test-oss-0815/testlake/parquet",
		ExternalSourceSpec{CloudProvider: "aliyun", Region: "cn-hangzhou"})
	if err != nil {
		t.Fatalf("ResolveExternalSource() error = %v", err)
	}
	if location.Form != LocationFormMilvus {
		t.Fatalf("expected Milvus form, got %v", location.Form)
	}
	if location.Bucket != "test-oss-0815" {
		t.Fatalf("unexpected bucket: %s", location.Bucket)
	}
	if location.RootPath != "testlake/parquet" {
		t.Fatalf("unexpected root path: %s", location.RootPath)
	}
	if location.Address != "oss-cn-hangzhou.aliyuncs.com" {
		t.Fatalf("unexpected address: %s", location.Address)
	}
}

func TestResolveExternalSourceCloudEndpointHostStaysMilvus(t *testing.T) {
	// s3://s3.amazonaws.com/bucket/key: host is a known cloud endpoint, so it
	// stays Milvus form even with a regional spec.
	location, err := ResolveExternalSource("s3://s3.amazonaws.com/my-bucket/key",
		ExternalSourceSpec{CloudProvider: "aws", Region: "us-east-1"})
	if err != nil {
		t.Fatalf("ResolveExternalSource() error = %v", err)
	}
	if location.Form != LocationFormMilvus {
		t.Fatalf("expected Milvus form, got %v", location.Form)
	}
	if location.Bucket != "my-bucket" {
		t.Fatalf("unexpected bucket: %s", location.Bucket)
	}
}

func TestResolveExternalObjectKeyAWSForm(t *testing.T) {
	location := ExternalSourceLocation{
		Scheme:   "s3",
		Host:     "my-bucket",
		Bucket:   "my-bucket",
		RootPath: "root",
		Address:  "https://s3.us-east-1.amazonaws.com",
		Form:     LocationFormAWS,
	}

	tests := []struct {
		name string
		file string
		want string
	}{
		{
			name: "absolute aws uri",
			file: "s3://my-bucket/root/data/part-0001.parquet",
			want: "root/data/part-0001.parquet",
		},
		{
			name: "relative path",
			file: "data/part-0002.parquet",
			want: "root/data/part-0002.parquet",
		},
		{
			name: "mismatched bucket",
			file: "s3://other-bucket/root/data.parquet",
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ResolveExternalObjectKey(location, tt.file)
			if tt.want == "" {
				if err == nil {
					t.Fatalf("expected error for %q, got key %q", tt.file, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ResolveExternalObjectKey() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("ResolveExternalObjectKey() = %s, want %s", got, tt.want)
			}
		})
	}
}
