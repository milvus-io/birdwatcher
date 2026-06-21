package oss

import (
	"context"
	"path"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"

	"github.com/milvus-io/birdwatcher/models"
	storagecommon "github.com/milvus-io/birdwatcher/storage/common"
)

type ObjectStore interface {
	Open(ctx context.Context, key string, opts ...OpenOption) (storagecommon.ReadSeeker, error)
	Stat(ctx context.Context, key string) (*models.FsStat, error)
	List(ctx context.Context, prefix string, recursive bool) (<-chan ObjectInfo, error)
}

type ObjectInfo struct {
	Key   string
	Size  int64
	IsDir bool
	Err   error
}

type ResolvedObjectStore struct {
	Store      ObjectStore
	BucketName string
	RootPath   string
}

type openSettings struct {
	rangeSet bool
	start    int64
	end      int64
}

type OpenOption func(*openSettings)

func WithOpenRange(start, end int64) OpenOption {
	return func(settings *openSettings) {
		settings.rangeSet = true
		settings.start = start
		settings.end = end
	}
}

func ResolveObjectKey(rootPath, logicalPath string) string {
	resolved := strings.TrimSpace(logicalPath)
	if resolved == "" {
		return ""
	}
	if strings.Contains(resolved, "ROOT_PATH") {
		resolved = strings.ReplaceAll(resolved, "ROOT_PATH", strings.Trim(rootPath, "/"))
	}
	resolved = strings.TrimPrefix(resolved, "/")
	if resolved == "" {
		return ""
	}
	resolved = path.Clean(resolved)
	if resolved == "." {
		return ""
	}
	return resolved
}

type minioObjectStore struct {
	client *minio.Client
	bucket string
}

func NewMinioObjectStore(client *MinioClient) ObjectStore {
	return &minioObjectStore{client: client.Client, bucket: client.BucketName}
}

func NewMinioObjectStoreWithBucket(client *minio.Client, bucketName string) ObjectStore {
	return &minioObjectStore{client: client, bucket: bucketName}
}

func MinioClientFromObjectStore(store ObjectStore) (*minio.Client, bool) {
	compat, ok := store.(*minioObjectStore)
	if !ok {
		return nil, false
	}
	return compat.client, true
}

func (s *minioObjectStore) Open(ctx context.Context, key string, opts ...OpenOption) (storagecommon.ReadSeeker, error) {
	settings := &openSettings{}
	for _, opt := range opts {
		if opt != nil {
			opt(settings)
		}
	}

	getOpts := minio.GetObjectOptions{}
	if settings.rangeSet {
		if settings.start < 0 || settings.end < settings.start {
			return nil, errors.New("invalid open range")
		}
		if err := getOpts.SetRange(settings.start, settings.end); err != nil {
			return nil, err
		}
	}
	return s.client.GetObject(ctx, s.bucket, key, getOpts)
}

func (s *minioObjectStore) Stat(ctx context.Context, key string) (*models.FsStat, error) {
	info, err := s.client.StatObject(ctx, s.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}
	return &models.FsStat{Size: info.Size}, nil
}

func (s *minioObjectStore) List(ctx context.Context, prefix string, recursive bool) (<-chan ObjectInfo, error) {
	source := s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: recursive})
	result := make(chan ObjectInfo)
	go func() {
		defer close(result)
		for info := range source {
			result <- ObjectInfo{
				Key:   info.Key,
				Size:  info.Size,
				IsDir: strings.HasSuffix(info.Key, "/"),
				Err:   info.Err,
			}
		}
	}()
	return result, nil
}
