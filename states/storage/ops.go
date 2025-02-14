package storage

import (
	"context"

	"github.com/minio/minio-go/v7"

	"github.com/milvus-io/birdwatcher/models"
)

func (s *MinioState) Stat(ctx context.Context, path string) (*models.FsStat, error) {
	info, err := s.client.StatObject(ctx, s.bucket, path, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}

	result := &models.FsStat{
		Size: info.Size,
	}
	return result, nil
}
