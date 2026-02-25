package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
)

func (s *OSSState) Stat(ctx context.Context, path string) (*models.FsStat, error) {
	info, err := s.client.Client.StatObject(ctx, s.bucket, path, minio.StatObjectOptions{})
	if err != nil {
		return nil, err
	}

	result := &models.FsStat{
		Size: info.Size,
	}
	return result, nil
}

type GetParam struct {
	framework.ParamBase `use:"get [suggester:minio-path]" desc:"download file from minio to local"`
	TargetDir           string `name:"target" default:"" desc:"target local directory"`

	remotePath string
}

func (p *GetParam) ParseArgs(args []string) error {
	if len(args) == 0 {
		return errors.New("remote file path is required")
	}
	if len(args) > 1 {
		return errors.New("too many parameters")
	}
	p.remotePath = args[0]
	return nil
}

func (s *OSSState) GetCommand(ctx context.Context, p *GetParam) error {
	resolved := s.resolvePath(p.remotePath)
	if resolved == "" {
		return errors.New("cannot download root")
	}

	// List objects under the resolved path to find matching files
	base := toListingPrefix(resolved)
	ch := s.client.Client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
		Prefix:    base,
		Recursive: true,
	})

	// Also check if the resolved path is an exact file (not a directory)
	_, err := s.client.Client.StatObject(ctx, s.bucket, resolved, minio.StatObjectOptions{})
	isFile := err == nil

	if isFile {
		return s.downloadFile(ctx, resolved, p.TargetDir)
	}

	// Try as directory: download all files under prefix
	count := 0
	for info := range ch {
		if info.Err != nil {
			return fmt.Errorf("list objects error: %w", info.Err)
		}
		if strings.HasSuffix(info.Key, "/") {
			continue
		}
		if err := s.downloadFile(ctx, info.Key, p.TargetDir); err != nil {
			return err
		}
		count++
	}

	if count == 0 {
		return fmt.Errorf("no file found at %s", p.remotePath)
	}
	fmt.Printf("downloaded %d file(s)\n", count)
	return nil
}

func (s *OSSState) downloadFile(ctx context.Context, key string, targetDir string) error {
	obj, err := s.client.Client.GetObject(ctx, s.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("get object %s failed: %w", key, err)
	}
	defer obj.Close()

	fileName := path.Base(key)
	localPath := fileName
	if targetDir != "" {
		if err := os.MkdirAll(targetDir, 0o755); err != nil {
			return fmt.Errorf("create target directory failed: %w", err)
		}
		localPath = path.Join(targetDir, fileName)
	}

	f, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("create local file %s failed: %w", localPath, err)
	}
	defer f.Close()

	written, err := io.Copy(f, obj)
	if err != nil {
		return fmt.Errorf("write file %s failed: %w", localPath, err)
	}

	fmt.Printf("%s -> %s (%d bytes)\n", key, localPath, written)
	return nil
}
