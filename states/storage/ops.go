package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
)

func (s *OSSState) Stat(ctx context.Context, path string) (*models.FsStat, error) {
	return s.store.Stat(ctx, oss.ResolveObjectKey(s.rootPath, path))
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

	base := toListingPrefix(resolved)
	ch, err := s.store.List(ctx, base, true)
	if err != nil {
		return err
	}

	_, err = s.store.Stat(ctx, resolved)
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
		if info.IsDir || strings.HasSuffix(info.Key, "/") {
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
	obj, err := s.store.Open(ctx, key)
	if err != nil {
		return fmt.Errorf("get object %s failed: %w", key, err)
	}
	if closer, ok := obj.(io.Closer); ok {
		defer closer.Close()
	}

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
