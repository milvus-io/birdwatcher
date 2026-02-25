package storage

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/fatih/color"
	"github.com/minio/minio-go/v7"

	"github.com/milvus-io/birdwatcher/framework"
)

func (s *OSSState) getBase() string {
	base := s.prefix
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}

	return strings.TrimPrefix(base, "/")
}

// resolvePath resolves a user-provided path (absolute or relative) against the current prefix.
func (s *OSSState) resolvePath(inputPath string) string {
	var resolved string
	if strings.HasPrefix(inputPath, "/") {
		resolved = strings.TrimPrefix(inputPath, "/")
	} else {
		resolved = path.Join(s.prefix, inputPath)
	}
	resolved = path.Clean(resolved)
	if resolved == "." {
		resolved = ""
	}
	// Clamp above-root paths to root.
	for strings.HasPrefix(resolved, "..") {
		resolved = strings.TrimPrefix(resolved, "..")
		resolved = strings.TrimPrefix(resolved, "/")
	}
	return resolved
}

// toListingPrefix converts a resolved path to a MinIO ListObjects prefix (trailing /, no leading /).
func toListingPrefix(resolved string) string {
	if resolved == "" {
		return ""
	}
	return resolved + "/"
}

type LsParam struct {
	framework.ParamBase `use:"ls [suggester:minio-path]" desc:"ls file/folder"`

	prefix string
}

func (p *LsParam) ParseArgs(args []string) error {
	if len(args) == 0 {
		return nil
	}

	if len(args) > 1 {
		return errors.New("too many parameters")
	}

	p.prefix = args[0]

	return nil
}

func (s *OSSState) LsCommand(ctx context.Context, p *LsParam) error {
	var base string
	if p.prefix != "" {
		base = toListingPrefix(s.resolvePath(p.prefix))
	} else {
		base = s.getBase()
	}
	ch := s.client.Client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
		Prefix:    base,
		Recursive: false,
	})

	dc := color.New(color.FgCyan)
	fc := color.New(color.FgGreen)
	for info := range ch {
		name := strings.TrimPrefix(info.Key, base)
		c := fc
		isDirectory := false
		if strings.HasSuffix(name, "/") {
			name = strings.TrimSuffix(name, "/")
			c = dc
			isDirectory = true
		}
		fmt.Printf("%s", c.Sprint(name))
		if !isDirectory {
			fmt.Printf("\t Size: %d", info.Size)
		}
		fmt.Println()
	}
	return nil
}

type CdParam struct {
	framework.ParamBase `use:"cd [suggester:minio-path]" desc:"change directory"`

	prefix string
}

func (p *CdParam) ParseArgs(args []string) error {
	if len(args) == 0 {
		return nil
	}

	if len(args) > 1 {
		return errors.New("too many parameters")
	}

	p.prefix = args[0]

	return nil
}

func (s *OSSState) CdCommand(ctx context.Context, p *CdParam) error {
	// cd with no args or cd / → go to root
	if p.prefix == "" || p.prefix == "/" {
		s.prefix = ""
		return nil
	}

	resolved := s.resolvePath(p.prefix)

	// If resolved to root, just set it
	if resolved == "" {
		s.prefix = ""
		return nil
	}

	// Validate the target directory exists by listing with the resolved prefix
	listPrefix := toListingPrefix(resolved)
	ch := s.client.Client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
		Prefix:    listPrefix,
		Recursive: false,
	})

	// Check if any object exists under this prefix
	info, ok := <-ch
	if !ok || info.Err != nil {
		return fmt.Errorf("folder %s not exists", p.prefix)
	}

	s.prefix = resolved
	return nil
}

type PwdParam struct {
	framework.ParamBase `use:"pwd" desc:"print current working directory path for minio"`
}

func (s *OSSState) PwdCommand(ctx context.Context, p *PwdParam) error {
	fmt.Println(s.getBase())
	return nil
}
