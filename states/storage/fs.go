package storage

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/fatih/color"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/minio/minio-go/v7"
)

func (s *MinioState) getBase() string {
	base := s.prefix
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}

	return strings.TrimPrefix(base, "/")
}

type LsParam struct {
	framework.ParamBase `use:"ls" desc:"ls file/folder"`

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

func (s *MinioState) LsCommand(ctx context.Context, p *LsParam) error {
	base := s.getBase()
	ch := s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
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
	framework.ParamBase `use:"cd" desc:"ls file/folder"`

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

func (s *MinioState) CdCommand(ctx context.Context, p *CdParam) error {

	base := s.getBase()

	// use absolute path
	if strings.HasPrefix(p.prefix, "/") {
		base = path.Dir(strings.TrimPrefix(p.prefix, "/"))
	} else {
		base = path.Join(base, path.Dir(p.prefix))
	}
	if base == "." {
		base = ""
	}
	p.prefix = strings.TrimSuffix(path.Base(p.prefix), "/")
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}

	ch := s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
		Prefix:    base,
		Recursive: false,
	})

	folders := make(map[string]struct{})

	for info := range ch {
		name := strings.TrimPrefix(info.Key, base)
		if strings.HasSuffix(name, "/") {
			name = strings.TrimSuffix(name, "/")
			folders[name] = struct{}{}
		}
	}

	fmt.Println(base, p.prefix, folders)
	if _, ok := folders[p.prefix]; !ok {
		return fmt.Errorf("folder %s not exists", p.prefix)
	}

	s.prefix = path.Join(base, p.prefix)

	return nil
}

type PwdParam struct {
	framework.ParamBase `use:"pwd" desc:"print current working directory path for minio"`
}

func (s *MinioState) PwdCommand(ctx context.Context, p *PwdParam) error {
	fmt.Println(s.getBase())
	return nil
}
