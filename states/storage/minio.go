package storage

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/autocomplete"
)

type OSSState struct {
	*framework.CmdState

	connectParam oss.MinioClientParam
	client       *oss.MinioClient
	bucket       string
	prefix       string
}

const minioPathSuggester = "minio-path"

func (s *OSSState) SetupCommands() {
	cmd := s.GetCmd()

	s.MergeFunctionCommands(cmd, s)
	s.UpdateState(cmd, s, s.SetupCommands)

	autocomplete.RegisterValueSuggester(minioPathSuggester, autocomplete.ValueSuggestFunc(func(partial string) []string {
		return s.suggestPaths(partial)
	}))
}

// Label overrides default cmd label behavior
// returning OSS[PROVIDER](BUCKET/CURR_DIR)
func (s *OSSState) Label() string {
	return fmt.Sprintf("OSS[%s](%s/%s)", s.connectParam.CloudProvider, s.connectParam.BucketName, s.prefix)
}

func (s *OSSState) Close() {
	autocomplete.UnregisterValueSuggester(minioPathSuggester)
}

func (s *OSSState) suggestPaths(partial string) []string {
	var listPrefix string
	var filterPart string
	var resultPrefix string

	if partial == "" || partial == "/" {
		// List current directory (or root for "/")
		if partial == "/" {
			listPrefix = ""
			resultPrefix = "/"
		} else {
			listPrefix = s.getBase()
			resultPrefix = ""
		}
		filterPart = ""
	} else if strings.HasSuffix(partial, "/") {
		// "subdir/" → list inside that directory
		resolved := s.resolvePath(partial)
		listPrefix = toListingPrefix(resolved)
		resultPrefix = partial
		filterPart = ""
	} else {
		// "sub" or "dir/sub" → list parent, filter by base
		dir := path.Dir(partial)
		filterPart = path.Base(partial)
		if dir == "." {
			listPrefix = s.getBase()
			resultPrefix = ""
		} else {
			resolved := s.resolvePath(dir)
			listPrefix = toListingPrefix(resolved)
			resultPrefix = dir + "/"
		}
	}

	ctx, cancel := s.Ctx()
	defer cancel()

	ch := s.client.Client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{
		Prefix:    listPrefix,
		Recursive: false,
	})

	var results []string
	for info := range ch {
		if info.Err != nil {
			continue
		}
		name := strings.TrimPrefix(info.Key, listPrefix)
		name = strings.TrimSuffix(name, "/")
		if filterPart == "" || strings.HasPrefix(name, filterPart) {
			results = append(results, resultPrefix+name)
		}
	}
	return results
}

type ConnectOSSParam struct {
	framework.ParamBase `use:"connect oss" desc:"connect to OSS instance using underling minio client"`
	Bucket              string `name:"bucket" default:"" desc:"bucket name"`
	Address             string `name:"address" default:"127.0.0.1" desc:"minio address to connect"`
	Port                string `name:"port" default:"9000"`
	CloudProvider       string `name:"cloudProvider" default:"aws"`
	Region              string `name:"region" default:""`
	RootPath            string `name:"rootPath" default:""`
	UseIAM              bool   `name:"iam" default:"false" desc:"use IAM mode"`
	IAMEndpoint         string `name:"iamEndpoint" default:"" desc:"IAM endpoint address"`
	AK                  string `name:"ak" default:"" desc:"access key/username"`
	SK                  string `name:"sk" default:"" desc:"secret key/password"`
	UseSSL              bool   `name:"ssl" default:"" desc:"use SSL"`
	SkipBucketCheck     bool   `name:"skipBucketCheck" default:"true"`
}

func ConnectOSS(ctx context.Context, p *ConnectOSSParam, parent *framework.CmdState) (*OSSState, error) {
	mp := oss.MinioClientParam{
		CloudProvider: p.CloudProvider,
		Region:        p.Region,
		Addr:          p.Address,
		Port:          p.Port,
		AK:            p.AK,
		SK:            p.SK,

		BucketName: p.Bucket,
		RootPath:   p.RootPath,

		UseIAM: p.UseIAM,
		UseSSL: p.UseSSL,
	}
	if p.SkipBucketCheck {
		oss.WithSkipCheckBucket(true)(&mp)
	}

	mClient, err := oss.NewMinioClient(ctx, mp)
	if err != nil {
		return nil, err
	}

	return &OSSState{
		client:       mClient,
		bucket:       p.Bucket,
		connectParam: mp,
		prefix:       mp.RootPath, // set current dir to rootPath if provided
		CmdState:     parent.Spawn(fmt.Sprintf("OSS[%s](%s/%s)", p.CloudProvider, p.Bucket, p.RootPath)),
	}, nil
}
