package storage

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/milvus-io/birdwatcher/framework"
)

type MinioState struct {
	*framework.CmdState

	client *minio.Client
	bucket string
	prefix string
}

func (s *MinioState) SetupCommands() {
	cmd := s.GetCmd()

	s.MergeFunctionCommands(cmd, s)
	s.UpdateState(cmd, s, s.SetupCommands)
}

type ConnectMinioParam struct {
	framework.ParamBase `use:"connect-minio" desc:"connect to minio instance"`
	Bucket              string `name:"bucket" default:"" desc:"bucket name"`
	Address             string `name:"address" default:"127.0.0.1:9000" desc:"minio address to connect"`
	UseIAM              bool   `name:"iam" default:"false" desc:"use IAM mode"`
	IAMEndpoint         string `name:"iamEndpoint" default:"" desc:"IAM endpoint address"`
	AK                  string `name:"ak" default:"" desc:"access key/username"`
	SK                  string `name:"sk" default:"" desc:"secret key/password"`
	UseSSL              bool   `name:"ssl" default:"" desc:"use SSL"`
}

func ConnectMinio(ctx context.Context, p *ConnectMinioParam, parent *framework.CmdState) (*MinioState, error) {
	var cred *credentials.Credentials
	if p.UseIAM {
		cred = credentials.NewIAM(p.IAMEndpoint)
	} else {
		cred = credentials.NewStaticV4(p.AK, p.SK, "")
	}

	minioClient, err := minio.New(p.Address, &minio.Options{
		Creds:  cred,
		Secure: p.UseSSL,
	})
	if err != nil {
		return nil, err
	}

	exists, err := minioClient.BucketExists(ctx, p.Bucket)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("bucket %s not exists", p.Bucket)
	}

	return &MinioState{
		client:   minioClient,
		bucket:   p.Bucket,
		CmdState: parent.Spawn("Minio(Addr)"),
	}, nil
}
