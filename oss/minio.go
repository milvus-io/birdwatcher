package oss

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinioClientParam struct {
	Addr          string
	Port          string
	AK            string
	SK            string
	UseIAM        bool
	IAMEndpoint   string
	UseSSL        bool
	CloudProvider string
	Region        string

	BucketName string
	RootPath   string
}

// MinioClient wraps minio client, bucket info within
type MinioClient struct {
	Client     *minio.Client
	BucketName string
	RootPath   string
}

func NewMinioClient(ctx context.Context, p MinioClientParam) (*MinioClient, error) {
	opts := &minio.Options{
		Secure:       p.UseSSL,
		BucketLookup: minio.BucketLookupAuto,
	}
	endpoint := fmt.Sprintf("%s:%s", p.Addr, p.Port)

	switch p.CloudProvider {
	case "aws":
		processMinioAwsOptions(p, opts)
	case "gcp":
		// adhoc to remove port of gcs address to let minio-go know it's gcs
		if strings.Contains(endpoint, GcsDefaultAddress) {
			endpoint = GcsDefaultAddress
		}
		processMinioGcpOptions(p, opts)
	default:
		return nil, errors.Newf("Cloud provider %s not supported yet", p.CloudProvider)
	}
	client, err := minio.New(endpoint, opts)
	if err != nil {
		return nil, err
	}

	ok, err := client.BucketExists(ctx, p.BucketName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.Newf("Bucket %s not exists", p.BucketName)
	}

	return &MinioClient{
		Client:     client,
		BucketName: p.BucketName,
		RootPath:   p.RootPath,
	}, nil
}

func processMinioAwsOptions(p MinioClientParam, opts *minio.Options) {
	if p.UseIAM {
		opts.Creds = credentials.NewIAM(p.IAMEndpoint)
	} else {
		opts.Creds = credentials.NewStaticV4(p.AK, p.SK, "")
	}
}
