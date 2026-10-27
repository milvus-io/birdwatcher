package oss

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOSSOverrides(t *testing.T) {
	original := MinioClientParam{UseIAM: true, UseSSL: true, AK: "old-ak", SK: "old-sk", Region: "original"}
	params, err := (OSSOverrideParam{}).ConnectParams()
	require.NoError(t, err)
	got := original
	for _, apply := range params {
		apply(&got)
	}
	require.Equal(t, original, got)
	p := OSSOverrideParam{UseIAM: "false", UseSSL: "false", AccessKeyID: "new-ak", CloudProvider: "aws", BucketName: "bucket", RootPath: "root", IAMEndpoint: "iam", LoadFrequency: "60"}
	params, err = p.ConnectParams()
	require.NoError(t, err)
	for _, apply := range params {
		apply(&got)
	}
	require.False(t, got.UseIAM)
	require.False(t, got.UseSSL)
	require.Equal(t, "new-ak", got.AK)
	require.Equal(t, "old-sk", got.SK)
	require.Equal(t, "original", got.Region)
	require.Equal(t, "aws", got.CloudProvider)
	require.Equal(t, "bucket", got.BucketName)
	require.Equal(t, "root", got.RootPath)
	require.Equal(t, "iam", got.IAMEndpoint)
	require.Equal(t, 60, got.LoadFrequency)
	for _, p := range []OSSOverrideParam{{UseIAM: "masked"}, {UseSSL: "bad"}, {LoadFrequency: "-1"}, {LoadFrequency: "masked"}} {
		_, err := p.ConnectParams()
		require.Error(t, err)
	}
}

func TestOSSStandaloneRequiresLocation(t *testing.T) {
	// A nil getStore ensures standalone never attempts configuration discovery.
	_, err := NewResolvedObjectStoreWithOverrides(context.Background(), OSSOverrideParam{OSSStandalone: true}, "", true, nil)
	require.ErrorContains(t, err, "requires --minioAddr")
}

func TestNewResolvedObjectStoreWithOverrides(t *testing.T) {
	var captured []MinioConnectParam
	getStore := func(_ context.Context, params ...MinioConnectParam) (*ResolvedObjectStore, error) {
		captured = params
		return &ResolvedObjectStore{}, nil
	}
	store, err := NewResolvedObjectStoreWithOverrides(context.Background(), OSSOverrideParam{UseIAM: "true", CloudProvider: "aws"}, "host:9000", true, getStore)
	require.NoError(t, err)
	require.NotNil(t, store)
	require.NotEmpty(t, captured)
	mp := MinioClientParam{}
	for _, apply := range captured {
		apply(&mp)
	}
	require.True(t, mp.UseIAM)
	require.Equal(t, "aws", mp.CloudProvider)
	require.Equal(t, "host:9000", mp.Addr)
	require.True(t, mp.skipCheckBucket)
}
