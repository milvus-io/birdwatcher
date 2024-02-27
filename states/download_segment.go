package states

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/manifoldco/promptui"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/indexpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/spf13/cobra"
)

func getDownloadSegmentCmd(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "download-segment",
		Short: "download segment file with provided segment id",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				fmt.Println("no segment id provided")
				return
			}

			segSet := make(map[int64]struct{})
			for _, arg := range args {
				id, err := strconv.ParseInt(arg, 10, 64)
				if err == nil {
					//skip bad segment id for now
					segSet[id] = struct{}{}
				}
			}

			segments, err := common.ListSegments(cli, basePath, func(info *datapb.SegmentInfo) bool {
				_, ok := segSet[info.ID]
				return ok
			})
			if err != nil {
				fmt.Println("failed to list segment info", err.Error())
				return
			}

			minioClient, bucketName, err := getMinioAccess()
			if err != nil {
				fmt.Println("failed to get minio access", err.Error())
				return
			}

			folder := fmt.Sprintf("dlsegment_%s", time.Now().Format("20060102150406"))
			for _, segment := range segments {
				common.FillFieldsIfV2(cli, basePath, segment)
				downloadSegment(minioClient, bucketName, segment, nil, folder)
			}

		},
	}

	return cmd
}

func getMinioWithInfo(addr string, ak, sk string, bucketName string, secure bool) (*minio.Client, string, error) {
	cred := credentials.NewStaticV4(ak, sk, "")
	minioClient, err := minio.New(addr, &minio.Options{
		Creds:  cred,
		Secure: secure,
	})
	if err != nil {
		return nil, "", err
	}
	exists, err := minioClient.BucketExists(context.Background(), bucketName)
	if !exists {
		fmt.Printf("bucket %s not exists\n", bucketName)
		return nil, "", err
	}

	if !exists {
		fmt.Printf("Bucket not exist\n")
		return nil, "", errors.New("bucket not exists")
	}

	return minioClient, bucketName, nil
}

func getMinioWithIAM(addr string, bucketName string, secure bool) (*minio.Client, string, error) {
	cred := credentials.NewIAM("")
	minioClient, err := minio.New(addr, &minio.Options{
		Creds:  cred,
		Secure: secure,
	})
	if err != nil {
		return nil, "", err
	}
	exists, err := minioClient.BucketExists(context.Background(), bucketName)
	if !exists {
		fmt.Printf("bucket %s not exists\n", bucketName)
		return nil, "", err
	}

	if !exists {
		fmt.Printf("Bucket not exist\n")
		return nil, "", errors.New("bucket not exists")
	}

	return minioClient, bucketName, nil
}

func getMinioAccess() (*minio.Client, string, error) {
	p := promptui.Prompt{
		Label: "BucketName",
	}
	bucketName, err := p.Run()
	if err != nil {
		return nil, "", err
	}

	minioClient, err := getMinioClient()
	if err != nil {
		fmt.Println("cannot get minio client", err.Error())
		return nil, "", err

	}
	exists, err := minioClient.BucketExists(context.Background(), bucketName)
	if !exists {
		fmt.Printf("bucket %s not exists\n", bucketName)
		return nil, "", err
	}

	if !exists {
		fmt.Printf("Bucket not exist\n")
		return nil, "", errors.New("bucket not exists")
	}

	return minioClient, bucketName, nil
}

func downloadSegment(cli *minio.Client, bucketName string, segment *datapb.SegmentInfo, indexMeta *indexpb.IndexMeta, folderPath string) error {

	p := path.Join(folderPath, fmt.Sprintf("%d", segment.ID))
	if _, err := os.Stat(p); errors.Is(err, os.ErrNotExist) {
		err := os.MkdirAll(p, os.ModePerm)
		if err != nil {
			fmt.Println("Failed to create folder,", err.Error())
			return err
		}
	}

	fmt.Printf("Downloading Segment: %d ...\n", segment.ID)

	for _, fieldBinlog := range segment.Binlogs {
		folder := fmt.Sprintf("%s/%d", p, fieldBinlog.FieldID)
		err := os.MkdirAll(folder, 0777)
		if err != nil {
			fmt.Println("Failed to create sub-folder", err.Error())
			return err
		}

		for _, binlog := range fieldBinlog.Binlogs {
			obj, err := cli.GetObject(context.Background(), bucketName, binlog.GetLogPath(), minio.GetObjectOptions{})
			if err != nil {
				fmt.Printf("failed to download file bucket=\"%s\", filePath = \"%s\", err: %s\n", bucketName, binlog.GetLogPath(), err.Error())
				return err
			}

			name := path.Base(binlog.GetLogPath())

			f, err := os.Create(path.Join(folder, name))
			if err != nil {
				fmt.Println("failed to open file")
				return err
			}
			w := bufio.NewWriter(f)
			r := bufio.NewReader(obj)
			io.Copy(w, r)
		}
	}

	if indexMeta != nil {
		fmt.Println("downloading index files ...")
		folder := path.Join(p, "index")
		for _, indexFile := range indexMeta.GetIndexFilePaths() {
			obj, err := cli.GetObject(context.Background(), bucketName, indexFile, minio.GetObjectOptions{})
			if err != nil {
				fmt.Println("failed to download file", bucketName, indexFile)
				//index not affect segment download result
				continue
			}

			name := path.Base(indexFile)
			f, err := os.Create(path.Join(folder, name))
			if err != nil {
				fmt.Println("failed to create index file")
				continue
			}
			w := bufio.NewWriter(f)
			r := bufio.NewReader(obj)
			io.Copy(w, r)
		}
	}
	return nil
}
