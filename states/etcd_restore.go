package states

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"os"

	"github.com/congqixia/birdwatcher/models"
	"github.com/congqixia/birdwatcher/proto/v2.0/commonpb"
	"github.com/golang/protobuf/proto"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// restoreEtcd write back backup file content to etcd
// need backup first before calling this function
// basePath is needed for key indication
func restoreEtcd(cli *clientv3.Client, filePath string, basePath string) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	r, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer r.Close()

	scanner := bufio.NewScanner(r)

	if !scanner.Scan() {
		fmt.Println("backup file empty")
		return nil
	}
	header := &models.BackupHeader{}
	err = proto.Unmarshal(scanner.Bytes(), header)
	if err != nil {
		fmt.Println("cannot parse backup header", err.Error())
		return err
	}

	switch header.Version {
	case 1:
		return restoreFromV1File(cli, scanner)
	default:
		fmt.Printf("Backup version %d not supported\n", header.Version)
		return fmt.Errorf("Backup version %d not supported\n", header.Version)
	}

	return nil
}

func restoreFromV1File(cli *clientv3.Client, scanner *bufio.Scanner) error {
	for scanner.Scan() {
		entry := &commonpb.KeyDataPair{}

		err := proto.Unmarshal(scanner.Bytes(), entry)
		if err != nil {
			//Skip for now
			fmt.Println("fail to parse line: %s, skip for now", err.Error())
			continue
		}

		_, err = cli.Put(context.Background(), entry.Key, string(entry.Data))
		if err != nil {
			fmt.Println("failed save kv into etcd, ", err.Error())
			return err
		}
	}
	return nil
}
