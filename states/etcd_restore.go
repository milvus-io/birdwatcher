package states

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/congqixia/birdwatcher/models"
	"github.com/congqixia/birdwatcher/proto/v2.0/commonpb"
	"github.com/golang/protobuf/proto"
	"github.com/gosuri/uilive"
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

	lb := make([]byte, 8)
	rd := bufio.NewReader(r)
	lenRead, err := rd.Read(lb)
	if err == io.EOF || lenRead < 8 {
		fmt.Println("File does not contains valid header")
	}

	nextBytes := binary.LittleEndian.Uint64(lb)
	headerBs := make([]byte, nextBytes)

	lenRead, err = rd.Read(headerBs)
	if err != nil {
		fmt.Println("failed to read header", err.Error())
		return nil
	}

	if lenRead != int(nextBytes) {
		fmt.Println("not enough bytes for header")
		return nil
	}

	header := &models.BackupHeader{}
	err = proto.Unmarshal(headerBs, header)
	if err != nil {
		fmt.Println("cannot parse backup header", err.Error())
		return err
	}
	fmt.Printf("header: %#v\n", header)

	switch header.Version {
	case 1:
		return restoreFromV1File(cli, rd, header)
	default:
		fmt.Printf("backup version %d not supported\n", header.Version)
		return fmt.Errorf("backup version %d not supported", header.Version)
	}
}

func restoreFromV1File(cli *clientv3.Client, rd io.Reader, header *models.BackupHeader) error {
	var nextBytes uint64
	var bs []byte

	lb := make([]byte, 8)
	i := 0
	progressDisplay := uilive.New()
	progressFmt := "Restoring backup ... %d%%(%d/%d)\n"

	progressDisplay.Start()
	fmt.Fprintf(progressDisplay, progressFmt, 0, 0, header.Entries)
	defer progressDisplay.Stop()

	for {

		bsRead, err := io.ReadFull(rd, lb) //rd.Read(lb)
		// all file read
		if err == io.EOF {
			return nil
		}
		if err != nil {
			fmt.Println("failed to read file:", err.Error())
			return err
		}
		if bsRead < 8 {
			fmt.Printf("fail to read next length %d instead of 8 read\n", bsRead)
			return errors.New("invalid file format")
		}

		nextBytes = binary.LittleEndian.Uint64(lb)
		bs = make([]byte, nextBytes)

		// cannot use rd.Read(bs), since proto marshal may generate a stopper
		bsRead, err = io.ReadFull(rd, bs)
		if err != nil {
			fmt.Println("failed to read next kv data", err.Error())
			return err
		}
		if uint64(bsRead) != nextBytes {
			fmt.Printf("bytesRead(%d)is not equal to nextBytes(%d)\n", bsRead, nextBytes)
			return errors.New("bad file format")
		}

		entry := &commonpb.KeyDataPair{}
		err = proto.Unmarshal(bs, entry)
		if err != nil {
			//Skip for now
			fmt.Printf("fail to parse line: %s, skip for now\n", err.Error())
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		_, err = cli.Put(ctx, entry.Key, string(entry.Data))
		if err != nil {
			fmt.Println("failed save kv into etcd, ", err.Error())
			return err
		}
		i++
		progress := i * 100 / int(header.Entries)

		fmt.Fprintf(progressDisplay, progressFmt, progress, i, header.Entries)

	}
}
