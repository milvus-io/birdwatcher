package states

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/gosuri/uilive"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

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

func restoreV2File(rd *bufio.Reader, state *embedEtcdMockState) error {
	var err error
	for {
		var ph models.PartHeader
		err = readFixLengthHeader(rd, &ph)
		if err != nil {
			//TODO check EOF
			return nil
		}

		switch ph.PartType {
		case int32(models.EtcdBackup):
			instance, err := restoreEtcdFromBackV2(state.client, rd, ph)
			if err != nil {
				fmt.Println("failed to restore etcd from backup file", err.Error())
				return err
			}
			state.SetInstance(instance)
		case int32(models.MetricsBackup):
			restoreMetrics(rd, ph, func(session *models.Session, metrics, defaultMetrics []byte) {
				state.metrics[fmt.Sprintf("%s-%d", session.ServerName, session.ServerID)] = metrics
				state.defaultMetrics[fmt.Sprintf("%s-%d", session.ServerName, session.ServerID)] = defaultMetrics
			})
		case int32(models.Configurations):
			testRestoreConfigurations(rd, ph)
		case int32(models.AppMetrics):
			testRestoreConfigurations(rd, ph)
		}
	}
}

func restoreEtcdFromBackV2(cli *clientv3.Client, rd io.Reader, ph models.PartHeader) (string, error) {
	meta := make(map[string]string)
	err := json.Unmarshal(ph.Extra, &meta)
	if err != nil {
		return "", err
	}

	cnt, err := strconv.ParseInt(meta["cnt"], 10, 64)
	if err != nil {
		return "", err
	}

	var nextBytes uint64
	var bs []byte

	lb := make([]byte, 8)
	i := 0
	progressDisplay := uilive.New()
	progressFmt := "Restoring backup ... %d%%(%d/%d)\n"

	progressDisplay.Start()
	fmt.Fprintf(progressDisplay, progressFmt, 0, 0, cnt)
	defer progressDisplay.Stop()

	for {

		bsRead, err := io.ReadFull(rd, lb) //rd.Read(lb)
		// all file read
		if err == io.EOF {
			return meta["instance"], nil
		}
		if err != nil {
			fmt.Println("failed to read file:", err.Error())
			return "", err
		}
		if bsRead < 8 {
			fmt.Printf("fail to read next length %d instead of 8 read\n", bsRead)
			return "", errors.New("invalid file format")
		}

		nextBytes = binary.LittleEndian.Uint64(lb)
		// stopper found
		if nextBytes == 0 {
			return meta["instance"], nil
		}
		bs = make([]byte, nextBytes)

		// cannot use rd.Read(bs), since proto marshal may generate a stopper
		bsRead, err = io.ReadFull(rd, bs)
		if err != nil {
			fmt.Println("failed to read next kv data", err.Error())
			return "", err
		}
		if uint64(bsRead) != nextBytes {
			fmt.Printf("bytesRead(%d)is not equal to nextBytes(%d)\n", bsRead, nextBytes)
			return "", errors.New("bad file format")
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
			return "", err
		}
		i++
		progress := i * 100 / int(cnt)

		fmt.Fprintf(progressDisplay, progressFmt, progress, i, cnt)
	}

}

func restoreMetrics(rd io.Reader, ph models.PartHeader, handler func(session *models.Session, metrics, defaultMetrics []byte)) error {
	for {
		bs, nb, err := readBackupBytes(rd)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		// stopper
		if nb == 0 {
			return nil
		}

		// session
		session := &models.Session{}
		err = json.Unmarshal(bs, session)
		if err != nil {
			return err
		}

		mbs, _, err := readBackupBytes(rd)
		if err != nil {
			return err
		}

		dmbs, _, err := readBackupBytes(rd)
		if err != nil {
			return err
		}
		handler(session, mbs, dmbs)
	}
}

func testRestoreMetrics(rd io.Reader, ph models.PartHeader) error {
	for {
		bs, nb, err := readBackupBytes(rd)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		// stopper
		if nb == 0 {
			return nil
		}

		// session
		fmt.Println(string(bs))

		bs, _, err = readBackupBytes(rd)
		if err != nil {
			return err
		}

		fmt.Println("metrics len:", len(bs))
		bs, _, err = readBackupBytes(rd)
		if err != nil {
			return err
		}
		fmt.Println("default metrics len:", len(bs))
	}
}

func testRestoreConfigurations(rd io.Reader, ph models.PartHeader) error {
	for {
		bs, nb, err := readBackupBytes(rd)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		// stopper
		if nb == 0 {
			return nil
		}

		// session
		fmt.Println(string(bs))

		bs, _, err = readBackupBytes(rd)
		if err != nil {
			return err
		}

		fmt.Println("configuration len:", len(bs))

	}
}

func testRestoreAppMetrics(rd io.Reader, ph models.PartHeader) error {
	for {
		bs, nb, err := readBackupBytes(rd)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		// stopper
		if nb == 0 {
			return nil
		}

		// session
		fmt.Println(string(bs))

		bs, _, err = readBackupBytes(rd)
		if err != nil {
			return err
		}

		fmt.Println("app metrics len:", len(bs))
	}
}
