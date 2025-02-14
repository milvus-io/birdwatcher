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
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/gosuri/uilive"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/states/kv"
)

func restoreFromV1File(cli kv.MetaKV, rd io.Reader, header *models.BackupHeader) error {
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
		bsRead, err := io.ReadFull(rd, lb) // rd.Read(lb)
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
			// Skip for now
			fmt.Printf("fail to parse line: %s, skip for now\n", err.Error())
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		err = cli.Save(ctx, entry.Key, string(entry.Data))
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
			// TODO check EOF
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
			// testRestoreConfigurations(rd, ph)
		case int32(models.AppMetrics):
			// testRestoreConfigurations(rd, ph)
		}
	}
}

func restoreEtcdFromBackV2(cli kv.MetaKV, rd io.Reader, ph models.PartHeader) (string, error) {
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

	batchNum := 10
	ch := make(chan []*commonpb.KeyDataPair, 10)
	errCh := make(chan error, 1)

	go func() {
		defer close(ch)
		batch := make([]*commonpb.KeyDataPair, 0, batchNum)
		defer func() {
			if len(batch) > 0 {
				ch <- batch
			}
		}()
		var lastPrint time.Time
		for {
			bsRead, err := io.ReadFull(rd, lb) // rd.Read(lb)
			// all file read
			if err == io.EOF {
				// return meta["instance"], nil
				errCh <- nil
				return
			}
			if err != nil {
				fmt.Println("failed to read file:", err.Error())
				errCh <- err
				return
			}
			if bsRead < 8 {
				fmt.Printf("fail to read next length %d instead of 8 read\n", bsRead)
				errCh <- errors.New("invalid file format")
				return
			}

			nextBytes = binary.LittleEndian.Uint64(lb)
			// stopper found
			if nextBytes == 0 {
				errCh <- nil
				return
			}
			bs = make([]byte, nextBytes)

			// cannot use rd.Read(bs), since proto marshal may generate a stopper
			bsRead, err = io.ReadFull(rd, bs)
			if err != nil {
				fmt.Println("failed to read next kv data", err.Error())
				errCh <- err
				return
			}
			if uint64(bsRead) != nextBytes {
				fmt.Printf("bytesRead(%d)is not equal to nextBytes(%d)\n", bsRead, nextBytes)
				errCh <- errors.New("bad file format")
				return
			}

			entry := &commonpb.KeyDataPair{}
			err = proto.Unmarshal(bs, entry)
			if err != nil {
				// Skip for now
				fmt.Printf("fail to parse line: %s, skip for now\n", err.Error())
				continue
			}

			batch = append(batch, entry)
			if len(batch) >= batchNum {
				ch <- batch
				batch = make([]*commonpb.KeyDataPair, 0, batchNum)
			}
			i++
			progress := i * 100 / int(cnt)

			if time.Since(lastPrint) > time.Millisecond*10 || progress == 100 {
				fmt.Fprintf(progressDisplay, progressFmt, progress, i, cnt)
				lastPrint = time.Now()
			}
		}
	}()

	var wg sync.WaitGroup
	workerNum := 3
	wg.Add(workerNum)
	for i := 0; i < workerNum; i++ {
		go func() {
			defer wg.Done()
			for batch := range ch {
				keys := make([]string, 0, len(batch))
				values := make([]string, 0, len(batch))
				for _, entry := range batch {
					keys = append(keys, entry.Key)
					values = append(values, string(entry.Data))
				}
				func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
					defer cancel()

					err = cli.MultiSave(ctx, keys, values)
					// _, err := cli.Txn(ctx).If().Then(ops...).Commit()
					if err != nil {
						fmt.Println(err.Error())
					}
				}()
			}
		}()
	}

	err = <-errCh
	wg.Wait()
	if err != nil {
		return "", err
	}

	return meta["instance"], nil
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
