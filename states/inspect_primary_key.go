package states

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/minio/minio-go/v7"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/oss"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/storage"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type InspectPKParam struct {
	framework.ParamBase `use:"inspect-pk" desc:"inspect pk column remote or local"`
	Remote              bool   `name:"remote" default:"false" desc:"inspect remote pk"`
	LocalPath           string `name:"localPath" default:"" desc:"local pk file path"`
	// remote related params
	CollectionID int64  `name:"collection" default:"0" desc:"collection id to inspect"`
	SegmentID    int64  `name:"segment" default:"0" desc:"segment id to inspect"`
	MinioAddress string `name:"minioAddr" default:"" desc:"the minio address to override, leave empty to use milvus.yaml value"`

	ResultLimit int64 `name:"resultLimit" default:"10" desc:"Dedup result print limit, default 10"`
}

func (s *InstanceState) InspectPKCommand(ctx context.Context, p *InspectPKParam) error {
	var intResult map[int64]int
	var strResult map[string]int
	var err error
	if p.Remote {
		intResult, strResult, err = s.inspectRemote(ctx, p)
	} else {
		intResult, strResult, err = s.inspectLocal(ctx, p)
	}

	if err != nil {
		return err
	}

	total := len(intResult) + len(strResult)
	if total == 0 {
		fmt.Println("no duplicated pk found")
		return nil
	}
	var i int64
	for pk, cnt := range intResult {
		if i > p.ResultLimit {
			break
		}
		fmt.Printf("PK %d duplicated %d times\n", pk, cnt+1)
		i++
	}
	for pk, cnt := range strResult {
		if i > p.ResultLimit {
			break
		}
		fmt.Printf("PK %s duplicated %d times\n", pk, cnt+1)
		i++
	}
	if i < int64(total) {
		fmt.Println("...")
		fmt.Printf("%d total duplicated pk value found\n", total)
	}

	return nil
}

func (s *InstanceState) DedupScanner() (map[int64]int, map[string]int, func(int64), func(string)) {
	intIDs := make(map[int64]struct{})
	strIDs := make(map[string]struct{})
	intResult := make(map[int64]int)
	strResult := make(map[string]int)
	return intResult, strResult,
		func(v int64) {
			_, ok := intIDs[v]
			if ok {
				intResult[v]++
			}
			intIDs[v] = struct{}{}
		}, func(v string) {
			_, ok := strIDs[v]
			if ok {
				strResult[v]++
			}
			strIDs[v] = struct{}{}
		}
}

func (s *InstanceState) inspectRemote(ctx context.Context, p *InspectPKParam) (map[int64]int, map[string]int, error) {
	fmt.Println("Using remote inspect mode")
	if p.CollectionID == 0 {
		return nil, nil, errors.New("collection id not provided")
	}

	collection, err := common.GetCollectionByIDVersion(ctx, s.client, s.basePath, p.CollectionID)
	if err != nil {
		return nil, nil, err
	}
	pkField, ok := collection.GetPKField()
	if !ok {
		return nil, nil, errors.New("pk field not found")
	}

	segments, err := common.ListSegments(ctx, s.client, s.basePath, func(s *models.Segment) bool {
		return p.SegmentID == 0 || p.SegmentID == s.ID
	})
	if err != nil {
		return nil, nil, err
	}

	params := []oss.MinioConnectParam{}
	if p.MinioAddress != "" {
		params = append(params, oss.WithMinioAddr(p.MinioAddress))
	}

	minioClient, bucketName, rootPath, err := s.GetMinioClientFromCfg(ctx, params...)
	if err != nil {
		fmt.Println("Failed to create folder,", err.Error())
	}

	intResult, strResult, is, ss := s.DedupScanner()

	count := 0
	for _, segment := range segments {
		for _, fieldBinlog := range segment.GetBinlogs() {
			if fieldBinlog.FieldID != pkField.FieldID {
				continue
			}

			for _, binlog := range fieldBinlog.Binlogs {
				logPath := strings.ReplaceAll(binlog.LogPath, "ROOT_PATH", rootPath)
				log.Println("start to scan pk binlog: ", logPath)
				obj, err := minioClient.GetObject(ctx, bucketName, logPath, minio.GetObjectOptions{})
				if err != nil {
					fmt.Println("failed to open file:", bucketName, logPath)
					return nil, nil, err
				}

				s.scanPKBinlogFile(obj, is, ss)
				obj.Close()

				count++
			}
		}
	}

	return intResult, strResult, nil
}

func (s *InstanceState) inspectLocal(ctx context.Context, p *InspectPKParam) (map[int64]int, map[string]int, error) {
	fmt.Println("Using local inspect mode, localPath: ", p.LocalPath)
	intResult, strResult, is, ss := s.DedupScanner()
	err := filepath.WalkDir(p.LocalPath, func(path string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}
		fmt.Println("Start parsing binlog file: ", path)
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		var cnt int
		s.scanPKBinlogFile(f, is, ss)

		fmt.Printf("%d values scanned\n", cnt)

		return nil
	})
	return intResult, strResult, err
}

func (s *InstanceState) scanPKBinlogFile(f storage.ReadSeeker, scanInt func(v int64), scanVarchar func(v string)) error {
	r, desc, err := storage.NewBinlogReader(f)
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	switch desc.PayloadDataType {
	case schemapb.DataType_Int64:
		values, err := r.NextInt64EventReader()
		if err != nil {
			return err
		}
		for _, v := range values {
			scanInt(v)
		}
	case schemapb.DataType_VarChar:
		values, err := r.NextVarcharEventReader()
		if err != nil {
			return err
		}
		for _, v := range values {
			scanVarchar(v)
		}
	}
	return nil
}
