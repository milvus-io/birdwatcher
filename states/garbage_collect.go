package states

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/manifoldco/promptui"
	"github.com/minio/minio-go/v7"
	"github.com/spf13/cobra"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

func getGarbageCollectCmd(cli kv.MetaKV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "garbage-collect",
		Short: "scan oss of milvus instance for garbage(dry-run)",
		Run: func(cmd *cobra.Command, args []string) {
			p := promptui.Prompt{
				Label: "BucketName",
			}
			bucketName, err := p.Run()
			if err != nil {
				fmt.Println("failed to get bucketName:", err.Error())
				return
			}
			p.Label = "Minio Root Path"
			minioRootPath, err := p.Run()
			if err != nil {
				fmt.Println("failed to get minioRootPath:", err.Error())
				return
			}

			minioClient, err := getMinioClient()
			if err != nil {
				fmt.Println("failed to get minio client:", err.Error())
				return
			}

			exists, _ := minioClient.BucketExists(context.Background(), bucketName)
			if !exists {
				fmt.Printf("bucket %s not exists\n", bucketName)
				return
			}
			garbageCollect(cli, basePath, minioClient, minioRootPath, bucketName)
		},
	}

	return cmd
}

const (
	// TODO silverxia change to configuration
	insertLogPrefix = `insert_log`
	statsLogPrefix  = `stats_log`
	deltaLogPrefix  = `delta_log`
)

func garbageCollect(cli kv.MetaKV, basePath string, minioClient *minio.Client, minioRootPath string, bucketName string) {
	segments, err := common.ListSegments(context.TODO(), cli, basePath, func(*models.Segment) bool { return true })
	if err != nil {
		fmt.Println("failed to list segments:", err.Error())
		return
	}

	idSegments := make(map[int64]*datapb.SegmentInfo)
	binlog := make(map[string]struct{})
	deltalog := make(map[string]struct{})
	statslog := make(map[string]struct{})

	for _, info := range segments {
		segment := info.SegmentInfo
		common.FillFieldsIfV2(cli, basePath, segment)
		idSegments[segment.ID] = segment

		if !isSegmentHealthy(segment) {
			continue
		}
		for _, logs := range segment.GetBinlogs() {
			for _, l := range logs.GetBinlogs() {
				binlog[l.GetLogPath()] = struct{}{}
			}
		}

		for _, statsLogs := range segment.GetStatslogs() {
			for _, l := range statsLogs.GetBinlogs() {
				statslog[l.GetLogPath()] = struct{}{}
			}
		}

		for _, deltaLogs := range segment.GetDeltalogs() {
			for _, l := range deltaLogs.GetBinlogs() {
				deltalog[l.GetLogPath()] = struct{}{}
			}
		}
	}

	// walk only data cluster related prefixes
	prefixes := make([]string, 0, 3)
	prefixes = append(prefixes, path.Join(minioRootPath, insertLogPrefix))
	prefixes = append(prefixes, path.Join(minioRootPath, statsLogPrefix))
	prefixes = append(prefixes, path.Join(minioRootPath, deltaLogPrefix))

	logs := make([]map[string]struct{}, 0, 3)
	logs = append(logs, binlog, statslog, deltalog)

	fileName := fmt.Sprintf("gc_report_%s", time.Now().Format("2006-01-02T15:04:05"))

	logOutput, err := os.Create(fileName)
	if err != nil {
		fmt.Println("failed to create file report file", err.Error())
	}
	defer logOutput.Close()
	w := bufio.NewWriter(logOutput)

	counter := 0
	for idx, prefix := range prefixes {
		for info := range minioClient.ListObjects(context.TODO(), bucketName, minio.ListObjectsOptions{
			Prefix:    prefix,
			Recursive: true,
		}) {
			fmt.Println(info.Key)
			counter++
			w.WriteString(fmt.Sprintf("Processing path: \"%s\"", info.Key))
			logsMap := logs[idx]
			_, has := logsMap[info.Key]
			if has {
				w.WriteString(", result: valid key\n")
				continue
			}

			segmentID, err := ParseSegmentIDByBinlog(minioRootPath, info.Key)
			if err != nil {
				w.WriteString(fmt.Sprintf(", failed to parse segmentID: %s\n", err.Error()))
				continue
			}

			segment, ok := idSegments[segmentID]
			if ok {
				if segment.GetState() == commonpb.SegmentState_Dropped {
					w.WriteString(fmt.Sprintf(", segment %d is dropped, waiting for gc\n", segmentID))
					continue
				}
			}

			w.WriteString(", not relate meta found, maybe garbage\n")
		}
	}
	w.Flush()
	fmt.Printf("%d entries processed, report file written to %s\n", counter, fileName)
}

// ParseSegmentIDByBinlog parse segment id from binlog paths
// if path format is not expected, returns error
func ParseSegmentIDByBinlog(rootPath, path string) (int64, error) {
	if !strings.HasPrefix(path, rootPath) {
		return 0, fmt.Errorf("path:\"%s\" does not contains rootPath:\"%s\"", path, rootPath)
	}

	// remove leading "/"
	p := path[len(rootPath):]
	for strings.HasPrefix(p, "/") {
		p = p[1:]
	}

	// binlog path should consist of "insertLog/collID/partID/segID/fieldID/fileName"
	keyStr := strings.Split(p, "/")
	if len(keyStr) != 6 {
		return 0, fmt.Errorf("%s is not a valid binlog path", p)
	}
	return strconv.ParseInt(keyStr[len(keyStr)-3], 10, 64)
}

func isSegmentHealthy(segment *datapb.SegmentInfo) bool {
	return segment != nil &&
		segment.GetState() != commonpb.SegmentState_SegmentStateNone &&
		segment.GetState() != commonpb.SegmentState_NotExist &&
		segment.GetState() != commonpb.SegmentState_Dropped
}
