package common

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

const ()

var ErrReachMaxNumOfWalkSegment = errors.New("reach max number of the walked segments")

func ListSegments(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(*models.Segment) bool) ([]*models.Segment, error) {
	prefix := path.Join(basePath, DCPrefix, SegmentMetaPrefix) + "/"

	segments, err := ListObj2Models(ctx, cli, prefix, func(info *datapb.SegmentInfo, key string) *models.Segment {

		return models.NewSegment(info, key, getSegmentLazyFunc(cli, basePath, info))
	}, filters...)
	if err != nil {
		return nil, err
	}
	return segments, nil
}

// ListSegmentsVersion list segment info as specified version.
// func ListSegmentsVersion(ctx context.Context, cli kv.MetaKV, basePath string, version string, filters ...func(*models.Segment) bool) ([]*models.Segment, error) {
// 	prefix := path.Join(basePath, DCPrefix, SegmentMetaPrefix) + "/"
// 	switch version {
// 	case models.LTEVersion2_1:
// 		segments, keys, err := ListProtoObjects[datapb.SegmentInfo](ctx, cli, prefix)
// 		if err != nil {
// 			return nil, err
// 		}

// 		return lo.FilterMap(segments, func(segment datapb.SegmentInfo, idx int) (*models.Segment, bool) {
// 			s := models.NewSegmentFromV2_1(&segment, keys[idx])
// 			for _, filter := range filters {
// 				if !filter(s) {
// 					return nil, false
// 				}
// 			}
// 			return s, true
// 		}), nil
// 	case models.GTEVersion2_2:
// 		segments, keys, err := ListProtoObjects[datapb.SegmentInfo](ctx, cli, prefix)
// 		if err != nil {
// 			return nil, err
// 		}

// 		return lo.FilterMap(segments, func(segment datapb.SegmentInfo, idx int) (*models.Segment, bool) {
// 			s := models.NewSegmentFromV2_2(&segment, keys[idx], getSegmentLazyFunc(cli, basePath, segment))
// 			for _, filter := range filters {
// 				if !filter(s) {
// 					return nil, false
// 				}
// 			}
// 			return s, true
// 		}), nil
// 	default:
// 		return nil, fmt.Errorf("undefined version: %s", version)
// 	}
// }

func getSegmentLazyFunc(cli kv.MetaKV, basePath string, segment *datapb.SegmentInfo) func() ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
	return func() ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
		prefix := path.Join(basePath, "datacoord-meta", fmt.Sprintf("binlog/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID))

		f := func(pb func(segment *datapb.SegmentInfo, fieldID int64, logID int64) string) ([]*datapb.FieldBinlog, error) {
			fields, _, err := ListProtoObjects[datapb.FieldBinlog](context.Background(), cli, prefix)
			if err != nil {
				return nil, err
			}
			for _, field := range fields {
				for _, binlog := range field.GetBinlogs() {
					binlog.LogPath = pb(segment, field.GetFieldID(), binlog.GetLogID())
				}
			}
			return fields, err
		}

		binlogs, err := f(func(segment *datapb.SegmentInfo, fieldID int64, logID int64) string {
			return fmt.Sprintf("ROOT_PATH/insert_log/%d/%d/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID, fieldID, logID)
		})
		if err != nil {
			return nil, nil, nil, err
		}

		prefix = path.Join(basePath, "datacoord-meta", fmt.Sprintf("statslog/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID))
		statslogs, err := f(func(segment *datapb.SegmentInfo, fieldID int64, logID int64) string {
			return fmt.Sprintf("ROOT_PATH/stats_log/%d/%d/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID, fieldID, logID)
		})
		if err != nil {
			return nil, nil, nil, err
		}

		prefix = path.Join(basePath, "datacoord-meta", fmt.Sprintf("deltalog/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID))
		deltalogs, err := f(func(segment *datapb.SegmentInfo, fieldID int64, logID int64) string {
			return fmt.Sprintf("ROOT_PATH/delta_log/%d/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID, logID)
		})
		if err != nil {
			return nil, nil, nil, err
		}

		return binlogs, statslogs, deltalogs, nil
	}
}

// ListSegments list segment info from etcd
// func ListSegments(cli kv.MetaKV, basePath string, filter func(*datapb.SegmentInfo) bool) ([]*datapb.SegmentInfo, error) {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
// 	defer cancel()
// 	_, vals, err := cli.LoadWithPrefix(ctx, path.Join(basePath, SegmentMetaPrefix)+"/")
// 	if err != nil {
// 		return nil, err
// 	}
// 	segments := make([]*datapb.SegmentInfo, 0, len(vals))
// 	for _, val := range vals {
// 		info := &datapb.SegmentInfo{}
// 		err = proto.Unmarshal([]byte(val), info)
// 		if err != nil {
// 			continue
// 		}
// 		if filter == nil || filter(info) {
// 			segments = append(segments, info)
// 		}
// 	}

// 	sort.Slice(segments, func(i, j int) bool {
// 		return segments[i].GetID() < segments[j].GetID()
// 	})
// 	return segments, nil
// }

// FillFieldsIfV2 fill binlog paths fields for v2 segment info.
func FillFieldsIfV2(cli kv.MetaKV, basePath string, segment *datapb.SegmentInfo) error {
	if len(segment.Binlogs) == 0 {
		prefix := path.Join(basePath, "datacoord-meta", fmt.Sprintf("binlog/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID))
		fields, _, err := ListProtoObjects[datapb.FieldBinlog](context.Background(), cli, prefix)
		if err != nil {
			return err
		}

		segment.Binlogs = make([]*datapb.FieldBinlog, 0, len(fields))
		for _, field := range fields {
			f := &datapb.FieldBinlog{
				FieldID: field.FieldID,
				Binlogs: make([]*datapb.Binlog, 0, len(field.Binlogs)),
			}

			for _, binlog := range field.Binlogs {
				l := &datapb.Binlog{
					EntriesNum:    binlog.EntriesNum,
					TimestampFrom: binlog.TimestampFrom,
					TimestampTo:   binlog.TimestampTo,
					LogPath:       binlog.LogPath,
					LogSize:       binlog.LogSize,
				}
				if l.LogPath == "" {
					l.LogPath = fmt.Sprintf("files/insert_log/%d/%d/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID, field.FieldID, binlog.LogID)
				}
				f.Binlogs = append(f.Binlogs, l)
			}
			segment.Binlogs = append(segment.Binlogs, f)
		}
	}

	if len(segment.Deltalogs) == 0 {
		prefix := path.Join(basePath, "datacoord-meta", fmt.Sprintf("deltalog/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID))
		fields, _, err := ListProtoObjects[datapb.FieldBinlog](context.Background(), cli, prefix)
		if err != nil {
			return err
		}

		segment.Deltalogs = make([]*datapb.FieldBinlog, 0, len(fields))
		for _, field := range fields {
			f := &datapb.FieldBinlog{
				FieldID: field.FieldID,
				Binlogs: make([]*datapb.Binlog, 0, len(field.Binlogs)),
			}

			for _, binlog := range field.Binlogs {
				l := &datapb.Binlog{
					EntriesNum:    binlog.EntriesNum,
					TimestampFrom: binlog.TimestampFrom,
					TimestampTo:   binlog.TimestampTo,
					LogPath:       binlog.LogPath,
					LogSize:       binlog.LogSize,
				}
				if l.LogPath == "" {
					l.LogPath = fmt.Sprintf("files/delta_log/%d/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID, binlog.LogID)
				}
				f.Binlogs = append(f.Binlogs, l)
			}
			segment.Deltalogs = append(segment.Deltalogs, f)
		}
	}

	if len(segment.Statslogs) == 0 {
		prefix := path.Join(basePath, "datacoord-meta", fmt.Sprintf("statslog/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID))
		fields, _, err := ListProtoObjects[datapb.FieldBinlog](context.Background(), cli, prefix)
		if err != nil {
			return err
		}

		segment.Statslogs = make([]*datapb.FieldBinlog, 0, len(fields))
		for _, field := range fields {
			f := &datapb.FieldBinlog{
				FieldID: field.FieldID,
				Binlogs: make([]*datapb.Binlog, 0, len(field.Binlogs)),
			}

			for _, binlog := range field.Binlogs {
				l := &datapb.Binlog{
					EntriesNum:    binlog.EntriesNum,
					TimestampFrom: binlog.TimestampFrom,
					TimestampTo:   binlog.TimestampTo,
					LogPath:       binlog.LogPath,
					LogSize:       binlog.LogSize,
				}
				if l.LogPath == "" {
					l.LogPath = fmt.Sprintf("files/statslog/%d/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID, binlog.LogID)
				}
				f.Binlogs = append(f.Binlogs, l)
			}
			segment.Statslogs = append(segment.Statslogs, f)
		}
	}

	return nil
}

// ListLoadedSegments list v2.1 loaded segment info.
func ListLoadedSegments(cli kv.MetaKV, basePath string, filter func(*querypb.SegmentInfo) bool) ([]*querypb.SegmentInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	prefix := path.Join(basePath, "queryCoord-segmentMeta")

	segments, _, err := ListProtoObjects(ctx, cli, prefix, filter)
	if err != nil {
		return nil, err
	}

	return segments, nil
}

// RemoveSegment delete segment entry from etcd.
func RemoveSegment(ctx context.Context, cli kv.MetaKV, basePath string, info *datapb.SegmentInfo) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	segmentPath := path.Join(basePath, "datacoord-meta/s", fmt.Sprintf("%d/%d/%d", info.CollectionID, info.PartitionID, info.ID))
	err := cli.Remove(ctx, segmentPath)
	if err != nil {
		return err
	}

	// delete binlog entries
	binlogPrefix := path.Join(basePath, "datacoord-meta/binlog", fmt.Sprintf("%d/%d/%d", info.CollectionID, info.PartitionID, info.ID))
	err = cli.Remove(ctx, binlogPrefix)
	if err != nil {
		fmt.Printf("failed to delete binlogs from etcd for segment %d, err: %s\n", info.GetID(), err.Error())
	}

	// delete deltalog entries
	deltalogPrefix := path.Join(basePath, "datacoord-meta/deltalog", fmt.Sprintf("%d/%d/%d", info.CollectionID, info.PartitionID, info.ID))
	err = cli.Remove(ctx, deltalogPrefix)
	if err != nil {
		fmt.Printf("failed to delete deltalogs from etcd for segment %d, err: %s\n", info.GetID(), err.Error())
	}

	// delete statslog entries
	statslogPrefix := path.Join(basePath, "datacoord-meta/statslog", fmt.Sprintf("%d/%d/%d", info.CollectionID, info.PartitionID, info.ID))
	err = cli.Remove(ctx, statslogPrefix)
	if err != nil {
		fmt.Printf("failed to delete statslogs from etcd for segment %d, err: %s\n", info.GetID(), err.Error())
	}

	return err
}

func RemoveSegmentByID(ctx context.Context, cli kv.MetaKV, basePath string, collectionID, partitionID, segmentID int64) error {
	segmentPath := path.Join(basePath, "datacoord-meta/s", fmt.Sprintf("%d/%d/%d", collectionID, partitionID, segmentID))
	err := cli.Remove(ctx, segmentPath)
	if err != nil {
		return err
	}

	// delete binlog entries
	binlogPrefix := path.Join(basePath, "datacoord-meta/binlog", fmt.Sprintf("%d/%d/%d", collectionID, partitionID, segmentID))
	err = cli.Remove(ctx, binlogPrefix)
	if err != nil {
		fmt.Printf("failed to delete binlogs from etcd for segment %d, err: %s\n", segmentID, err.Error())
	}

	// delete deltalog entries
	deltalogPrefix := path.Join(basePath, "datacoord-meta/deltalog", fmt.Sprintf("%d/%d/%d", collectionID, partitionID, segmentID))
	err = cli.Remove(ctx, deltalogPrefix)
	if err != nil {
		fmt.Printf("failed to delete deltalogs from etcd for segment %d, err: %s\n", segmentID, err.Error())
	}

	// delete statslog entries
	statslogPrefix := path.Join(basePath, "datacoord-meta/statslog", fmt.Sprintf("%d/%d/%d", collectionID, partitionID, segmentID))
	err = cli.Remove(ctx, statslogPrefix)
	if err != nil {
		fmt.Printf("failed to delete statslogs from etcd for segment %d, err: %s\n", segmentID, err.Error())
	}

	return err
}

func RemoveSegmentInsertLogPath(ctx context.Context, cli kv.MetaKV, basePath string, collectionID, partitionID, segmentID, fieldID, logID int64) error {
	// delete binlog entries
	BinLogPath := path.Join(basePath, "datacoord-meta", "insert_log", fmt.Sprintf("%d/%d/%d/%d/%d", collectionID, partitionID, segmentID, fieldID, logID))
	fmt.Println("remove", BinLogPath)
	err := cli.Remove(ctx, BinLogPath)
	if err != nil {
		fmt.Printf("failed to delete insert binlogs from etcd for segment %d, err: %s\n", segmentID, err.Error())
	}
	return err
}

func RemoveSegmentDeltaLogPath(ctx context.Context, cli kv.MetaKV, basePath string, collectionID, partitionID, segmentID, logID int64) error {
	// delete binlog entries
	DeltaLogPath := path.Join(basePath, "datacoord-meta", "delta_log", fmt.Sprintf("%d/%d/%d/%d", collectionID, partitionID, segmentID, logID))
	fmt.Println("remove", DeltaLogPath)
	err := cli.Remove(ctx, DeltaLogPath)
	if err != nil {
		fmt.Printf("failed to delete delta binlogs from etcd for segment %d, err: %s\n", segmentID, err.Error())
	}
	return err
}

func RemoveSegmentStatLogPath(ctx context.Context, cli kv.MetaKV, basePath string, collectionID, partitionID, segmentID, fieldID, logID int64) error {
	// delete binlog entries
	StatLogPath := path.Join(basePath, "datacoord-meta", "stats_log", fmt.Sprintf("%d/%d/%d/%d/%d", collectionID, partitionID, segmentID, fieldID, logID))
	fmt.Println("remove", StatLogPath)
	err := cli.Remove(ctx, StatLogPath)
	if err != nil {
		fmt.Printf("failed to delete stat logs from etcd for segment %d, err: %s\n", segmentID, err.Error())
	}
	return err
}

func UpdateSegments(ctx context.Context, cli kv.MetaKV, basePath string, collectionID int64, fn func(segment *datapb.SegmentInfo)) error {
	prefix := path.Join(basePath, fmt.Sprintf("%s/%d", SegmentMetaPrefix, collectionID)) + "/"
	segments, keys, err := ListProtoObjects[datapb.SegmentInfo](ctx, cli, prefix)
	if err != nil {
		return err
	}

	for idx, info := range segments {
		fn(info)
		bs, err := proto.Marshal(info)
		if err != nil {
			return err
		}

		err = cli.Save(ctx, keys[idx], string(bs))
		if err != nil {
			return err
		}
	}
	return nil
}

// WalkAllSegments walk all segment info from etcd with func
func WalkAllSegments(cli kv.MetaKV, basePath string, filter func(*datapb.SegmentInfo) bool, op func(*datapb.SegmentInfo) error, limit int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	cnt := int64(0)
	return WalkWithPrefix(ctx, cli, path.Join(basePath, SegmentMetaPrefix)+"/", 1000, func(k []byte, v []byte) error {
		info := &datapb.SegmentInfo{}
		err := proto.Unmarshal(v, info)
		if err != nil {
			return err
		}

		if filter == nil || filter(info) {
			err = op(info)
			if err != nil {
				return err
			}
			cnt++
			if cnt >= limit {
				return ErrReachMaxNumOfWalkSegment
			}
		}
		return nil
	})
}

func WalkWithPrefix(ctx context.Context, cli kv.MetaKV, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	return cli.WalkWithPrefix(ctx, prefix, paginationSize, func(key, value []byte) error {
		return fn(key, value)
	})
}
