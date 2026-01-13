package remove

import (
	"context"
	"fmt"
	"path"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

var backupKeyPrefix = "birdwatcher/backup"

type RemoveBinlogParam struct {
	framework.ParamBase `use:"remove etcd-config" desc:"remove etcd stored configuations"`
	LogType             string `name:"p.LogType" default:"unknown" desc:"log type: binlog/deltalog/statslog"`
	CollectionID        int64  `name:"p.CollectionID" default:"0" desc:"collection id to remove"`
	PartitionID         int64  `name:"p.PartitionID" default:"0" desc:"partition id to remove"`
	SegmentID           int64  `name:"p.SegmentID" default:"0" desc:"segment id to remove"`
	FieldID             int64  `name:"p.FieldID" default:"0" desc:"field id to remove"`
	LogID               int64  `name:"logID" default:"0" desc:"log id to remove"`

	Restore   bool `name:"restore" default:"false" desc:"flags indicating whether to restore removed command"`
	RemoveAll bool `name:"removeAll" default:"false" desc:"remove all binlogs belongs to the field"`
	Run       bool `name:"run" default:"false" desc:"flag to control actually run or dry"`
}

func (c *ComponentRemove) RemoveBinlogCommand(ctx context.Context, p *RemoveBinlogParam) error {
	var key string
	switch p.LogType {
	case "binlog":
		key = path.Join(c.basePath, "datacoord-meta",
			fmt.Sprintf("binlog/%d/%d/%d/%d", p.CollectionID, p.PartitionID, p.SegmentID, p.FieldID))
	case "deltalog":
		key = path.Join(c.basePath, "datacoord-meta",
			fmt.Sprintf("deltalog/%d/%d/%d/%d", p.CollectionID, p.PartitionID, p.SegmentID, p.FieldID))
	case "statslog":
		key = path.Join(c.basePath, "datacoord-meta",
			fmt.Sprintf("statslog/%d/%d/%d/%d", p.CollectionID, p.PartitionID, p.SegmentID, p.FieldID))
	default:
		return fmt.Errorf("p.LogType unknown: %s", p.LogType)
	}

	if p.Restore {
		err := c.restoreBinlog(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to restore binlog, %s", err.Error())
		}
		return nil
	}

	err := c.backupBinlog(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to backup binlog, %s", err.Error())
	}

	// remove all
	if p.RemoveAll {
		_, err = c.getFieldBinlog(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get field binlog, %s", err.Error())
		}
		if !p.Run {
			return nil
		}
		fmt.Printf("key: %s will be deleted\n", key)
		err = c.removeBinlog(ctx, key)
		if err != nil {
			return err
		}
		return nil
	}

	// remove one
	{
		fieldBinlog, err := c.getFieldBinlog(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get field binlog, %s", err.Error())
		}
		fieldBinlog, err = removeLogFromFieldBinlog(key, p.LogID, fieldBinlog)
		if err != nil {
			return fmt.Errorf("failed to remove log from field binlog, %s", err.Error())
		}

		if !p.Run {
			return nil
		}

		err = c.saveFieldBinlog(ctx, key, fieldBinlog)
		if err != nil {
			return err
		}
		fmt.Printf("Remove one binlog %s/%d from etcd succeeds.\n", key, p.LogID)
	}
	return nil
}

func (c *ComponentRemove) backupBinlog(ctx context.Context, key string) error {
	val, err := c.client.Load(ctx, key)
	if err != nil {
		fmt.Printf("get key:%s failed\n", key)
		return err
	}

	backupKey := path.Join(backupKeyPrefix, key)
	fmt.Printf("start backup key:%s to %s \n", key, backupKey)
	err = c.client.Save(ctx, backupKey, val)
	if err != nil {
		fmt.Println("failed save kv into etcd, ", err.Error())
		return err
	}
	fmt.Printf("backup key:%s finished\n", key)
	return nil
}

func (c *ComponentRemove) restoreBinlog(ctx context.Context, key string) error {
	backupKey := path.Join(backupKeyPrefix, key)
	val, err := c.client.Load(ctx, backupKey)
	if err != nil {
		fmt.Printf("get backup key:%s failed\n", backupKey)
		return err
	}

	fmt.Printf("start restore key:%s to %s\n", backupKey, key)
	err = c.client.Save(ctx, key, val)
	if err != nil {
		fmt.Println("failed save kv into etcd, ", err.Error())
		return err
	}
	fmt.Printf("restore key:%s finished\n", key)
	return nil
}

func (c *ComponentRemove) removeBinlog(ctx context.Context, key string) error {
	err := c.client.Remove(ctx, key)
	if err != nil {
		fmt.Printf("delete key:%s failed\n", key)
		return err
	}
	fmt.Printf("remove key:%s finished\n", key)
	return nil
}

func (c *ComponentRemove) getFieldBinlog(ctx context.Context, key string) (*datapb.FieldBinlog, error) {
	value, err := c.client.Load(ctx, key)
	if err != nil {
		fmt.Printf("get key:%s failed\n", key)
		return nil, err
	}
	fieldBinlog := &datapb.FieldBinlog{}
	err = proto.Unmarshal([]byte(value), fieldBinlog)
	if err != nil {
		return nil, err
	}
	fmt.Println("FieldBinlog(before):")
	fmt.Println("**************************************")
	fmt.Println(fieldBinlog)
	fmt.Println("**************************************")
	return fieldBinlog, nil
}

func removeLogFromFieldBinlog(key string, logID int64, fieldBinlog *datapb.FieldBinlog) (*datapb.FieldBinlog, error) {
	binlogs := lo.Filter(fieldBinlog.GetBinlogs(), func(binlog *datapb.Binlog, _ int) bool {
		if logID == binlog.GetLogID() {
			fmt.Printf("logID matched, binlog: %s/%d\n", key, logID)
		}
		return logID != binlog.GetLogID()
	})
	fieldBinlog.Binlogs = binlogs

	fmt.Println("FieldBinlog(after):")
	fmt.Println("**************************************")
	fmt.Println(fieldBinlog)
	fmt.Println("**************************************")
	return fieldBinlog, nil
}

func (c *ComponentRemove) saveFieldBinlog(ctx context.Context, key string, fieldBinlog *datapb.FieldBinlog) error {
	mb, err := proto.Marshal(fieldBinlog)
	if err != nil {
		return err
	}
	err = c.client.Save(ctx, key, string(mb))
	if err != nil {
		fmt.Println("failed save field binlog kv into etcd, ", err.Error())
		return err
	}
	fmt.Printf("save field binlog kv done. key: %s\n", key)
	return nil
}
