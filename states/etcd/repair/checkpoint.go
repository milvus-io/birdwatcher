package repair

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/mq"
	"github.com/milvus-io/birdwatcher/mq/ifc"
	"github.com/milvus-io/birdwatcher/proto/v2.0/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.0/internalpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	etcdversion "github.com/milvus-io/birdwatcher/states/etcd/version"
	"github.com/milvus-io/birdwatcher/utils"
)

// CheckpointCommand usage:
// repair checkpoint --collection 437744071571606912 --vchannel by-dev-rootcoord-dml_3_437744071571606912v1 --mq_type kafka --address localhost:9092 --set_to latest-msgid
// repair checkpoint --collection 437744071571606912 --vchannel by-dev-rootcoord-dml_3_437744071571606912v1 --mq_type pulsar --address pulsar://localhost:6650 --set_to latest-msgid
func CheckpointCommand(cli clientv3.KV, basePath string) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "checkpoint",
		Short:   "reset checkpoint of vchannels to latest checkpoint(or latest msgID) of physical channel",
		Aliases: []string{"rc"},
		Run: func(cmd *cobra.Command, args []string) {
			collID, err := cmd.Flags().GetInt64("collection")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			vchannel, err := cmd.Flags().GetString("vchannel")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			setTo, err := cmd.Flags().GetString("set_to")
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			//coll, err := common.GetCollectionByID(cli, basePath, collID)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			coll, err := common.GetCollectionByIDVersion(ctx, cli, basePath, etcdversion.GetVersion(), collID)

			if err != nil {
				fmt.Println("failed to get collection", err.Error())
				return
			}

			switch setTo {
			case "latest-cp":
				setCheckPointWithLatestCheckPoint(cli, basePath, coll, vchannel)
				return
			case "latest-msgid":
				mqType, err := cmd.Flags().GetString("mq_type")
				if err != nil {
					fmt.Println(err.Error())
					return
				}

				address, err := cmd.Flags().GetString("address")
				if err != nil {
					fmt.Println(err.Error())
					return
				}
				setCheckPointWithLatestMsgID(cli, basePath, coll, mqType, address, vchannel)
				return
			default:
				fmt.Println("Unknown set to target:", setTo)
				return
			}
		},
	}
	cmd.Flags().Int64("collection", 0, "collection id")
	cmd.Flags().String("vchannel", "", "vchannel name")
	cmd.Flags().String("set_to", "latest-cp", "support latest-cp(the latest checkpoint from segment checkpoint of "+
		"corresponding collection on this physical channel) and latest-msgid(the latest msg from this physical channel)")
	cmd.Flags().String("mq_type", "kafka", "MQ type, only support kafka(default) and pulsar")
	cmd.Flags().String("address", "localhost:9092", "mq endpoint, default value is kafka address")
	return cmd
}

func setCheckPointWithLatestMsgID(cli clientv3.KV, basePath string, coll *models.Collection, mqType, address, vchannel string) {
	for _, ch := range coll.Channels {
		if ch.VirtualName == vchannel {
			pChannel := ch.PhysicalName
			cp, err := getLatestFromPChannel(mqType, address, vchannel)
			if err != nil {
				fmt.Printf("vchannel:%s -> pchannel:%s, get latest msgID faile, err:%s\n", ch.VirtualName, pChannel, err.Error())
				return
			}

			err = saveChannelCheckpoint(cli, basePath, ch.VirtualName, cp)
			t, _ := utils.ParseTS(cp.GetTimestamp())
			if err != nil {
				fmt.Printf("failed to set latest msgID(ts:%v) for vchannel:%s", t, ch.VirtualName)
				return
			}
			fmt.Printf("vchannel:%s set to latest msgID(ts:%v) finshed\n", vchannel, t)
			return
		}
	}
	fmt.Printf("vchannel:%s doesn't exists in collection: %d\n", vchannel, coll.ID)
}

func setCheckPointWithLatestCheckPoint(cli clientv3.KV, basePath string, coll *models.Collection, vchannel string) {
	pChannelName2LatestCP, err := getLatestCheckpointFromPChannel(cli, basePath)
	if err != nil {
		fmt.Println("failed to get latest cp of all pchannel", err.Error())
		return
	}

	fmt.Println("list the latest checkpoint of all physical channels:")
	for k, v := range pChannelName2LatestCP {
		t, _ := utils.ParseTS(v.GetTimestamp())
		fmt.Printf("pchannel: %s, the lastest checkpoint ts: %v\n", k, t)
	}

	for _, ch := range coll.Channels {
		if ch.VirtualName == vchannel {
			pChannel := ch.PhysicalName
			cp, ok := pChannelName2LatestCP[pChannel]
			if !ok {
				fmt.Printf("vchannel:%s -> pchannel:%s, the pchannel doesn't exists\n", ch.VirtualName, pChannel)
				return
			}

			err := saveChannelCheckpoint(cli, basePath, ch.VirtualName, cp)
			t, _ := utils.ParseTS(cp.GetTimestamp())
			if err != nil {
				fmt.Printf("failed to set latest checkpoint(ts:%v) for vchannel:%s", t, ch.VirtualName)
				return
			}
			fmt.Printf("vchannel:%s set to latest checkpoint(ts:%v) finshed\n", vchannel, t)
			return
		}
	}

	fmt.Printf("vchannel:%s doesn't exists in collection: %d\n", vchannel, coll.ID)
}

func saveChannelCheckpoint(cli clientv3.KV, basePath string, channelName string, pos *internalpb.MsgPosition) error {
	key := path.Join(basePath, "datacoord-meta", "channel-cp", channelName)
	bs, err := proto.Marshal(pos)
	if err != nil {
		fmt.Println("failed to marshal segment info", err.Error())
	}
	_, err = cli.Put(context.Background(), key, string(bs))
	return err
}

func getLatestCheckpointFromPChannel(cli clientv3.KV, basePath string) (map[string]*internalpb.MsgPosition, error) {
	segments, err := common.ListSegments(cli, basePath, func(info *datapb.SegmentInfo) bool {
		return true
	})
	if err != nil {
		fmt.Printf("fail to list segment for all channel, err: %s\n", err.Error())
		return nil, err
	}

	ret := make(map[string]*internalpb.MsgPosition)
	for _, segment := range segments {
		if segment.State != commonpb.SegmentState_Flushed &&
			segment.State != commonpb.SegmentState_Growing &&
			segment.State != commonpb.SegmentState_Flushing {
			continue
		}
		// skip all empty segment
		if segment.GetDmlPosition() == nil && segment.GetStartPosition() == nil {
			continue
		}

		var segPos *internalpb.MsgPosition
		if segment.GetDmlPosition() != nil {
			segPos = segment.GetDmlPosition()
		} else {
			segPos = segment.GetStartPosition()
		}

		pChannel := ToPhysicalChannel(segment.GetInsertChannel())
		pos, ok := ret[pChannel]
		if !ok || segPos.GetTimestamp() > pos.GetTimestamp() {
			ret[pChannel] = segPos
		}
	}

	return ret, nil
}

func getLatestFromPChannel(mqType, address, vchannel string) (*internalpb.MsgPosition, error) {
	topic := ToPhysicalChannel(vchannel)
	consumer, err := mq.NewConsumer(mqType, address, topic, ifc.MqOption{
		SubscriptionInitPos: ifc.SubscriptionPositionLatest,
	})
	if err != nil {
		return nil, err
	}
	defer consumer.Close()

	msg, err := consumer.GetLastMessage()
	if err != nil {
		return nil, err
	}

	position, err := mq.GetMsgPosition(msg)
	if err != nil {
		return nil, err
	}

	return position, nil
}

// ToPhysicalChannel get physical channel name from virtual channel name
func ToPhysicalChannel(vchannel string) string {
	index := strings.LastIndex(vchannel, "_")
	if index < 0 {
		return vchannel
	}
	return vchannel[:index]
}
