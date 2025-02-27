package repair

import (
	"context"
	"fmt"
	"path"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/mq"
	"github.com/milvus-io/birdwatcher/mq/ifc"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/birdwatcher/utils"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
)

type RepairCheckpointParam struct {
	framework.ParamBase `use:"repair checkpoint" desc:"reset checkpoint of vchannels to latest checkpoint(or latest msgID) of physical channel"`
	Collection          int64  `name:"collection" default:"0" desc:"collection id"`
	VChannel            string `name:"vchannel" default:"" desc:"vchannel name"`
	SetTo               string `name:"set_to" default:"latest-cp" desc:"support latest-cp(the latest checkpoint from segment checkpoint of corresponding collection on this physical channel) and latest-msgid(the latest msg from this physical channel)"`
	MqType              string `name:"mq_type" default:"kafka" desc:"MQ type, only support kafka(default) and pulsar"`
	Address             string `name:"address" default:"localhost:9092" desc:"mq endpoint, default value is kafka address"`
	Run                 bool   `name:"run" default:"false" desc:"actual do repair"`
}

// CheckpointCommand usage:
// repair checkpoint --collection 437744071571606912 --vchannel by-dev-rootcoord-dml_3_437744071571606912v1 --mq_type kafka --address localhost:9092 --set_to latest-msgid
// repair checkpoint --collection 437744071571606912 --vchannel by-dev-rootcoord-dml_3_437744071571606912v1 --mq_type pulsar --address pulsar://localhost:6650 --set_to latest-msgid
func (c *ComponentRepair) RepairCheckpointCommand(ctx context.Context, p *RepairCheckpointParam) error {
	coll, err := common.GetCollectionByIDVersion(ctx, c.client, c.basePath, p.Collection)
	if err != nil {
		return errors.Wrap(err, "failed to get collection")
	}

	switch p.SetTo {
	case "latest-cp":
		return setCheckPointWithLatestCheckPoint(ctx, c.client, c.basePath, coll, p.VChannel)
	case "latest-msgid":
		return setCheckPointWithLatestMsgID(ctx, c.client, c.basePath, coll, p.MqType, p.Address, p.VChannel)
	default:
		fmt.Println("Unknown set to target:", p.SetTo)
	}

	return nil
}

func setCheckPointWithLatestMsgID(ctx context.Context, cli kv.MetaKV, basePath string, coll *models.Collection, mqType, address, vchannel string) error {
	for _, ch := range coll.Channels() {
		if ch.VirtualName == vchannel {
			pChannel := ch.PhysicalName
			cp, err := getLatestFromPChannel(mqType, address, vchannel)
			if err != nil {
				return errors.Wrapf(err, "vchannel:%s -> pchannel:%s, get latest msgID failed", ch.VirtualName, pChannel)
			}

			err = saveChannelCheckpoint(ctx, cli, basePath, ch.VirtualName, cp)
			t, _ := utils.ParseTS(cp.GetTimestamp())
			if err != nil {
				return errors.Wrapf(err, "failed to set latest msgID(ts:%v) for vchannel:%s", t, ch.VirtualName)
			}
			fmt.Printf("vchannel:%s set to latest msgID(ts:%v) finshed\n", vchannel, t)
			return nil
		}
	}
	return errors.Newf("vchannel:%s doesn't exists in collection: %d\n", vchannel, coll.GetProto().ID)
}

func setCheckPointWithLatestCheckPoint(ctx context.Context, cli kv.MetaKV, basePath string, coll *models.Collection, vchannel string) error {
	pChannelName2LatestCP, err := getLatestCheckpointFromPChannel(ctx, cli, basePath)
	if err != nil {
		return errors.Wrap(err, "failed to get latest cp of all pchannel")
	}

	fmt.Println("list the latest checkpoint of all physical channels:")
	for k, v := range pChannelName2LatestCP {
		t, _ := utils.ParseTS(v.GetTimestamp())
		fmt.Printf("pchannel: %s, the lastest checkpoint ts: %v\n", k, t)
	}

	for _, ch := range coll.Channels() {
		if ch.VirtualName == vchannel {
			pChannel := ch.PhysicalName
			cp, ok := pChannelName2LatestCP[pChannel]
			if !ok {
				return errors.Errorf("vchannel:%s -> pchannel:%s, the pchannel doesn't exists\n", ch.VirtualName, pChannel)
			}

			t, _ := utils.ParseTS(cp.GetTimestamp())
			err := saveChannelCheckpoint(ctx, cli, basePath, ch.VirtualName, cp)
			if err != nil {
				return errors.Errorf("failed to set latest checkpoint(ts:%v) for vchannel:%s", t, ch.VirtualName)
			}
			fmt.Printf("vchannel:%s set to latest checkpoint(ts:%v) finshed\n", vchannel, t)
			return nil
		}
	}

	return errors.Newf("vchannel:%s doesn't exists in collection: %d\n", vchannel, coll.GetProto().ID)
}

func saveChannelCheckpoint(ctx context.Context, cli kv.MetaKV, basePath string, channelName string, pos *msgpb.MsgPosition) error {
	key := path.Join(basePath, "datacoord-meta", "channel-cp", channelName)
	bs, err := proto.Marshal(pos)
	if err != nil {
		fmt.Println("failed to marshal segment info", err.Error())
	}
	err = cli.Save(ctx, key, string(bs))
	return err
}

func getLatestCheckpointFromPChannel(ctx context.Context, cli kv.MetaKV, basePath string) (map[string]*msgpb.MsgPosition, error) {
	segments, err := common.ListSegments(ctx, cli, basePath)
	if err != nil {
		fmt.Printf("fail to list segment for all channel, err: %s\n", err.Error())
		return nil, err
	}

	ret := make(map[string]*msgpb.MsgPosition)
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

		var segPos *msgpb.MsgPosition
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

func getLatestFromPChannel(mqType, address, vchannel string) (*msgpb.MsgPosition, error) {
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
