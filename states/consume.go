package states

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/mq"
	"github.com/milvus-io/birdwatcher/mq/ifc"
	"github.com/milvus-io/birdwatcher/proto/v2.2/commonpb"
	"github.com/milvus-io/birdwatcher/proto/v2.2/msgpb"
)

type ConsumeParam struct {
	framework.ParamBase `use:"consume" desc:"consume msgs from provided topic"`
	MqType              string `name:"mq_type" default:"pulsar" desc:"message queue type to consume"`
	MqAddress           string `name:"mq_addr" default:"pulsar://127.0.0.1:6650" desc:"message queue service address"`
	Topic               string `name:"topic" default:"" desc:"topic to consume"`
	ShardName           string `name:"shard_name" default:"" desc:"shard name(vchannel name) to filter with"`
	Detail              bool   `name:"detail" default:"false" desc:"print msg detail"`
}

func (s *InstanceState) ConsumeCommand(ctx context.Context, p *ConsumeParam) error {
	c, err := mq.NewConsumer(p.MqType, p.MqAddress, p.Topic, ifc.MqOption{
		SubscriptionInitPos: ifc.SubscriptionPositionEarliest,
	})
	if err != nil {
		return err
	}

	latestID, err := c.GetLastMessageID()
	if err != nil {
		return err
	}
	if latestID.AtEarliestPosition() {
		fmt.Println("empty topic")
		return nil
	}

	for {
		msg, err := c.Consume()
		if err != nil {
			return err
		}
		header := commonpb.MsgHeader{}
		proto.Unmarshal(msg.Payload(), &header)
		msgType := header.GetBase().GetMsgType()
		if msgType != commonpb.MsgType_TimeTick {
			fmt.Printf("%s ", msgType)
			switch msgType {
			case commonpb.MsgType_Insert, commonpb.MsgType_Delete:
				v, err := ParseMsg(header.GetBase().GetMsgType(), msg.Payload())
				if err != nil {
					fmt.Println(err.Error())
				}
				if p.ShardName == "" || v.GetShardName() == p.ShardName {
					if p.Detail {
						fmt.Print(v)
					} else {
						fmt.Print(v.GetShardName())
					}
				}
			default:
			}
			fmt.Println()
		}
		if eq, _ := msg.ID().Equal(latestID.Serialize()); eq {
			break
		}
	}
	return nil
}

func ParseMsg(msgType commonpb.MsgType, payload []byte) (interface {
	fmt.Stringer
	GetShardName() string
}, error) {
	var msg interface {
		proto.Message
		GetShardName() string
	}
	switch msgType {
	case commonpb.MsgType_Insert:
		msg = &msgpb.InsertRequest{}
	case commonpb.MsgType_Delete:
		msg = &msgpb.DeleteRequest{}
	}
	err := proto.Unmarshal(payload, msg)
	if err != nil {
		return nil, err
	}
	return msg, nil
}
