package states

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type WALMessageIDParam struct {
	framework.ParamBase `use:"parse-wal-message-id" desc:"parse wal message id from binary"`
	IDBinary            string `name:"id-binary" default:"" desc:"string of binary message id"`
	WALName             string `name:"wal-name" default:"" desc:"a hint of wal name, auto guess if not provided"`
}

func (c *ApplicationState) WalMessageIDCommand(ctx context.Context, p *WALMessageIDParam) error {
	if p.WALName != "" {
		messageID := common.GetMessageIDString(p.WALName, p.IDBinary)
		fmt.Printf("%s [%s]\n", messageID, p.WALName)
		return nil
	}
	walName, messageID := common.GuessWALName(p.IDBinary)
	fmt.Printf("%s [%s]\n", messageID, walName)
	return nil
}
