package remove

import (
	"context"
	"fmt"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type RemoveIndexParam struct {
	framework.ExecutionParam `use:"remove index" desc:"Remove index meta"`
	IndexID                  int64 `name:"indexID" default:"0" desc:"index id to remove"`
}

func (c *ComponentRemove) RemoveIndexCommand(ctx context.Context, p *RemoveIndexParam) error {
	indexes, err := common.ListIndex(ctx, c.client, c.basePath, func(index *models.FieldIndex) bool {
		return index.GetProto().GetIndexInfo().GetIndexID() == p.IndexID
	})
	if err != nil {
		return err
	}

	if len(indexes) == 0 {
		fmt.Printf("no index found with index id %d\n", p.IndexID)
		return nil
	}

	if !p.Run {
		fmt.Println("===Dry Run ===")
		for _, index := range indexes {
			fmt.Printf("Hit index: %s\n", index.GetProto().String())
			fmt.Printf("Delete Key: %s\n\n", index.Key())
		}
		return nil
	}

	for _, index := range indexes {
		err := c.client.Remove(ctx, index.Key())
		if err != nil {
			return err
		}
	}
	fmt.Printf("Remove index %d done, mixcoord shall be restarted to apply change\n", p.IndexID)

	return nil
}
