package repair

import (
	"context"
	"fmt"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
)

type RepairCollectionDBNameParam struct {
	framework.ExecutionParam `use:"repair collection-dbname" desc:"check and fix DBName in CollectionMeta to match the DBName from the corresponding DBMeta"`
}

func (c *ComponentRepair) RepairCollectionDBNameCommand(ctx context.Context, p *RepairCollectionDBNameParam) error {
	databases, err := common.ListDatabase(ctx, c.client, c.basePath)
	if err != nil {
		return fmt.Errorf("failed to list databases: %w", err)
	}

	id2db := lo.SliceToMap(databases, func(item *models.Database) (int64, *models.Database) {
		return item.GetProto().GetId(), item
	})

	collections, err := common.ListCollectionWithoutFields(ctx, c.client, c.basePath)
	if err != nil {
		return fmt.Errorf("failed to list collections: %w", err)
	}

	if len(collections) == 0 {
		fmt.Println("No collections found.")
		return nil
	}

	fmt.Printf("Found %d collection(s), %d database(s)\n", len(collections), len(databases))

	mismatchCount := 0
	for _, coll := range collections {
		collProto := coll.GetProto()
		dbID := collProto.GetDbId()
		currentDBName := collProto.GetSchema().GetDbName()

		db, ok := id2db[dbID]
		if !ok {
			fmt.Printf("[warn] collection %d (%s) references dbID=%d which does not exist in DBMeta, skipping\n",
				collProto.GetID(), collProto.GetSchema().GetName(), dbID)
			continue
		}

		expectedDBName := db.GetProto().GetName()
		if currentDBName == expectedDBName {
			continue
		}

		mismatchCount++
		fmt.Printf("Mismatch: collection %d (%s) has DBName=%q, expected=%q (dbID=%d)\n",
			collProto.GetID(), collProto.GetSchema().GetName(), currentDBName, expectedDBName, dbID)

		if !p.Run {
			fmt.Printf("[dry-run] would fix DBName for collection %d\n", collProto.GetID())
			continue
		}

		collProto.Schema.DbName = expectedDBName
		bs, err := proto.Marshal(collProto)
		if err != nil {
			return fmt.Errorf("failed to marshal collection %d: %w", collProto.GetID(), err)
		}
		if err := c.client.Save(ctx, coll.Key(), string(bs)); err != nil {
			return fmt.Errorf("failed to save collection %d: %w", collProto.GetID(), err)
		}
		fmt.Printf("Fixed DBName for collection %d: %q -> %q\n", collProto.GetID(), currentDBName, expectedDBName)
	}

	if mismatchCount == 0 {
		fmt.Println("All collections have correct DBName. No fix needed.")
	} else {
		fmt.Printf("Total mismatches: %d\n", mismatchCount)
	}

	return nil
}
