package repair

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
)

type WALRecoveryStorageParam struct {
	framework.ParamBase `use:"repair wal-recovery-storage" desc:"recover wal from storage"`
	Run                 bool `name:"run" default:"false" desc:"actual do repair"`
}

func (c *ComponentRepair) WALRecoveryStorageCommand(ctx context.Context, p *WALRecoveryStorageParam) error {
	metas, err := common.ListWALDistribution(ctx, c.client, c.basePath, "")
	if err != nil {
		return errors.Wrap(err, "failed to list wal distribution")
	}

	needFix := make([]*streamingpb.VChannelMeta, 0)
	for _, meta := range metas {
		pchannel := meta.Channel.Name
		storage, err := common.ListWALRecoveryStorage(ctx, c.client, c.basePath, pchannel)
		if err != nil {
			return errors.Wrap(err, "failed to list wal recovery storage")
		}
		for _, vchannel := range storage.VChannels {
			if len(vchannel.CollectionInfo.Schemas) == 0 {
				// If the schema is lost, we need to fix it.
				needFix = append(needFix, vchannel)
			}
		}
	}

	if len(needFix) == 0 {
		fmt.Println("no need to fix wal recovery storage")
		return nil
	}
	collectionIDs := make(map[int64]bool)
	for _, vchannel := range needFix {
		collectionIDs[vchannel.CollectionInfo.CollectionId] = true
	}
	collections, err := common.ListCollections(ctx, c.client, c.basePath, func(collection *models.Collection) bool {
		_, ok := collectionIDs[collection.GetProto().GetID()]
		return ok
	})
	if err != nil {
		return errors.Wrap(err, "failed to list collections")
	}
	collectionMap := make(map[int64]*models.Collection)
	for _, collection := range collections {
		collectionMap[collection.GetProto().GetID()] = collection
	}
	for _, vchannel := range needFix {
		if _, ok := collectionMap[vchannel.CollectionInfo.CollectionId]; !ok {
			return errors.Errorf("collection not found, collectionID: %d", vchannel.CollectionInfo.CollectionId)
		}
	}

	if p.Run {
		fmt.Println("run mode, will fix wal recovery storage...")
	} else {
		fmt.Println("dry run mode, will not fix wal recovery storage...")
	}
	for _, vchannel := range needFix {
		collection := collectionMap[vchannel.CollectionInfo.CollectionId]
		collectionProto := collectionMap[vchannel.CollectionInfo.CollectionId].GetProto()
		schema := &streamingpb.CollectionSchemaOfVChannel{
			Schema: &schemapb.CollectionSchema{
				Name:               collectionProto.Schema.Name,
				Description:        collectionProto.Schema.Description,
				AutoID:             collectionProto.Schema.AutoID,
				Fields:             collectionProto.Schema.Fields,
				EnableDynamicField: collectionProto.Schema.EnableDynamicField,
				Properties:         collectionProto.Properties,
				Functions: lo.Map(collection.Functions, func(function *models.Function, _ int) *schemapb.FunctionSchema {
					return function.GetProto()
				}),
				DbName:            collectionProto.Schema.DbName,
				StructArrayFields: collectionProto.Schema.StructArrayFields,
			},
			CheckpointTimeTick: 0,
			State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
		}
		fmt.Println("--------------------------------")
		if !p.Run {
			schemaJSON, err := protojson.MarshalOptions{
				EmitUnpopulated:   true,
				EmitDefaultValues: true,
			}.Marshal(schema)
			if err != nil {
				return errors.Wrap(err, "failed to marshal schema")
			}
			fmt.Printf("%s will be fixed with schema \n %s\n", vchannel.Vchannel, string(schemaJSON))
		} else {
			if err := c.fixWALRecoveryStorage(ctx, vchannel, schema); err != nil {
				fmt.Printf("failed to fix wal recovery storage for vchannel %s, error: %s\n", vchannel.Vchannel, err.Error())
				return err
			}
			fmt.Printf("%s fixed\n", vchannel.Vchannel)
		}
		fmt.Println("--------------------------------")
	}
	return nil
}

func (c *ComponentRepair) fixWALRecoveryStorage(ctx context.Context, vchannel *streamingpb.VChannelMeta, schema *streamingpb.CollectionSchemaOfVChannel) error {
	return common.SaveSchemaForVChannel(ctx, c.client, c.basePath, vchannel, schema)
}
