package show

import (
	"context"
	"fmt"
	"strings"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/utils"
)

type IndexParam struct {
	framework.DataSetParam `use:"show index" desc:"" alias:"indexes"`
	CollectionID           int64 `name:"collection" default:"0" desc:"collection id to list index on"`
}

// IndexCommand returns show index command.
func (c *ComponentShow) IndexCommand(ctx context.Context, p *IndexParam) (*framework.PresetResultSet, error) {
	fieldIndexes, err := common.ListIndex(ctx, c.client, c.metaPath, func(info *models.FieldIndex) bool {
		return p.CollectionID == 0 || p.CollectionID == info.GetProto().GetIndexInfo().GetCollectionID()
	})
	if err != nil {
		return nil, err
	}

	return framework.NewPresetResultSet(framework.NewListResult[Indexes](fieldIndexes), framework.NameFormat(p.Format)), nil
}

type Indexes struct {
	framework.ListResultSet[*models.FieldIndex]
}

func (rs *Indexes) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		sb := &strings.Builder{}
		for _, info := range rs.Data {
			rs.printIndex(sb, info)
		}
		fmt.Fprintf(sb, "--- Total Index(es): %d\n", len(rs.Data))
		return sb.String()
	case framework.FormatJSON:
		return rs.printAsJSON()
	default:
	}
	return ""
}

func (rs *Indexes) printAsJSON() string {
	type IndexParamJSON struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	type IndexJSON struct {
		IndexID      int64            `json:"index_id"`
		IndexName    string           `json:"index_name"`
		CollectionID int64            `json:"collection_id"`
		FieldID      int64            `json:"field_id"`
		IndexType    string           `json:"index_type"`
		MetricType   string           `json:"metric_type"`
		CreateTime   string           `json:"create_time"`
		Deleted      bool             `json:"deleted"`
		IndexParams  []IndexParamJSON `json:"index_params"`
		UserParams   []IndexParamJSON `json:"user_params"`
	}

	type OutputJSON struct {
		Indexes []IndexJSON `json:"indexes"`
		Total   int         `json:"total"`
	}

	output := OutputJSON{
		Indexes: make([]IndexJSON, 0, len(rs.Data)),
		Total:   len(rs.Data),
	}

	for _, info := range rs.Data {
		index := info.GetProto()
		createTime, _ := utils.ParseTS(index.GetCreateTime())

		indexParams := make([]IndexParamJSON, 0)
		for _, kv := range index.GetIndexInfo().GetIndexParams() {
			indexParams = append(indexParams, IndexParamJSON{
				Key:   kv.GetKey(),
				Value: kv.GetValue(),
			})
		}

		userParams := make([]IndexParamJSON, 0)
		for _, kv := range index.GetIndexInfo().GetUserIndexParams() {
			userParams = append(userParams, IndexParamJSON{
				Key:   kv.GetKey(),
				Value: kv.GetValue(),
			})
		}

		output.Indexes = append(output.Indexes, IndexJSON{
			IndexID:      index.GetIndexInfo().GetIndexID(),
			IndexName:    index.GetIndexInfo().GetIndexName(),
			CollectionID: index.GetIndexInfo().GetCollectionID(),
			FieldID:      index.GetIndexInfo().GetFieldID(),
			IndexType:    common.GetKVPair(index.GetIndexInfo().GetIndexParams(), "index_type"),
			MetricType:   common.GetKVPair(index.GetIndexInfo().GetIndexParams(), "metric_type"),
			CreateTime:   createTime.Format(tsPrintFormat),
			Deleted:      index.GetDeleted(),
			IndexParams:  indexParams,
			UserParams:   userParams,
		})
	}

	return framework.MarshalJSON(output)
}

func (rs *Indexes) printIndex(sb *strings.Builder, info *models.FieldIndex) {
	index := info.GetProto()
	fmt.Fprintln(sb, "==================================================================")
	fmt.Fprintf(sb, "Index ID: %d\tIndex Name: %s\tCollectionID: %d\tFieldID: %d\n", index.GetIndexInfo().GetIndexID(), index.GetIndexInfo().GetIndexName(), index.GetIndexInfo().GetCollectionID(), index.GetIndexInfo().GetFieldID())
	createTime, _ := utils.ParseTS(index.GetCreateTime())
	fmt.Fprintf(sb, "Create Time: %s\tDeleted: %t\n", createTime.Format(tsPrintFormat), index.GetDeleted())
	indexParams := index.GetIndexInfo().GetIndexParams()
	fmt.Fprintf(sb, "Index Type: %s\tMetric Type: %s\n",
		common.GetKVPair(indexParams, "index_type"),
		common.GetKVPair(indexParams, "metric_type"),
	)
	fmt.Fprintf(sb, "ParamsJSON : %s\n", common.GetKVPair(index.GetIndexInfo().GetUserIndexParams(), "params"))
	// print detail param in meta
	fmt.Fprintln(sb, "Index.IndexParams:")
	for _, kv := range indexParams {
		fmt.Fprintf(sb, "\t%s: %s\n", kv.GetKey(), kv.GetValue())
	}
	fmt.Fprintln(sb, "Index.UserParams")
	for _, kv := range index.GetIndexInfo().GetUserIndexParams() {
		fmt.Fprintf(sb, "\t%s: %s\n", kv.GetKey(), kv.GetValue())
	}
	fmt.Fprintln(sb, "==================================================================")
}
