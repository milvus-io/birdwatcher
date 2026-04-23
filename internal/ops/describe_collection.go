package ops

import (
	"context"
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

type DescribeCollectionParams struct {
	Name string `yaml:"name"`
}

func (p *DescribeCollectionParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if rc.Client == nil {
		return nil, errNoClient("describe_collection")
	}
	if p.Name == "" {
		return nil, fmt.Errorf("describe_collection: `name` required")
	}
	coll, err := rc.Client.DescribeCollection(ctx, milvusclient.NewDescribeCollectionOption(p.Name))
	if err != nil {
		return nil, err
	}
	PrintCollection(rc.Out(), coll)
	return coll, nil
}

// PrintCollection writes a readable summary. Exported so the CLI describe
// command can reuse the same formatting.
func PrintCollection(out interface{ Write([]byte) (int, error) }, c *entity.Collection) {
	fmt.Fprintf(out, "Name:     %s\n", c.Name)
	fmt.Fprintf(out, "ID:       %d\n", c.ID)
	fmt.Fprintf(out, "ShardNum: %d\n", c.ShardNum)
	fmt.Fprintf(out, "Loaded:   %v\n", c.Loaded)
	fmt.Fprintf(out, "Consist.: %s\n", c.ConsistencyLevel.CommonConsistencyLevel().String())
	if c.Schema != nil {
		fmt.Fprintln(out, "Fields:")
		for _, f := range c.Schema.Fields {
			tags := []string{f.DataType.String()}
			if f.PrimaryKey {
				tags = append(tags, "pk")
			}
			if f.AutoID {
				tags = append(tags, "auto_id")
			}
			if f.Nullable {
				tags = append(tags, "nullable")
			}
			if dim, ok := f.TypeParams["dim"]; ok {
				tags = append(tags, "dim="+dim)
			}
			if ml, ok := f.TypeParams["max_length"]; ok {
				tags = append(tags, "max_length="+ml)
			}
			fmt.Fprintf(out, "  - %-20s [%s]\n", f.Name, strings.Join(tags, ","))
		}
	}
}

func init() {
	Register("describe_collection", func() Op { return &DescribeCollectionParams{} })
}
