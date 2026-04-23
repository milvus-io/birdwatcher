package ops

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/client/v2/milvusclient"
)

type CreatePartitionParams struct {
	Collection string `yaml:"collection"`
	Name       string `yaml:"name"`
}

func (p *CreatePartitionParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
	if rc.Client == nil {
		return nil, errNoClient("create_partition")
	}
	if p.Collection == "" || p.Name == "" {
		return nil, fmt.Errorf("create_partition: `collection` and `name` required")
	}
	if err := rc.Client.CreatePartition(ctx, milvusclient.NewCreatePartitionOption(p.Collection, p.Name)); err != nil {
		return nil, err
	}
	fmt.Fprintf(rc.Out(), "partition %s/%s created\n", p.Collection, p.Name)
	return map[string]any{"collection": p.Collection, "name": p.Name}, nil
}

func init() {
	Register("create_partition", func() Op { return &CreatePartitionParams{} })
}
