package ops

// import (
// 	"context"
// 	"fmt"

// 	"github.com/milvus-io/birdwatcher/internal/client"
// )

// // ConnectParams is the one op that mutates RunContext: it builds a new
// // Milvus client and stashes it on the context so subsequent steps can use
// // it. If the context already held a client, it is closed first and the new
// // one is marked `OwnsClient` so the runner closes it on exit.
// type ConnectParams struct {
// 	Address  string `yaml:"address"`
// 	Username string `yaml:"username,omitempty"`
// 	Password string `yaml:"password,omitempty"`
// 	APIKey   string `yaml:"api_key,omitempty"`
// 	DBName   string `yaml:"db_name,omitempty"`
// 	TLS      bool   `yaml:"tls,omitempty"`
// }

// func (p *ConnectParams) Execute(ctx context.Context, rc *RunContext) (any, error) {
// 	if p.Address == "" {
// 		return nil, fmt.Errorf("connect: `address` required")
// 	}
// 	if rc.Client != nil && rc.OwnsClient {
// 		_ = rc.Client.Close(ctx)
// 	}
// 	cli, err := client.New(ctx, client.Config{
// 		Address:  p.Address,
// 		Username: p.Username,
// 		Password: p.Password,
// 		APIKey:   p.APIKey,
// 		DBName:   p.DBName,
// 		TLS:      p.TLS,
// 	})
// 	if err != nil {
// 		return nil, err
// 	}
// 	rc.Client = cli
// 	rc.OwnsClient = true
// 	fmt.Fprintf(rc.Out(), "connected to %s\n", p.Address)
// 	return map[string]any{"address": p.Address}, nil
// }

// func init() {
// 	Register("connect", func() Op { return &ConnectParams{} })
// }
