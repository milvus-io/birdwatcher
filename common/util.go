package common

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/milvus-io/birdwatcher/framework"
)

const (
	logicalBits     = 18
	logicalBitsMask = (1 << logicalBits) - 1
)

type ParseTSParam struct {
	framework.ParamBase `use:"parse-ts" desc:"parse hybrid timestamp"`
	args                []string
}

func (p *ParseTSParam) ParseArgs(args []string) error {
	p.args = args
	return nil
}

func (s *CmdState) ParseTSCommand(ctx context.Context, p *ParseTSParam) {
	if len(p.args) == 0 {
		fmt.Println("no ts provided")
	}

	for _, arg := range p.args {
		ts, err := strconv.ParseUint(arg, 10, 64)
		if err != nil {
			fmt.Printf("failed to parse ts from %s, err: %s\n", arg, err.Error())
			continue
		}

		t, _ := ParseTS(ts)
		fmt.Printf("Parse ts result, ts:%d, time: %v\n", ts, t)
	}
}

func ParseTS(ts uint64) (time.Time, uint64) {
	logical := ts & logicalBitsMask
	physical := ts >> logicalBits
	physicalTime := time.Unix(int64(physical/1000), int64(physical)%1000*time.Millisecond.Nanoseconds())
	return physicalTime, logical
}
