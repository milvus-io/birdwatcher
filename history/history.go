package history

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	"github.com/samber/lo"
)

type Item struct {
	Cmd string
	Ts  int64
}

func NewHistoryHelper(filePath string) *Helper {
	filePath = path.Join(filePath, ".bw_history")
	// read all
	readFile, err := os.Open(filePath)
	var lines []Item
	if err == nil {
		fileScanner := bufio.NewScanner(readFile)

		fileScanner.Split(bufio.ScanLines)

		for fileScanner.Scan() {
			bs := fileScanner.Bytes()
			hi := Item{}
			err := json.Unmarshal(bs, &hi)
			if err == nil {
				lines = append(lines, hi)
			}
		}

		readFile.Close()
	}
	// open file and create if non-existent
	hFile, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("[WARN] failed to open history file", err.Error())
	}

	return &Helper{
		hFile: hFile,
		items: lines,
	}
}

// Helper command history helper.
type Helper struct {
	items []Item
	hFile *os.File
}

// AddLog add cmd log into history helper.
func (h *Helper) AddLog(cmd string) {
	// skip empty line
	if len(strings.TrimSpace(cmd)) == 0 {
		return
	}
	hi := Item{
		Ts:  time.Now().Unix(),
		Cmd: cmd,
	}
	if h.hFile != nil {
		bs, _ := json.Marshal(hi)
		h.hFile.Write(bs)
		h.hFile.WriteString("\n")
	}
	h.items = append(h.items, Item{
		Ts:  time.Now().Unix(),
		Cmd: cmd,
	})
}

// List all history items with prefix.
func (h *Helper) List(input string) []Item {
	return lo.Filter(h.items, func(item Item, _ int) bool {
		return strings.HasPrefix(item.Cmd, input)
	})
}

func (h *Helper) Close() {
	if h.hFile != nil {
		h.hFile.Close()
	}
}
