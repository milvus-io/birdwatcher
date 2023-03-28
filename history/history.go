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

type HistoryItem struct {
	Cmd string
	Ts  int64
}

func NewHistoryHelper(filePath string) *HistoryHelper {
	filePath = path.Join(filePath, ".bw_history")
	// read all
	readFile, err := os.Open(filePath)
	var lines []HistoryItem
	if err == nil {
		fileScanner := bufio.NewScanner(readFile)

		fileScanner.Split(bufio.ScanLines)

		for fileScanner.Scan() {
			bs := fileScanner.Bytes()
			hi := HistoryItem{}
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

	return &HistoryHelper{
		hFile: hFile,
		items: lines,
	}
}

// HistoryHelper command history helper.
type HistoryHelper struct {
	items []HistoryItem
	hFile *os.File
}

// AddLog add cmd log into history helper.
func (h *HistoryHelper) AddLog(cmd string) {
	// skip empty line
	if len(strings.TrimSpace(cmd)) == 0 {
		return
	}
	hi := HistoryItem{
		Ts:  time.Now().Unix(),
		Cmd: cmd,
	}
	if h.hFile != nil {
		bs, _ := json.Marshal(hi)
		h.hFile.Write(bs)
		h.hFile.WriteString("\n")
	}
	h.items = append(h.items, HistoryItem{
		Ts:  time.Now().Unix(),
		Cmd: cmd,
	})
}

// List all history items with prefix.
func (h *HistoryHelper) List(input string) []HistoryItem {
	return lo.Filter(h.items, func(item HistoryItem, _ int) bool {
		return strings.HasPrefix(item.Cmd, input)
	})
}

func (h *HistoryHelper) Close() {
	if h.hFile != nil {
		h.hFile.Close()
	}
}
