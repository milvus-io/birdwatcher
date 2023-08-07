package states

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/storage"
)

type ParseIndexParam struct {
	framework.ParamBase `use:"parse-indexparam [file]" desc:"parse index params"`
	filePath            string
}

func (p *ParseIndexParam) ParseArgs(args []string) error {
	if len(args) != 1 {
		return errors.New("should provide only one file path")
	}
	p.filePath = args[0]
	return nil
}

// ParseIndexParamCommand parses index params from file.
func (app *ApplicationState) ParseIndexParamCommand(ctx context.Context, p *ParseIndexParam) error {
	f, err := openBackupFile(p.filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	r, evt, err := storage.NewIndexReader(f)
	if err != nil {
		return err
	}
	extra := make(map[string]any)
	json.Unmarshal(evt.ExtraBytes, &extra)
	key := extra["key"].(string)
	if key != "indexParams" && key != "SLICE_META" {
		fmt.Println("index data file found", extra)
		return nil
	}
	data, err := r.NextEventReader(f, evt.PayloadDataType)
	if err != nil {
		return err
	}

	if len(data) != 1 {
		fmt.Println("event data length is not 1")
		return nil
	}

	switch key {
	case "indexParams":
		params := make(map[string]string)
		json.Unmarshal(data[0], &params)
		fmt.Println(params)
	case "SLICE_META":
		fmt.Println(string(data[0]))
	}
	return nil
}

type ValidateIndexParam struct {
	framework.ParamBase `use:"validate-indexfiles [directory]" desc:"validate index file size"`
	directory           string
}

func (p *ValidateIndexParam) ParseArgs(args []string) error {
	if len(args) != 1 {
		return errors.New("should provide only one folder path")
	}
	p.directory = args[0]
	return nil
}

func (app *ApplicationState) ValidateIndexFilesCommand(ctx context.Context, p *ValidateIndexParam) error {
	folder := p.directory
	if err := testFolder(folder); err != nil {
		return err
	}

	filepath.WalkDir(folder, func(fp string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			idxParam := path.Join(fp, "indexParams")
			if info, err := os.Stat(idxParam); err == nil {
				if !info.IsDir() {
					bs, err := readIndexFile(idxParam, func(key string) bool {
						return key == "indexParams"
					})
					if err != nil {
						return nil
					}
					params := make(map[string]string)
					json.Unmarshal(bs, &params)
					indexType := params["index_type"]
					fmt.Printf("Path:[%s] IndexParam file found, index type is %s\n", fp, indexType)
					validateIndexFolder(fp, params)
				}
			} else if errors.Is(err, os.ErrNotExist) {
				// file not exist
			} else {
				fmt.Println(err.Error())
			}
		}
		return nil
	})
	return nil
}

func validateIndexFolder(fp string, params map[string]string) {
	fmt.Println(params)
	indexType := params["index_type"]
	var indexSize int64
	var dataSize int64
	filepath.WalkDir(fp, func(file string, d os.DirEntry, err error) error {
		switch indexType {
		case "":
			fallthrough
		case "STL_SORT":
			switch d.Name() {
			case "index_length":
				bs, err := readIndexFile(file, func(key string) bool { return key == "index_length" })
				if err != nil {
					fmt.Println(err.Error())
					return nil
				}
				indexSize = int64(binary.LittleEndian.Uint64(bs))
			case "index_data":
				bs, err := readIndexFile(file, func(key string) bool { return key == "index_data" })
				if err != nil {
					fmt.Println(err.Error())
					return nil
				}
				dataSize = int64(len(bs))
			}
		case "Trie":
			switch d.Name() {
			case "marisa_trie_index":
				bs, err := readIndexFile(file, func(key string) bool { return key == "marisa_trie_index" })
				if err != nil {
					fmt.Println(err.Error())
					return nil
				}
				fmt.Printf("%s: size %d\n", d.Name(), len(bs))
			case "marisa_trie_str_ids":
				bs, err := readIndexFile(file, func(key string) bool { return key == "marisa_trie_str_ids" })
				if err != nil {
					fmt.Println(err.Error())
					return nil
				}
				fmt.Printf("%s: size %d\n", d.Name(), len(bs))
			}
		}

		return nil
	})

	switch indexType {
	case "":
		fallthrough
	case "STL_SORT":
		fmt.Printf("indexSize: %d, dataSize:%d, multipler: %f\n", indexSize, dataSize, float64(dataSize)/float64(indexSize))
	}
}

type AssembleIndexFilesParam struct {
	framework.ParamBase `use:"assemble-indexfiles [directory]" desc:""`
	directory           string
}

func (p *AssembleIndexFilesParam) ParseArgs(args []string) error {
	if len(args) != 1 {
		return errors.New("should provide only one folder path")
	}
	p.directory = args[0]
	return nil
}

func (app *ApplicationState) AssembleIndexFilesCommand(ctx context.Context, p *AssembleIndexFilesParam) error {
	folder := p.directory
	if err := testFolder(folder); err != nil {
		return err
	}

	sliceMetaFile := path.Join(folder, "SLICE_META")
	prefix, num, err := tryParseSliceMeta(sliceMetaFile)
	if err != nil {
		fmt.Println("failed to parse SLICE_META", err.Error())
		return err
	}

	fmt.Printf("original file name: %s, slice num: %d\n", prefix, num)

	m := make(map[int64]struct{})

	filepath.Walk(folder, func(file string, info os.FileInfo, _ error) error {
		file = path.Base(file)
		if !strings.HasPrefix(file, prefix+"_") {
			fmt.Println("skip file", file)
			return nil
		}

		suffix := file[len(prefix)+1:]
		idx, err := strconv.ParseInt(suffix, 10, 64)
		if err != nil {
			fmt.Println(err.Error())
			return nil
		}

		m[idx] = struct{}{}
		return nil
	})
	if len(m) != num {
		fmt.Println("slice files not complete", m)
		return nil
	}

	outputPath := fmt.Sprintf("%s_%s", prefix, time.Now().Format("060102150406"))
	output, err := os.OpenFile(outputPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer output.Close()
	totalLen := int64(0)

	for i := 0; i < num; i++ {
		key := fmt.Sprintf("%s_%d", prefix, i)
		fmt.Print("processing file:", key)
		data, err := readIndexFile(path.Join(folder, key), func(metaKey string) bool {
			return metaKey == key
		})
		if err != nil {
			return err
		}
		fmt.Println("read data size:", len(data), hrSize(int64(len(data))))

		_, err = output.Write(data)
		if err != nil {
			return err
		}
		totalLen += int64(len(data))
	}
	fmt.Printf("index file write to %s success, total len %d\n", outputPath, totalLen)
	return nil
}

func hrSize(size int64) string {
	sf := float64(size)
	units := []string{"Bytes", "KB", "MB", "GB"}
	idx := 0
	for sf > 1024.0 && idx < 3 {
		sf /= 1024.0
		idx++
	}
	return fmt.Sprintf("%f %s", sf, units[idx])
}

func tryParseSliceMeta(file string) (string, int, error) {
	data, err := readIndexFile(file, func(key string) bool {
		if key != "SLICE_META" {
			fmt.Println("failed meta indicates file content not SLICE_META but", key)
			return false
		}
		return true
	})
	if err != nil {
		fmt.Println(err.Error())
		return "", 0, err
	}
	meta := &SliceMeta{}
	raw := bytes.Trim(data, "\x00")
	err = json.Unmarshal(raw, meta)
	if err != nil {
		fmt.Println("failed to unmarshal", err.Error())
		return "", 0, err
	}

	if len(meta.Meta) != 1 {
		return "", 0, errors.Newf("slice_meta item is not 1 but %d", len(meta.Meta))
	}

	fmt.Printf("SLICE_META total_num parsed: %d\n", meta.Meta[0].TotalLength)
	return meta.Meta[0].Name, meta.Meta[0].SliceNum, nil
}

type SliceMeta struct {
	Meta []struct {
		Name        string `json:"name"`
		SliceNum    int    `json:"slice_num"`
		TotalLength int64  `json:"total_len"`
	} `json:"meta"`
}

func readIndexFile(file string, validKey func(key string) bool) ([]byte, error) {
	if err := testFile(file); err != nil {
		fmt.Println("failed to test file", file)
		return nil, err
	}

	f, err := openBackupFile(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r, evt, err := storage.NewIndexReader(f)
	if err != nil {
		return nil, err
	}
	extra := make(map[string]any)
	json.Unmarshal(evt.ExtraBytes, &extra)
	key := extra["key"].(string)
	if !validKey(key) {
		return nil, errors.New("file meta key not valid")
	}

	data, err := r.NextEventReader(f, evt.PayloadDataType)
	if err != nil {
		return nil, err
	}
	if len(data) != 1 {
		return nil, errors.Newf("index file suppose to contain only one block but got %d", len(data))
	}

	return data[0], nil
}
