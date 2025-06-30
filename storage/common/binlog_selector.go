package common

import (
	"io"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/models"
)

type BinlogSelector interface {
	SelectBinlogs(binlogs []*models.FieldBinlog, batchIdx int) (map[int64]string, error)
}

type FieldIDSelector struct {
	selectedFields []int64
	set            map[int64]struct{}
}

func (s *FieldIDSelector) SelectBinlogs(binlogs []*models.FieldBinlog, batchIdx int) (map[int64]string, error) {
	result := make(map[int64]string)
	for _, binlog := range binlogs {
		if batchIdx >= len(binlog.Binlogs) {
			return nil, io.EOF
		}
		if _, ok := s.set[binlog.FieldID]; !ok {
			continue
		}
		result[binlog.FieldID] = binlog.Binlogs[batchIdx].LogPath
	}
	return result, nil
}

func NewFieldIDSelector(selectIDs []int64) *FieldIDSelector {
	s := &FieldIDSelector{
		selectedFields: selectIDs,
	}
	s.set = lo.SliceToMap(s.selectedFields, func(fieldID int64) (int64, struct{}) {
		return fieldID, struct{}{}
	})

	return s
}

// AllSelector currently, no selection could be made based on etcdmeta alone
type AllSelector struct{}

func (s *AllSelector) SelectBinlogs(binlogs []*models.FieldBinlog, batchIdx int) (map[int64]string, error) {
	result := make(map[int64]string)
	for _, binlog := range binlogs {
		if batchIdx >= len(binlog.Binlogs) {
			return nil, io.EOF
		}
		result[binlog.FieldID] = binlog.Binlogs[batchIdx].LogPath
	}
	return result, nil
}

func NewAllSelector() *AllSelector {
	return &AllSelector{}
}
