package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"

	binlogv1 "github.com/milvus-io/birdwatcher/storage/binlog/v1"
	"github.com/milvus-io/birdwatcher/storage/common"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// ReadDeltalogRecords reads every event/batch in a legacy binlog or a v2/v3
// Parquet deltalog. It owns and closes source, including on decoding errors.
func ReadDeltalogRecords(ctx context.Context, source common.ReadSeeker, pkType schemapb.DataType, add func(any, uint64)) (total int64, err error) {
	end := common.ScanOperation(ctx, "deltalog.decode pk_type=%s", pkType)
	defer func() { end(err, fmt.Sprintf("rows=%d", total)) }()
	if closer, ok := source.(io.Closer); ok {
		defer closer.Close()
	}
	if pkType != schemapb.DataType_Int64 && pkType != schemapb.DataType_VarChar {
		return 0, fmt.Errorf("unsupported deltalog primary key type %s", pkType)
	}
	var header [4]byte
	if _, err := io.ReadFull(source, header[:]); err != nil {
		return 0, err
	}
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	if string(header[:]) == "PAR1" {
		common.ScanDebug(ctx, "deltalog.format packed_parquet")
		// Keep Close ownership here; parquet.Reader.Close otherwise also closes
		// the source and would close it twice.
		pq, err := file.NewParquetReader(struct{ common.ReadSeeker }{source})
		if err != nil {
			return 0, err
		}
		defer pq.Close()
		return readDeleteParquet(ctx, pq, pkType, add)
	}
	common.ScanDebug(ctx, "deltalog.format legacy_binlog")
	br, err := binlogv1.NewBinlogReader(source)
	if err != nil {
		return 0, err
	}
	defer br.Close()
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		pq, err := br.NextParquetReader()
		if err == io.EOF {
			return total, nil
		}
		if err != nil {
			return total, err
		}
		n, err := readDeleteParquet(ctx, pq, pkType, add)
		pq.Close()
		total += n
		if err != nil {
			return total, err
		}
	}
}

func readDeleteParquet(ctx context.Context, pq *file.Reader, pkType schemapb.DataType, add func(any, uint64)) (int64, error) {
	ar, err := pqarrow.NewFileReader(pq, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
	if err != nil {
		return 0, err
	}
	rr, err := ar.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return 0, err
	}
	defer rr.Release()
	pkIndex, tsIndex := 0, 1
	if len(rr.Schema().Fields()) == 2 {
		pkIndex, tsIndex = -1, -1
		for i, field := range rr.Schema().Fields() {
			switch field.Name {
			case "0", "pk":
				pkIndex = i
			case "1", "ts":
				tsIndex = i
			}
		}
		if pkIndex < 0 || tsIndex < 0 {
			return 0, fmt.Errorf("deltalog is missing PK/timestamp columns: %s", rr.Schema())
		}
	}
	var total int64
	for common.NextScanRecord(ctx, rr, "deltalog.arrow.next rows_decoded=%d", total) {
		if err := ctx.Err(); err != nil {
			return total, err
		}
		rec := rr.Record()
		common.ScanDebug(ctx, "deltalog.batch rows=%d columns=%d", rec.NumRows(), rec.NumCols())
		if rec.NumCols() != 1 && rec.NumCols() != 2 {
			return total, fmt.Errorf("deltalog requires JSON or PK/timestamp columns, got %d", rec.NumCols())
		}
		for row := 0; row < int(rec.NumRows()); row++ {
			if rec.Column(pkIndex).IsNull(row) {
				return total, fmt.Errorf("null deltalog primary key at row %d", total)
			}
			if rec.NumCols() == 1 {
				var data []byte
				switch col := rec.Column(0).(type) {
				case *array.String:
					data = []byte(col.Value(row))
				case *array.Binary:
					data = col.Value(row)
				default:
					return total, fmt.Errorf("unsupported JSON deltalog column type %s", col.DataType())
				}
				var log DeleteLog
				if err := json.Unmarshal(data, &log); err != nil {
					return total, err
				}
				if log.PkType != int64(pkType) {
					return total, fmt.Errorf("deltalog primary key type %d does not match %s", log.PkType, pkType)
				}
				add(log.Pk.GetValue(), log.Ts)
			} else {
				pk, ok := DeserializeItem(rec.Column(pkIndex), pkType, row)
				if !ok {
					return total, fmt.Errorf("invalid deltalog primary key column type %s", rec.Column(pkIndex).DataType())
				}
				ts, ok := rec.Column(tsIndex).(*array.Int64)
				if !ok || ts.IsNull(row) {
					return total, fmt.Errorf("invalid deltalog timestamp column")
				}
				if value, ok := pk.(string); ok {
					pk = strings.Clone(value)
				}
				add(pk, uint64(ts.Value(row)))
			}
			total++
		}
	}
	if err := rr.Err(); err != nil && err != io.EOF {
		return total, err
	}
	return total, nil
}
