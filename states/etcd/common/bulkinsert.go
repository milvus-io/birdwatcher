package common

import (
	"context"
	"path"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	"github.com/milvus-io/birdwatcher/states/kv"
)

const (
	ImportJobPrefix     = "datacoord-meta/import-job"
	PreImportTaskPrefix = "datacoord-meta/preimport-task"
	ImportTaskPrefix    = "datacoord-meta/import-task"
)

// ListImportJobs list import jobs.
func ListImportJobs(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(*datapb.ImportJob) bool) ([]*datapb.ImportJob, error) {
	prefix := path.Join(basePath, ImportJobPrefix) + "/"
	jobs, _, err := ListProtoObjects[datapb.ImportJob](ctx, cli, prefix)
	if err != nil {
		return nil, err
	}

	return lo.FilterMap(jobs, func(job datapb.ImportJob, idx int) (*datapb.ImportJob, bool) {
		for _, filter := range filters {
			if !filter(&job) {
				return nil, false
			}
		}
		return &job, true
	}), nil
}

// ListPreImportTasks list pre-import tasks.
func ListPreImportTasks(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(v2 *datapb.PreImportTask) bool) ([]*datapb.PreImportTask, error) {
	prefix := path.Join(basePath, PreImportTaskPrefix) + "/"
	tasks, _, err := ListProtoObjects[datapb.PreImportTask](ctx, cli, prefix)
	if err != nil {
		return nil, err
	}

	return lo.FilterMap(tasks, func(task datapb.PreImportTask, idx int) (*datapb.PreImportTask, bool) {
		for _, filter := range filters {
			if !filter(&task) {
				return nil, false
			}
		}
		return &task, true
	}), nil
}

// ListImportTasks list import tasks.
func ListImportTasks(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(v2 *datapb.ImportTaskV2) bool) ([]*datapb.ImportTaskV2, error) {
	prefix := path.Join(basePath, ImportTaskPrefix) + "/"
	tasks, _, err := ListProtoObjects[datapb.ImportTaskV2](ctx, cli, prefix)
	if err != nil {
		return nil, err
	}

	return lo.FilterMap(tasks, func(task datapb.ImportTaskV2, idx int) (*datapb.ImportTaskV2, bool) {
		for _, filter := range filters {
			if !filter(&task) {
				return nil, false
			}
		}
		return &task, true
	}), nil
}
