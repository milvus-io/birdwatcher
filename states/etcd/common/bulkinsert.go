package common

import (
	"context"
	"path"

	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

const (
	ImportJobPrefix     = "datacoord-meta/import-job"
	PreImportTaskPrefix = "datacoord-meta/preimport-task"
	ImportTaskPrefix    = "datacoord-meta/import-task"
)

// ListImportJobs list import jobs.
func ListImportJobs(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(*models.ImportJob) bool) ([]*models.ImportJob, error) {
	prefix := path.Join(basePath, ImportJobPrefix) + "/"
	return ListObj2Models(ctx, cli, prefix, models.NewProtoWrapper[*datapb.ImportJob], filters...)
}

// ListPreImportTasks list pre-import tasks.
func ListPreImportTasks(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(v2 *models.PreImportTask) bool) ([]*models.PreImportTask, error) {
	prefix := path.Join(basePath, PreImportTaskPrefix) + "/"
	return ListObj2Models(ctx, cli, prefix, models.NewProtoWrapper[*datapb.PreImportTask], filters...)
}

// ListImportTasks list import tasks.
func ListImportTasks(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(v2 *models.ImportTaskV2) bool) ([]*models.ImportTaskV2, error) {
	prefix := path.Join(basePath, ImportTaskPrefix) + "/"
	return ListObj2Models(ctx, cli, prefix, models.NewProtoWrapper[*datapb.ImportTaskV2], filters...)
}

// ListImportJobs list import jobs.
// func ListImportJobs(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(*datapb.ImportJob) bool) ([]*datapb.ImportJob, []string, error) {
// 	prefix := path.Join(basePath, ImportJobPrefix) + "/"
// 	jobs, keys, err := ListProtoObjects[datapb.ImportJob](ctx, cli, prefix)
// 	if err != nil {
// 		return nil, nil, err
// 	}

// 	resultJobs := make([]*datapb.ImportJob, 0, len(jobs))
// 	resultKeys := make([]string, 0, len(keys))

// 	filterFn := func(job datapb.ImportJob) bool {
// 		for _, filter := range filters {
// 			if !filter(&job) {
// 				return false
// 			}
// 		}
// 		return true
// 	}
// 	for i, job := range jobs {
// 		if ok := filterFn(job); ok {
// 			resultJobs = append(resultJobs, &jobs[i])
// 			resultKeys = append(resultKeys, keys[i])
// 		}
// 	}

// 	return resultJobs, resultKeys, nil
// }

// ListPreImportTasks list pre-import tasks.
// func ListPreImportTasks(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(v2 *datapb.PreImportTask) bool) ([]*datapb.PreImportTask, error) {
// 	prefix := path.Join(basePath, PreImportTaskPrefix) + "/"
// 	tasks, _, err := ListProtoObjects[datapb.PreImportTask](ctx, cli, prefix)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return lo.FilterMap(tasks, func(task datapb.PreImportTask, idx int) (*datapb.PreImportTask, bool) {
// 		for _, filter := range filters {
// 			if !filter(&task) {
// 				return nil, false
// 			}
// 		}
// 		return &task, true
// 	}), nil
// }

// ListImportTasks list import tasks.
// func ListImportTasks(ctx context.Context, cli kv.MetaKV, basePath string, filters ...func(v2 *datapb.ImportTaskV2) bool) ([]*datapb.ImportTaskV2, error) {
// 	prefix := path.Join(basePath, ImportTaskPrefix) + "/"
// 	tasks, _, err := ListProtoObjects[datapb.ImportTaskV2](ctx, cli, prefix)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return lo.FilterMap(tasks, func(task datapb.ImportTaskV2, idx int) (*datapb.ImportTaskV2, bool) {
// 		for _, filter := range filters {
// 			if !filter(&task) {
// 				return nil, false
// 			}
// 		}
// 		return &task, true
// 	}), nil
// }
