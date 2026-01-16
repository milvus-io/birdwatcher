package show

import (
	"context"
	"fmt"
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/models"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

const printFileLimit = 3

type ImportJobParam struct {
	framework.ParamBase `use:"show bulkinsert" desc:"display bulkinsert jobs and tasks" alias:"import"`

	JobID        int64  `name:"job" default:"0" desc:"job id to filter with"`
	CollectionID int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	State        string `name:"state" default:"" desc:"target import job state, [pending, preimporting, importing, failed, completed]"`
	Detail       bool   `name:"detail" default:"false" desc:"flags indicating whether printing detail bulkinsert job"`
	ShowAllFiles bool   `name:"showAllFiles" default:"false" desc:"flags indicating whether printing all files"`
	Format       string `name:"format" default:"" desc:"output format (default, json)"`
}

// BulkInsertCommand returns show bulkinsert command.
func (c *ComponentShow) BulkInsertCommand(ctx context.Context, p *ImportJobParam) (*framework.PresetResultSet, error) {
	jobs, err := common.ListImportJobs(ctx, c.client, c.metaPath, func(job *models.ImportJob) bool {
		proto := job.GetProto()
		return (p.JobID == 0 || proto.GetJobID() == p.JobID) &&
			(p.CollectionID == 0 || proto.GetCollectionID() == p.CollectionID) &&
			(p.State == "" || strings.EqualFold(proto.GetState().String(), p.State))
	})
	if err != nil {
		return nil, err
	}

	rs := &ImportJobs{
		jobs:         jobs,
		detail:       p.Detail,
		showAllFiles: p.ShowAllFiles,
		ctx:          ctx,
		client:       c.client,
		metaPath:     c.metaPath,
	}

	return framework.NewPresetResultSet(rs, framework.NameFormat(p.Format)), nil
}

type ImportJobs struct {
	jobs         []*models.ImportJob
	detail       bool
	showAllFiles bool
	ctx          context.Context
	client       kv.MetaKV
	metaPath     string
}

func (rs *ImportJobs) Entities() any {
	return rs.jobs
}

func (rs *ImportJobs) PrintAs(format framework.Format) string {
	switch format {
	case framework.FormatDefault, framework.FormatPlain:
		return rs.printDefault()
	case framework.FormatJSON:
		return rs.printAsJSON()
	}
	return ""
}

func (rs *ImportJobs) printDefault() string {
	sb := &strings.Builder{}

	countMap := make(map[string]int)
	collectionID2Jobs := lo.GroupBy(rs.jobs, func(job *models.ImportJob) int64 {
		return job.GetProto().GetCollectionID()
	})

	for _, colJobs := range collectionID2Jobs {
		for _, job := range colJobs {
			countMap[job.GetProto().GetState().String()]++
			if rs.detail {
				if len(rs.jobs) > 1 {
					fmt.Fprintln(sb, "Please specify the job ID (-job={JobID}) to show detailed info.")
					break
				}
				printDetailedImportJobToBuilder(rs.ctx, sb, rs.client, rs.metaPath, job.GetProto(), rs.showAllFiles)
			} else {
				printSimpleImportJobToBuilder(sb, job.GetProto())
			}
		}
		fmt.Fprintln(sb)
	}

	total := lo.SumBy(lo.Values(countMap), func(count int) int {
		return count
	})
	str := fmt.Sprintf("--- Total: %d", total)
	for state, count := range countMap {
		str = fmt.Sprintf("%s %s:%d", str, state, count)
	}
	fmt.Fprintln(sb, str)
	return sb.String()
}

func (rs *ImportJobs) printAsJSON() string {
	type ImportJobJSON struct {
		JobID          int64    `json:"job_id"`
		DBID           int64    `json:"db_id"`
		CollectionID   int64    `json:"collection_id"`
		CollectionName string   `json:"collection_name,omitempty"`
		State          string   `json:"state"`
		CreateTime     string   `json:"create_time"`
		CompleteTime   string   `json:"complete_time,omitempty"`
		Reason         string   `json:"reason,omitempty"`
		PartitionIDs   []int64  `json:"partition_ids,omitempty"`
		Vchannels      []string `json:"vchannels,omitempty"`
	}

	type OutputJSON struct {
		Jobs       []ImportJobJSON `json:"jobs"`
		Total      int             `json:"total"`
		StateCount map[string]int  `json:"state_count"`
	}

	countMap := make(map[string]int)
	output := OutputJSON{
		Jobs:       make([]ImportJobJSON, 0, len(rs.jobs)),
		StateCount: countMap,
	}

	for _, job := range rs.jobs {
		proto := job.GetProto()
		countMap[proto.GetState().String()]++

		item := ImportJobJSON{
			JobID:          proto.GetJobID(),
			DBID:           proto.GetDbID(),
			CollectionID:   proto.GetCollectionID(),
			CollectionName: proto.GetCollectionName(),
			State:          proto.GetState().String(),
			CreateTime:     proto.GetCreateTime(),
			PartitionIDs:   proto.GetPartitionIDs(),
			Vchannels:      proto.GetVchannels(),
		}

		if proto.GetState() == internalpb.ImportJobState_Failed {
			item.Reason = proto.GetReason()
		}
		if proto.GetState() == internalpb.ImportJobState_Completed {
			item.CompleteTime = proto.GetCompleteTime()
		}

		output.Jobs = append(output.Jobs, item)
	}

	output.Total = len(rs.jobs)
	output.StateCount = countMap

	return framework.MarshalJSON(output)
}

func printSimpleImportJobToBuilder(sb *strings.Builder, job *datapb.ImportJob) {
	str := fmt.Sprintf("JobID: %d DBID: %d CollectionID: %d State: %s CreateTime: %s",
		job.GetJobID(), job.GetDbID(), job.GetCollectionID(), job.State.String(), job.GetCreateTime())
	if job.GetState() == internalpb.ImportJobState_Failed {
		str = fmt.Sprintf("%s Reason: %s", str, job.GetReason())
	}
	if job.GetState() == internalpb.ImportJobState_Completed {
		str = fmt.Sprintf("%s CompleteTime: %s", str, job.GetCompleteTime())
	}
	fmt.Fprintln(sb, str)
}

func PrintSimpleImportJob(job *datapb.ImportJob) {
	str := fmt.Sprintf("JobID: %d DBID: %d CollectionID: %d State: %s CreateTime: %s",
		job.GetJobID(), job.GetDbID(), job.GetCollectionID(), job.State.String(), job.GetCreateTime())
	if job.GetState() == internalpb.ImportJobState_Failed {
		str = fmt.Sprintf("%s Reason: %s", str, job.GetReason())
	}
	if job.GetState() == internalpb.ImportJobState_Completed {
		str = fmt.Sprintf("%s CompleteTime: %s", str, job.GetCompleteTime())
	}
	fmt.Println(str)
}

func printDetailedImportJobToBuilder(ctx context.Context, sb *strings.Builder, client kv.MetaKV, basePath string, job *datapb.ImportJob, showAllFiles bool) {
	// Get job's tasks.
	preimportTasks, err := common.ListPreImportTasks(ctx, client, basePath, func(task *models.PreImportTask) bool {
		return task.GetProto().GetJobID() == job.GetJobID()
	})
	if err != nil {
		fmt.Fprintln(sb, "failed to list preimport tasks, err=", err.Error())
		return
	}
	importTasks, err := common.ListImportTasks(ctx, client, basePath, func(task *models.ImportTaskV2) bool {
		return task.GetProto().GetJobID() == job.GetJobID()
	})
	if err != nil {
		fmt.Fprintln(sb, "failed to list import tasks, err=", err.Error())
		return
	}

	fmt.Fprintln(sb, "===================================")
	fmt.Fprintln(sb, "           Import Job Details      ")
	fmt.Fprintln(sb, "===================================")
	fmt.Fprintf(sb, "Job ID               : %d\n", job.GetJobID())
	fmt.Fprintf(sb, "State                : %s\n", job.GetState())
	fmt.Fprintf(sb, "DB ID                : %d\n", job.GetDbID())
	fmt.Fprintf(sb, "Collection ID        : %d\n", job.GetCollectionID())
	fmt.Fprintf(sb, "Collection Name      : %s\n", job.GetCollectionName())
	fmt.Fprintf(sb, "Partition IDs        : %s\n", formatIntSlice(job.GetPartitionIDs()))
	fmt.Fprintf(sb, "Vchannels            : %s\n", strings.Join(job.GetVchannels(), ", "))
	fmt.Fprintf(sb, "Reason               : %s\n", job.GetReason())
	fmt.Fprintf(sb, "Create Time          : %s\n", job.GetCreateTime())
	fmt.Fprintf(sb, "Complete Time        : %s\n", job.GetCompleteTime())
	fmt.Fprintf(sb, "Timeout TS           : %d\n", job.GetTimeoutTs())
	fmt.Fprintf(sb, "Cleanup TS           : %d\n", job.GetCleanupTs())
	fmt.Fprintf(sb, "Requested Disk Size  : %d MB\n", job.GetRequestedDiskSize())
	fmt.Fprintf(sb, "Options              : %v\n", common.KVListMap(job.GetOptions()))
	printFilesToBuilder(sb, job.Files, showAllFiles)

	fmt.Fprintln(sb, "\n--------- Pre-Import Tasks ---------")
	for i, task := range preimportTasks {
		fmt.Fprintf(sb, "\n[%d] %s\n", i+1, strings.Repeat("-", 30))
		printPreImportTaskToBuilder(sb, task.GetProto(), showAllFiles)
	}

	fmt.Fprintln(sb, "\n--------- Import Tasks ---------")
	for i, task := range importTasks {
		fmt.Fprintf(sb, "\n[%d] %s\n", i+1, strings.Repeat("-", 30))
		printImportTaskToBuilder(sb, task.GetProto(), showAllFiles)
	}

	fmt.Fprintln(sb, "===================================")
}

func PrintDetailedImportJob(ctx context.Context, client kv.MetaKV, basePath string, job *datapb.ImportJob, showAllFiles bool) {
	// Get job's tasks.
	preimportTasks, err := common.ListPreImportTasks(ctx, client, basePath, func(task *models.PreImportTask) bool {
		return task.GetProto().GetJobID() == job.GetJobID()
	})
	if err != nil {
		fmt.Println("failed to list preimport tasks, err=", err.Error())
		return
	}
	importTasks, err := common.ListImportTasks(ctx, client, basePath, func(task *models.ImportTaskV2) bool {
		return task.GetProto().GetJobID() == job.GetJobID()
	})
	if err != nil {
		fmt.Println("failed to list import tasks, err=", err.Error())
		return
	}

	fmt.Println("===================================")
	fmt.Println("           Import Job Details      ")
	fmt.Println("===================================")
	fmt.Printf("Job ID               : %d\n", job.GetJobID())
	fmt.Printf("State                : %s\n", job.GetState())
	fmt.Printf("DB ID                : %d\n", job.GetDbID())
	fmt.Printf("Collection ID        : %d\n", job.GetCollectionID())
	fmt.Printf("Collection Name      : %s\n", job.GetCollectionName())
	fmt.Printf("Partition IDs        : %s\n", formatIntSlice(job.GetPartitionIDs()))
	fmt.Printf("Vchannels            : %s\n", strings.Join(job.GetVchannels(), ", "))
	fmt.Printf("Reason               : %s\n", job.GetReason())
	fmt.Printf("Create Time          : %s\n", job.GetCreateTime())
	fmt.Printf("Complete Time        : %s\n", job.GetCompleteTime())
	fmt.Printf("Timeout TS           : %d\n", job.GetTimeoutTs())
	fmt.Printf("Cleanup TS           : %d\n", job.GetCleanupTs())
	fmt.Printf("Requested Disk Size  : %d MB\n", job.GetRequestedDiskSize())
	fmt.Printf("Options              : %v\n", common.KVListMap(job.GetOptions()))
	printFiles(job.Files, showAllFiles)

	fmt.Println("\n--------- Pre-Import Tasks ---------")
	for i, task := range preimportTasks {
		fmt.Printf("\n[%d] %s\n", i+1, strings.Repeat("-", 30))
		PrintPreImportTask(task.GetProto(), showAllFiles)
	}

	fmt.Println("\n--------- Import Tasks ---------")
	for i, task := range importTasks {
		fmt.Printf("\n[%d] %s\n", i+1, strings.Repeat("-", 30))
		PrintImportTask(task.GetProto(), showAllFiles)
	}

	fmt.Println("===================================")
}

func printPreImportTaskToBuilder(sb *strings.Builder, task *datapb.PreImportTask, showAllFiles bool) {
	fmt.Fprintf(sb, "TaskID               : %d\n", task.GetTaskID())
	fmt.Fprintf(sb, "NodeID               : %d\n", task.GetNodeID())
	fmt.Fprintf(sb, "State                : %s\n", task.GetState())
	fmt.Fprintf(sb, "Reason               : %s\n", task.GetReason())
	printFileStatsToBuilder(sb, task.GetFileStats(), showAllFiles)
}

func PrintPreImportTask(task *datapb.PreImportTask, showAllFiles bool) {
	fmt.Printf("TaskID               : %d\n", task.GetTaskID())
	fmt.Printf("NodeID               : %d\n", task.GetNodeID())
	fmt.Printf("State                : %s\n", task.GetState())
	fmt.Printf("Reason               : %s\n", task.GetReason())
	printFileStats(task.GetFileStats(), showAllFiles)
}

func printImportTaskToBuilder(sb *strings.Builder, task *datapb.ImportTaskV2, showAllFiles bool) {
	fmt.Fprintf(sb, "TaskID               : %d\n", task.GetTaskID())
	fmt.Fprintf(sb, "NodeID               : %d\n", task.GetNodeID())
	fmt.Fprintf(sb, "State                : %s\n", task.GetState())
	fmt.Fprintf(sb, "Reason               : %s\n", task.GetReason())
	fmt.Fprintf(sb, "Segment IDs          : %s\n", formatIntSlice(task.GetSegmentIDs()))
	fmt.Fprintf(sb, "Complete Time        : %s\n", task.GetCompleteTime())
	printFileStatsToBuilder(sb, task.GetFileStats(), showAllFiles)
}

func PrintImportTask(task *datapb.ImportTaskV2, showAllFiles bool) {
	fmt.Printf("TaskID               : %d\n", task.GetTaskID())
	fmt.Printf("NodeID               : %d\n", task.GetNodeID())
	fmt.Printf("State                : %s\n", task.GetState())
	fmt.Printf("Reason               : %s\n", task.GetReason())
	fmt.Printf("Segment IDs          : %s\n", formatIntSlice(task.GetSegmentIDs()))
	fmt.Printf("Complete Time        : %s\n", task.GetCompleteTime())
	printFileStats(task.GetFileStats(), showAllFiles)
}

func printFilesToBuilder(sb *strings.Builder, importFiles []*internalpb.ImportFile, showAllFiles bool) {
	files := lo.Map(importFiles, func(file *internalpb.ImportFile, _ int) string {
		return fmt.Sprintf("[%s]", strings.Join(file.GetPaths(), " "))
	})
	fmt.Fprint(sb, "Files                : \n")
	if showAllFiles || len(files) <= printFileLimit {
		for _, file := range files {
			fmt.Fprintf(sb, "  - %s\n", file)
		}
	} else {
		for _, file := range files[:printFileLimit] {
			fmt.Fprintf(sb, "  - %s\n", file)
		}
		fmt.Fprintf(sb, "  ... and %d more\n", len(files)-printFileLimit)
	}
}

func printFiles(importFiles []*internalpb.ImportFile, showAllFiles bool) {
	files := lo.Map(importFiles, func(file *internalpb.ImportFile, _ int) string {
		return fmt.Sprintf("[%s]", strings.Join(file.GetPaths(), " "))
	})
	fmt.Print("Files                : \n")
	if showAllFiles || len(files) <= printFileLimit {
		for _, file := range files {
			fmt.Printf("  - %s\n", file)
		}
	} else {
		for _, file := range files[:printFileLimit] {
			fmt.Printf("  - %s\n", file)
		}
		fmt.Printf("  ... and %d more\n", len(files)-printFileLimit)
	}
}

func printFileStatsToBuilder(sb *strings.Builder, fileStats []*datapb.ImportFileStats, showAllFiles bool) {
	fmt.Fprint(sb, "Files Stats          : \n")
	printStat := func(stat *datapb.ImportFileStats) {
		fmt.Fprintf(sb, "  - File:%s FileSize:%d\n", strings.Join(stat.GetImportFile().GetPaths(), ","), stat.GetFileSize())
		fmt.Fprintf(sb, "    Rows:%d MemorySize:%d\n", stat.GetTotalRows(), stat.GetTotalMemorySize())
		fmt.Fprintf(sb, "    HashedStats:%v\n", stat.GetHashedStats())
	}
	if showAllFiles || len(fileStats) <= printFileLimit {
		for _, stat := range fileStats {
			printStat(stat)
		}
	} else {
		for _, stat := range fileStats[:printFileLimit] {
			printStat(stat)
		}
		fmt.Fprintf(sb, "  ... and %d more\n", len(fileStats)-printFileLimit)
	}
}

func printFileStats(fileStats []*datapb.ImportFileStats, showAllFiles bool) {
	fmt.Print("Files Stats          : \n")
	printStat := func(stat *datapb.ImportFileStats) {
		fmt.Printf("  - File:%s FileSize:%d\n", strings.Join(stat.GetImportFile().GetPaths(), ","), stat.GetFileSize())
		fmt.Printf("    Rows:%d MemorySize:%d\n", stat.GetTotalRows(), stat.GetTotalMemorySize())
		fmt.Printf("    HashedStats:%v\n", stat.GetHashedStats())
	}
	if showAllFiles || len(fileStats) <= printFileLimit {
		for _, stat := range fileStats {
			printStat(stat)
		}
	} else {
		for _, stat := range fileStats[:printFileLimit] {
			printStat(stat)
		}
		fmt.Printf("  ... and %d more\n", len(fileStats)-printFileLimit)
	}
}

func formatIntSlice(slice []int64) string {
	var strSlice []string
	for _, v := range slice {
		strSlice = append(strSlice, fmt.Sprintf("%d", v))
	}
	return strings.Join(strSlice, ", ")
}
