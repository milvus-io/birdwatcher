package show

import (
	"context"
	"fmt"
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/birdwatcher/framework"
	"github.com/milvus-io/birdwatcher/proto/v2.2/datapb"
	"github.com/milvus-io/birdwatcher/proto/v2.2/internalpb"
	"github.com/milvus-io/birdwatcher/states/etcd/common"
	"github.com/milvus-io/birdwatcher/states/kv"
)

const printFileLimit = 3

type ImportJobParam struct {
	framework.ParamBase `use:"show bulkinsert" desc:"display bulkinsert jobs and tasks" alias:"import"`

	JobID        int64  `name:"job" default:"0" desc:"job id to filter with"`
	CollectionID int64  `name:"collection" default:"0" desc:"collection id to filter with"`
	State        string `name:"state" default:"" desc:"target import job state, [pending, preimporting, importing, failed, completed]"`
	Detail       bool   `name:"detail" default:"false" desc:"flags indicating whether printing detail bulkinsert job"`
	ShowAllFiles bool   `name:"showAllFiles" default:"false" desc:"flags indicating whether printing all files"`
}

// BulkInsertCommand returns show bulkinsert command.
func (c *ComponentShow) BulkInsertCommand(ctx context.Context, p *ImportJobParam) error {
	jobs, _, err := common.ListImportJobs(ctx, c.client, c.metaPath, func(job *datapb.ImportJob) bool {
		return (p.JobID == 0 || job.GetJobID() == p.JobID) &&
			(p.CollectionID == 0 || job.GetCollectionID() == p.CollectionID) &&
			(p.State == "" || strings.EqualFold(job.GetState().String(), p.State))
	})
	if err != nil {
		fmt.Println("failed to list bulkinsert jobs, err=", err.Error())
		return nil
	}

	countMap := make(map[string]int)
	collectionID2Jobs := lo.GroupBy(jobs, func(job *datapb.ImportJob) int64 {
		return job.GetCollectionID()
	})

	for _, colJobs := range collectionID2Jobs {
		for _, job := range colJobs {
			countMap[job.GetState().String()]++
			if p.Detail {
				if p.JobID == 0 {
					fmt.Println("Please specify the job ID (-job={JobID}) to show detailed info.")
					return nil
				}
				PrintDetailedImportJob(ctx, c.client, c.metaPath, job, p.ShowAllFiles)
			} else {
				PrintSimpleImportJob(job)
			}
		}
		fmt.Printf("\n")
	}

	total := lo.SumBy(lo.Values(countMap), func(count int) int {
		return count
	})
	str := fmt.Sprintf("--- Total: %d", total)
	for state, count := range countMap {
		str = fmt.Sprintf("%s %s:%d", str, state, count)
	}
	fmt.Println(str)
	return nil
}

func PrintSimpleImportJob(job *datapb.ImportJob) {
	str := fmt.Sprintf("JobID: %d DBID: %d CollectionID: %d State: %s StartTime: %s",
		job.GetJobID(), job.GetDbID(), job.GetCollectionID(), job.State.String(), job.GetStartTime())
	if job.GetState() == internalpb.ImportJobState_Failed {
		str = fmt.Sprintf("%s Reason: %s", str, job.GetReason())
	}
	if job.GetState() == internalpb.ImportJobState_Completed {
		str = fmt.Sprintf("%s CompleteTime: %s", str, job.GetCompleteTime())
	}
	fmt.Println(str)
}

func PrintDetailedImportJob(ctx context.Context, client kv.MetaKV, basePath string, job *datapb.ImportJob, showAllFiles bool) {
	// Get job's tasks.
	preimportTasks, err := common.ListPreImportTasks(ctx, client, basePath, func(task *datapb.PreImportTask) bool {
		return task.GetJobID() == job.GetJobID()
	})
	if err != nil {
		fmt.Println("failed to list preimport tasks, err=", err.Error())
		return
	}
	importTasks, err := common.ListImportTasks(ctx, client, basePath, func(task *datapb.ImportTaskV2) bool {
		return task.GetJobID() == job.GetJobID()
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
	fmt.Printf("Start Time           : %s\n", job.GetStartTime())
	fmt.Printf("Complete Time        : %s\n", job.GetCompleteTime())
	fmt.Printf("Timeout TS           : %d\n", job.GetTimeoutTs())
	fmt.Printf("Cleanup TS           : %d\n", job.GetCleanupTs())
	fmt.Printf("Requested Disk Size  : %d MB\n", job.GetRequestedDiskSize())
	fmt.Printf("Options              : %v\n", common.KVListMap(job.GetOptions()))
	printFiles(job.Files, showAllFiles)

	fmt.Println("\n--------- Pre-Import Tasks ---------")
	for i, task := range preimportTasks {
		fmt.Printf("\n[%d] %s\n", i+1, strings.Repeat("-", 30))
		PrintPreImportTask(task, showAllFiles)
	}

	fmt.Println("\n--------- Import Tasks ---------")
	for i, task := range importTasks {
		fmt.Printf("\n[%d] %s\n", i+1, strings.Repeat("-", 30))
		PrintImportTask(task, showAllFiles)
	}

	fmt.Println("===================================")
}

func PrintPreImportTask(task *datapb.PreImportTask, showAllFiles bool) {
	fmt.Printf("TaskID               : %d\n", task.GetTaskID())
	fmt.Printf("NodeID               : %d\n", task.GetNodeID())
	fmt.Printf("State                : %s\n", task.GetState())
	fmt.Printf("Reason               : %s\n", task.GetReason())
	printFileStats(task.GetFileStats(), showAllFiles)
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
