# Import Task Removal Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add commands to view/remove individual import tasks and enhance import job removal to include associated tasks.

**Architecture:** Extend existing bulkinsert command infrastructure in show/remove modules. Use existing task listing functions from common module. Follow dry-run pattern with `--run` flag.

**Tech Stack:** Go, etcd client (v3), Milvus protobuf models, birdwatcher framework

---

## Chunk 1: Add Import-Job Alias and Show Import-Task Command

### Task 1: Add import-job Alias to Existing Command

**Files:**
- Modify: `states/etcd/show/bulkinsert.go:20-28`

- [ ] **Step 1: Update alias in ImportJobParam struct**

Location: `states/etcd/show/bulkinsert.go:21`

Change:
```go
framework.DataSetParam `use:"show bulkinsert" desc:"display bulkinsert jobs and tasks" alias:"import"`
```

To:
```go
framework.DataSetParam `use:"show bulkinsert" desc:"display bulkinsert jobs and tasks" alias:"import,import-job"`
```

- [ ] **Step 2: Verify the change**

Run: `git diff states/etcd/show/bulkinsert.go`

Expected: See alias updated from `"import"` to `"import,import-job"`

- [ ] **Step 3: Commit the alias change**

```bash
git add states/etcd/show/bulkinsert.go
git commit -s -m "feat: add import-job alias to show bulkinsert command"
```

---

### Task 2: Add Show Import-Task Command

**Files:**
- Modify: `states/etcd/show/bulkinsert.go` (add after line 52)
- Modify: `tests/e2e/commands/test_show.sh` (add test cases)

- [ ] **Step 1: Add ImportTaskParam struct**

Add after `ImportJobParam` struct definition (after line 28):

```go
type ImportTaskParam struct {
	framework.DataSetParam `use:"show import-task" desc:"display import task by task ID"`

	TaskID       int64 `name:"task" default:"0" desc:"task id to display"`
	ShowAllFiles bool  `name:"showAllFiles" default:"false" desc:"flags indicating whether printing all files"`
}
```

- [ ] **Step 2: Add ShowImportTaskCommand method**

Add after `BulkInsertCommand` method (after line 52):

```go
// ShowImportTaskCommand returns show import-task command.
func (c *ComponentShow) ShowImportTaskCommand(ctx context.Context, p *ImportTaskParam) error {
	if p.TaskID == 0 {
		fmt.Println("Error: task id is required (use --task <taskID>)")
		return nil
	}

	// Search in PreImportTasks
	preimportTasks, err := common.ListPreImportTasks(ctx, c.client, c.metaPath, func(task *models.PreImportTask) bool {
		return task.GetProto().GetTaskID() == p.TaskID
	})
	if err != nil {
		fmt.Println("failed to list preimport tasks, err=", err.Error())
		return err
	}

	// Search in ImportTaskV2
	importTasks, err := common.ListImportTasks(ctx, c.client, c.metaPath, func(task *models.ImportTaskV2) bool {
		return task.GetProto().GetTaskID() == p.TaskID
	})
	if err != nil {
		fmt.Println("failed to list import tasks, err=", err.Error())
		return err
	}

	// Handle results
	if len(preimportTasks) == 0 && len(importTasks) == 0 {
		fmt.Printf("cannot find target import task: %d\n", p.TaskID)
		return nil
	}

	// Show PreImportTasks if found
	if len(preimportTasks) > 0 {
		if len(importTasks) > 0 {
			fmt.Println("Warning: Found task in both PreImportTask and ImportTaskV2 collections")
		}
		for _, task := range preimportTasks {
			fmt.Println("===================================")
			fmt.Println("    [PreImportTask] Task Details   ")
			fmt.Println("===================================")
			PrintPreImportTask(task.GetProto(), p.ShowAllFiles)
			fmt.Println("===================================")
		}
	}

	// Show ImportTaskV2 if found
	if len(importTasks) > 0 {
		for _, task := range importTasks {
			fmt.Println("===================================")
			fmt.Println("     [ImportTaskV2] Task Details   ")
			fmt.Println("===================================")
			PrintImportTask(task.GetProto(), p.ShowAllFiles)
			fmt.Println("===================================")
		}
	}

	return nil
}
```

- [ ] **Step 3: Test the command manually**

```bash
# Build birdwatcher
go build -o birdwatcher .

# Test with non-existent task (should show not found message)
./birdwatcher
> connect --etcd localhost:2379 --rootPath by-dev
> show import-task --task 999999
```

Expected output: `cannot find target import task: 999999`

- [ ] **Step 4: Add e2e test**

Edit `tests/e2e/commands/test_show.sh`, add after existing show tests:

```bash
# Show import-task tests
test_optional "show import-task help" "show import-task --help"
test_optional "show import-task non-existent" "show import-task --task 999999"
```

- [ ] **Step 5: Run e2e test**

```bash
cd tests/e2e
./run_tests.sh
```

Expected: New tests pass (or are skipped if optional)

- [ ] **Step 6: Commit show import-task command**

```bash
git add states/etcd/show/bulkinsert.go tests/e2e/commands/test_show.sh
git commit -s -m "feat: add show import-task command

- Add ImportTaskParam struct for command parameters
- Add ShowImportTaskCommand method to display task by ID
- Auto-detect task type (PreImportTask or ImportTaskV2)
- Handle edge case where task exists in both collections
- Add e2e tests for show import-task command"
```

---

## Chunk 2: Add Remove Import-Task Command

### Task 3: Add Remove Import-Task Command

**Files:**
- Modify: `states/etcd/remove/bulkinsert.go` (add after line 57)
- Modify: `tests/e2e/commands/test_remove.sh` (add test cases)

- [ ] **Step 1: Add RemoveImportTaskParam struct**

Add after `RemoveImportJobParam` struct (after line 18):

```go
type RemoveImportTaskParam struct {
	framework.ExecutionParam `use:"remove import-task" desc:"Remove import task from datacoord meta with specified task id"`

	TaskID int64 `name:"task" default:"0" desc:"import task id to remove"`
}
```

- [ ] **Step 2: Verify show package import**

Check if the `show` package is imported in `states/etcd/remove/bulkinsert.go`. If not, add it:

```go
import (
	// ... existing imports ...
	"github.com/milvus-io/birdwatcher/states/etcd/show"
)
```

- [ ] **Step 3: Add RemoveImportTaskCommand method**

Add after `RemoveImportJobCommand` method (after line 57):

```go
func (c *ComponentRemove) RemoveImportTaskCommand(ctx context.Context, p *RemoveImportTaskParam) error {
	if p.TaskID == 0 {
		fmt.Println("Error: task id is required (use --task <taskID>)")
		return nil
	}

	// Search in PreImportTasks
	preimportTasks, err := common.ListPreImportTasks(ctx, c.client, c.basePath, func(task *models.PreImportTask) bool {
		return task.GetProto().GetTaskID() == p.TaskID
	})
	if err != nil {
		fmt.Println("failed to list preimport tasks, err=", err.Error())
		return err
	}

	// Search in ImportTaskV2
	importTasks, err := common.ListImportTasks(ctx, c.client, c.basePath, func(task *models.ImportTaskV2) bool {
		return task.GetProto().GetTaskID() == p.TaskID
	})
	if err != nil {
		fmt.Println("failed to list import tasks, err=", err.Error())
		return err
	}

	// Handle not found
	if len(preimportTasks) == 0 && len(importTasks) == 0 {
		fmt.Printf("cannot find target import task: %d\n", p.TaskID)
		return nil
	}

	// Show what will be deleted
	if len(preimportTasks) > 0 {
		if len(importTasks) > 0 {
			fmt.Println("Warning: Found task in both PreImportTask and ImportTaskV2, removing both")
		}
		for _, task := range preimportTasks {
			fmt.Printf("Selected PreImportTask, taskID: %d, key=%s\n", task.GetProto().GetTaskID(), task.Key())
			show.PrintPreImportTask(task.GetProto(), false)
		}
	}

	if len(importTasks) > 0 {
		for _, task := range importTasks {
			fmt.Printf("Selected ImportTaskV2, taskID: %d, key=%s\n", task.GetProto().GetTaskID(), task.Key())
			show.PrintImportTask(task.GetProto(), false)
		}
	}

	if !p.Run {
		return nil
	}

	// Delete PreImportTasks
	fmt.Printf("Start to delete import task(s)...\n")
	for _, task := range preimportTasks {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		err = c.client.Remove(ctx, task.Key())
		cancel()
		if err != nil {
			fmt.Printf("failed to delete PreImportTask %d, error: %s\n", task.GetProto().GetTaskID(), err.Error())
		} else {
			fmt.Printf("removed PreImportTask %d done\n", task.GetProto().GetTaskID())
		}
	}

	// Delete ImportTaskV2
	for _, task := range importTasks {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		err = c.client.Remove(ctx, task.Key())
		cancel()
		if err != nil {
			fmt.Printf("failed to delete ImportTaskV2 %d, error: %s\n", task.GetProto().GetTaskID(), err.Error())
		} else {
			fmt.Printf("removed ImportTaskV2 %d done\n", task.GetProto().GetTaskID())
		}
	}

	return nil
}
```

- [ ] **Step 4: Test the command manually (dry-run)**

```bash
# Build birdwatcher
go build -o birdwatcher .

# Test dry-run with non-existent task
./birdwatcher
> connect --etcd localhost:2379 --rootPath by-dev
> remove import-task --task 999999
```

Expected output:
```
cannot find target import task: 999999
```

Note: This uses `show.PrintPreImportTask` and `show.PrintImportTask` functions to display task details before removal.

- [ ] **Step 5: Add e2e test**

Edit `tests/e2e/commands/test_remove.sh`, add after existing remove tests:

```bash
# Remove import-task tests (dry-run only for safety)
test_optional "remove import-task dry" "remove import-task --task 999999 --run=false"
```

- [ ] **Step 6: Run e2e test**

```bash
cd tests/e2e
./run_tests.sh
```

Expected: New test passes (or skipped if optional)

- [ ] **Step 7: Commit remove import-task command**

```bash
git add states/etcd/remove/bulkinsert.go tests/e2e/commands/test_remove.sh
git commit -s -m "feat: add remove import-task command

- Add RemoveImportTaskParam struct for command parameters
- Add RemoveImportTaskCommand method to remove task by ID
- Auto-detect task type (PreImportTask or ImportTaskV2)
- Handle edge case where task exists in both collections
- Support dry-run mode (default) and actual deletion (--run)
- Add e2e tests for remove import-task command"
```

---

## Chunk 3: Enhance Remove Import-Job with Task Removal

### Task 4: Add --without-tasks Flag to Remove Import-Job

**Files:**
- Modify: `states/etcd/remove/bulkinsert.go:14-57`
- Modify: `tests/e2e/commands/test_remove.sh` (add test case)

- [ ] **Step 1: Add WithoutTasks field to RemoveImportJobParam**

Location: `states/etcd/remove/bulkinsert.go:14-18`

Update struct to:

```go
type RemoveImportJobParam struct {
	framework.ExecutionParam `use:"remove import-job" desc:"Remove import job from datacoord meta with specified job id" alias:"import"`

	JobID        int64 `name:"job" default:"" desc:"import job id to remove"`
	WithoutTasks bool  `name:"without-tasks" default:"false" desc:"remove job only, keep associated tasks"`
}
```

- [ ] **Step 2: Update RemoveImportJobCommand to handle task removal**

Note: The `show` package should already be imported from Task 3. Verify it's present in the imports.

Replace the existing `RemoveImportJobCommand` method (lines 20-57) with:

```go
func (c *ComponentRemove) RemoveImportJobCommand(ctx context.Context, p *RemoveImportJobParam) error {
	jobs, err := common.ListImportJobs(ctx, c.client, c.basePath, func(job *models.ImportJob) bool {
		return job.GetProto().GetJobID() == p.JobID
	})
	if err != nil {
		fmt.Println("failed to list bulkinsert jobs, err=", err.Error())
		return err
	}

	if len(jobs) == 0 {
		fmt.Printf("cannot find target import job: %d\n", p.JobID)
		return nil
	}
	if len(jobs) > 1 {
		fmt.Printf("unexpected import job, expect 1, but got %d\n", len(jobs))
		return nil
	}

	targetJob := jobs[0].GetProto()
	targetPath := jobs[0].Key()
	fmt.Printf("selected target import job, jobID: %d, key=%s\n", targetJob.GetJobID(), targetPath)
	show.PrintDetailedImportJob(ctx, c.client, c.basePath, targetJob, false)

	// Query associated tasks unless --without-tasks is specified
	var preimportTasks []*models.PreImportTask
	var importTasks []*models.ImportTaskV2

	if !p.WithoutTasks {
		fmt.Printf("\nQuerying associated tasks...\n")

		// Get PreImportTasks
		preimportTasks, err = common.ListPreImportTasks(ctx, c.client, c.basePath, func(task *models.PreImportTask) bool {
			return task.GetProto().GetJobID() == p.JobID
		})
		if err != nil {
			fmt.Println("failed to list preimport tasks, err=", err.Error())
			return err
		}

		// Get ImportTaskV2
		importTasks, err = common.ListImportTasks(ctx, c.client, c.basePath, func(task *models.ImportTaskV2) bool {
			return task.GetProto().GetJobID() == p.JobID
		})
		if err != nil {
			fmt.Println("failed to list import tasks, err=", err.Error())
			return err
		}

		// Display summary
		fmt.Printf("\nFound %d PreImportTask(s) and %d ImportTaskV2(s) associated with job %d\n",
			len(preimportTasks), len(importTasks), p.JobID)

		if len(preimportTasks) > 0 || len(importTasks) > 0 {
			fmt.Printf("These tasks will be removed along with the job.\n")
		}
	} else {
		fmt.Printf("\n--without-tasks specified, only the job will be removed\n")
	}

	if !p.Run {
		return nil
	}

	// Delete tasks first (if not skipped)
	if !p.WithoutTasks {
		// Delete PreImportTasks
		// Note: We continue on individual task deletion errors to maximize cleanup.
		// This resilience pattern ensures we attempt to delete all tasks and the job
		// even if some operations fail. Failures are logged and summarized.
		fmt.Printf("\nDeleting %d PreImportTask(s)...\n", len(preimportTasks))
		deletedPreImport := 0
		failedPreImport := []int64{}
		for _, task := range preimportTasks {
			taskID := task.GetProto().GetTaskID()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			err = c.client.Remove(ctx, task.Key())
			cancel()
			if err != nil {
				fmt.Printf("  Failed to delete PreImportTask %d: %s\n", taskID, err.Error())
				failedPreImport = append(failedPreImport, taskID)
			} else {
				fmt.Printf("  Deleted PreImportTask %d\n", taskID)
				deletedPreImport++
			}
		}

		// Delete ImportTaskV2
		fmt.Printf("\nDeleting %d ImportTaskV2(s)...\n", len(importTasks))
		deletedImport := 0
		failedImport := []int64{}
		for _, task := range importTasks {
			taskID := task.GetProto().GetTaskID()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
			err = c.client.Remove(ctx, task.Key())
			cancel()
			if err != nil {
				fmt.Printf("  Failed to delete ImportTaskV2 %d: %s\n", taskID, err.Error())
				failedImport = append(failedImport, taskID)
			} else {
				fmt.Printf("  Deleted ImportTaskV2 %d\n", taskID)
				deletedImport++
			}
		}

		// Print summary
		if len(failedPreImport) > 0 || len(failedImport) > 0 {
			fmt.Printf("\nTask deletion summary:\n")
			if len(failedPreImport) > 0 {
				fmt.Printf("  Failed PreImportTasks: %v\n", failedPreImport)
			}
			if len(failedImport) > 0 {
				fmt.Printf("  Failed ImportTaskV2s: %v\n", failedImport)
			}
			fmt.Printf("  Successfully deleted: %d PreImportTasks, %d ImportTaskV2s\n",
				deletedPreImport, deletedImport)
		}
	}

	// Delete the job
	fmt.Printf("\nStart to delete import job...\n")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	err = c.client.Remove(ctx, targetPath)
	cancel()
	if err != nil {
		fmt.Printf("failed to delete import job %d, error: %s\n", targetJob.GetJobID(), err.Error())
		return err
	}
	fmt.Printf("remove import job %d done\n", targetJob.GetJobID())
	return nil
}
```

- [ ] **Step 3: Test the command manually (dry-run)**

```bash
# Build birdwatcher
go build -o birdwatcher .

# Test dry-run with job that has tasks (default behavior)
./birdwatcher
> connect --etcd localhost:2379 --rootPath by-dev
> remove import-job --job 123
```

Expected output: Should show job details and list associated tasks

```bash
# Test dry-run with --without-tasks flag
> remove import-job --job 123 --without-tasks
```

Expected output: Should show only job details, mention tasks will not be removed

- [ ] **Step 4: Add e2e test**

Edit `tests/e2e/commands/test_remove.sh`, update the existing bulkinsert test:

```bash
# Update existing test
test_optional "remove bulkinsert dry" "remove bulkinsert --run=false"

# Add new test for --without-tasks flag
test_optional "remove import-job without-tasks dry" "remove import-job --job 999999 --without-tasks --run=false"
```

- [ ] **Step 5: Run e2e test**

```bash
cd tests/e2e
./run_tests.sh
```

Expected: Tests pass (or skipped if optional)

- [ ] **Step 6: Commit enhanced remove import-job command**

```bash
git add states/etcd/remove/bulkinsert.go tests/e2e/commands/test_remove.sh
git commit -s -m "feat: enhance remove import-job to remove associated tasks

- Add WithoutTasks field to RemoveImportJobParam
- Default behavior now removes job AND associated tasks
- Add --without-tasks flag to preserve old behavior
- Query and delete PreImportTasks and ImportTaskV2 by job ID
- Display deletion summary with success/failure counts
- Continue job deletion even if some tasks fail to delete
- Add e2e test for --without-tasks flag"
```

---

## Testing & Validation

### Task 5: Manual Integration Testing

**Prerequisites:**
- Running Milvus instance with import jobs and tasks in etcd
- Access to etcd via birdwatcher

- [ ] **Step 1: Test show import-job alias**

```bash
./birdwatcher
> connect --etcd localhost:2379 --rootPath by-dev
> show import-job
```

Expected: Should display import jobs (same as `show bulkinsert`)

- [ ] **Step 2: Test show import-task with existing task**

```bash
# First, find an existing task ID from show bulkinsert output
> show bulkinsert --detail --job <jobID>

# Then show individual task
> show import-task --task <taskID>
```

Expected: Should display task details with formatting

- [ ] **Step 3: Test show import-task with --showAllFiles**

```bash
> show import-task --task <taskID> --showAllFiles
```

Expected: Should display all files (not limited to 3)

- [ ] **Step 4: Test remove import-task dry-run**

```bash
> remove import-task --task <taskID>
```

Expected: Should show task details and message about dry-run

- [ ] **Step 5: Test remove import-job dry-run (default: with tasks)**

```bash
> remove import-job --job <jobID>
```

Expected: Should show job, list associated tasks, mention they will be deleted

- [ ] **Step 6: Test remove import-job dry-run (with --without-tasks)**

```bash
> remove import-job --job <jobID> --without-tasks
```

Expected: Should show only job, mention tasks will not be deleted

- [ ] **Step 7: Document test results**

Create a test log file:

```bash
echo "# Manual Integration Test Results - $(date)" > test-results.md
echo "" >> test-results.md
echo "## Test Summary" >> test-results.md
echo "- [ ] show import-job alias works" >> test-results.md
echo "- [ ] show import-task displays PreImportTask correctly" >> test-results.md
echo "- [ ] show import-task displays ImportTaskV2 correctly" >> test-results.md
echo "- [ ] show import-task handles non-existent task" >> test-results.md
echo "- [ ] show import-task --showAllFiles displays all files" >> test-results.md
echo "- [ ] remove import-task dry-run works" >> test-results.md
echo "- [ ] remove import-job default behavior shows tasks" >> test-results.md
echo "- [ ] remove import-job --without-tasks skips tasks" >> test-results.md
```

- [ ] **Step 8: Run all e2e tests**

```bash
cd tests/e2e
./run_tests.sh
```

Expected: All tests pass or skip gracefully

- [ ] **Step 9: Verify code coverage**

Note: Per user requirements (CLAUDE.md), code coverage must be greater than 90%.

```bash
# Run tests with coverage
go test -cover ./states/etcd/show/... ./states/etcd/remove/...

# For detailed coverage report
go test -coverprofile=coverage.out ./states/etcd/show/... ./states/etcd/remove/...
go tool cover -func=coverage.out
```

Expected: Coverage > 90% for modified packages. If coverage is below 90%, add unit tests before proceeding.

- [ ] **Step 10: Commit test results (if created)**

```bash
git add test-results.md
git commit -s -m "test: add manual integration test results for import task removal"
```

---

## Documentation

### Task 6: Update Documentation

**Files:**
- Check: `README.md`, `docs/` directory for user-facing documentation

- [ ] **Step 1: Check if command documentation exists**

```bash
ls docs/commands/ 2>/dev/null || echo "No commands documentation directory"
```

- [ ] **Step 2: Update README if it documents commands**

If `README.md` or other docs list available commands, add:
- `show import-task` - Display individual import task by task ID
- `remove import-task` - Remove individual import task by task ID
- `show import-job` - Alias for `show bulkinsert`
- Update `remove import-job` description to mention task removal

- [ ] **Step 3: Commit documentation updates (if any)**

```bash
git add README.md docs/
git commit -s -m "docs: update command documentation for import task removal"
```

---

## Final Validation

### Task 7: Pre-PR Checklist

- [ ] **Step 1: Run static checks**

```bash
make static-check
```

Expected: All checks pass

- [ ] **Step 2: Format code**

```bash
go fmt ./states/etcd/show/... ./states/etcd/remove/...
```

- [ ] **Step 3: Run all tests**

```bash
cd tests/e2e
./run_tests.sh
```

- [ ] **Step 4: Verify all commits are signed**

```bash
git log --show-signature -5
```

Expected: All commits have `Signed-off-by` line

- [ ] **Step 5: Review commit history**

```bash
git log --oneline origin/main..HEAD
```

Expected commits:
1. feat: add import-job alias to show bulkinsert command
2. feat: add show import-task command
3. feat: add remove import-task command
4. feat: enhance remove import-job to remove associated tasks
5. (optional) test: add manual integration test results
6. (optional) docs: update command documentation

- [ ] **Step 6: Rebase on latest main**

```bash
git fetch origin
git rebase origin/main
```

- [ ] **Step 7: Final build test**

```bash
go build -o birdwatcher .
./birdwatcher --version
```

Expected: Binary builds successfully

- [ ] **Step 8: Prepare and display PR content for user confirmation**

Per CLAUDE.md: "执行 git commit 前,必须先展示提交内容(标题、commit message)并等待用户确认"

Display PR content to user:

```
Title: feat: support remove import task

Body:
## Summary
- Add show import-task command to display individual tasks
- Add remove import-task command to remove individual tasks
- Add import-job alias to show bulkinsert for consistency
- Enhance remove import-job to remove associated tasks by default
- Add --without-tasks flag to preserve old remove import-job behavior

## Test Plan
- [x] Manual integration testing with existing Milvus instance
- [x] E2E shell tests for all new commands
- [x] Dry-run tests to verify safety mechanisms
- [x] Edge case testing (non-existent IDs, tasks in both collections)
- [x] Code coverage > 90%

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

**Wait for user confirmation before proceeding to Step 9**

- [ ] **Step 9: Push and create PR (after user confirms)**

```bash
# Push to remote
git push -u origin HEAD

# Create PR using gh CLI with confirmed content
gh pr create --title "feat: support remove import task" \
  --body "$(cat <<'EOF'
## Summary
- Add show import-task command to display individual tasks
- Add remove import-task command to remove individual tasks
- Add import-job alias to show bulkinsert for consistency
- Enhance remove import-job to remove associated tasks by default
- Add --without-tasks flag to preserve old remove import-job behavior

## Test Plan
- [x] Manual integration testing with existing Milvus instance
- [x] E2E shell tests for all new commands
- [x] Dry-run tests to verify safety mechanisms
- [x] Edge case testing (non-existent IDs, tasks in both collections)
- [x] Code coverage > 90%

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Notes

**TDD Approach:** This plan follows test-driven development where applicable. For commands, we add e2e tests and verify behavior before considering the task complete.

**DRY Principle:** We reuse existing functions (`ListPreImportTasks`, `ListImportTasks`, `PrintPreImportTask`, `PrintImportTask`) rather than duplicating logic.

**YAGNI Principle:** We implement only what's specified in the design - no extra features, no speculative generalization.

**Frequent Commits:** Each logical change (alias, show command, remove command, enhancement) gets its own commit with descriptive message.

**Error Handling:** All etcd operations use timeouts and display clear error messages. Partial failures during batch deletion are logged but don't stop the process.

**Safety First:** All remove commands default to dry-run mode. Actual deletion requires explicit `--run` flag.
