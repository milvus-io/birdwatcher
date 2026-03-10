# Import Task Removal Support Design

**Date:** 2026-03-10
**Status:** Approved

## Overview

This design adds the ability to show and remove individual import tasks (PreImportTask and ImportTaskV2) and enhances the existing `remove import-job` command to optionally remove associated tasks. This gives users fine-grained control over viewing and cleaning up import-related metadata in etcd.

## Motivation

Currently, birdwatcher supports:
- `show bulkinsert` - Display import jobs and their associated tasks
- `remove import-job` - Remove import jobs only

However, users cannot:
- View or remove individual import tasks (PreImportTask or ImportTaskV2) by task ID
- Remove import jobs along with their associated tasks in one operation

This creates operational gaps when debugging or cleaning up import operations, requiring manual etcd manipulation or multiple commands.

## Changes Summary

1. Add `import-job` alias to existing `show bulkinsert` command for consistency
2. New command: `show import-task --task <taskID>` for viewing individual tasks
3. New command: `remove import-task --task <taskID>` for removing individual tasks
4. Enhance `remove import-job` to remove associated tasks by default
5. Add `--without-tasks` flag to `remove import-job` to preserve old behavior

## Command Specifications

### 1. `show bulkinsert` / `show import-job` (alias)

**Existing command - add alias only**

```go
type ImportJobParam struct {
    framework.DataSetParam `use:"show bulkinsert" desc:"display bulkinsert jobs and tasks" alias:"import,import-job"`
    // ... existing fields
}
```

No behavioral changes - just adds `import-job` as an additional alias.

### 2. `show import-task --task <taskID>`

**New command for viewing individual tasks**

**Parameters:**
- `--task <taskID>` (required): Task ID to display
- `--showAllFiles` (optional, default: false): Show all files or limit to first 3

**Behavior:**
- Search for task ID in both PreImportTask and ImportTaskV2 collections
- If found in PreImportTask: display PreImportTask details (TaskID, NodeID, State, Reason, FileStats)
- If found in ImportTaskV2: display ImportTaskV2 details (TaskID, NodeID, State, Reason, SegmentIDs, CompleteTime, FileStats)
- If not found: print "cannot find target import task: <taskID>"
- If found in both (edge case): print warning and show both with type labels

**Example Usage:**
```bash
# Show task details
Milvus(by-dev): show import-task --task 123

# Show task with all files
Milvus(by-dev): show import-task --task 123 --showAllFiles
```

### 3. `remove import-task --task <taskID>`

**New command for removing individual tasks**

**Parameters:**
- `--task <taskID>` (required): Task ID to remove
- `--run` (inherited from framework): Dry-run by default, requires `--run` to actually delete

**Behavior:**
- Search for task ID in both PreImportTask and ImportTaskV2 collections
- Display found task details before removal
- If `--run` not specified: show what would be deleted and exit
- If `--run` specified: delete the task from etcd and confirm success
- If not found: print "cannot find target import task: <taskID>"
- If found in both (edge case): remove both with warning message

**Example Usage:**
```bash
# Dry-run (show what would be deleted)
Milvus(by-dev): remove import-task --task 123

# Actually delete the task
Milvus(by-dev): remove import-task --task 123 --run
```

### 4. `remove import-job --job <jobID> [--without-tasks]`

**Enhanced existing command**

**Parameters:**
- `--job <jobID>` (existing, required): Job ID to remove
- `--run` (existing, inherited): Dry-run by default
- `--without-tasks` (new, optional, default: false): Skip removing associated tasks

**Behavior changes:**
- **Default behavior (without-tasks=false)**: Remove job AND all associated PreImportTasks and ImportTaskV2 tasks
- **With --without-tasks flag**: Remove only the job (old behavior)
- Display what will be removed before deletion
- If `--run` not specified: show planned deletions and exit
- If `--run` specified: delete job and tasks (if applicable), confirm each deletion

**Example Usage:**
```bash
# Remove job and all associated tasks (new default)
Milvus(by-dev): remove import-job --job 456 --run

# Remove only the job, keep tasks (old behavior)
Milvus(by-dev): remove import-job --job 456 --without-tasks --run

# Dry-run to see what would be deleted
Milvus(by-dev): remove import-job --job 456
```

## Implementation Details

### File Changes

#### 1. `states/etcd/show/bulkinsert.go`

**Changes:**
- Update `ImportJobParam.alias` from `"import"` to `"import,import-job"`
- Add new `ImportTaskParam` struct
- Add new `ShowImportTaskCommand` method to `ComponentShow`
- Add helper function `printImportTaskDetails` for displaying task info

**New structures:**
```go
type ImportTaskParam struct {
    framework.DataSetParam `use:"show import-task" desc:"display import task by task ID"`

    TaskID       int64 `name:"task" default:"0" desc:"task id to display"`
    ShowAllFiles bool  `name:"showAllFiles" default:"false" desc:"flags indicating whether printing all files"`
}
```

#### 2. `states/etcd/remove/bulkinsert.go`

**Changes:**
- Update `RemoveImportJobParam` to add `WithoutTasks` field
- Update `RemoveImportJobCommand` to handle task removal
- Add new `RemoveImportTaskParam` struct
- Add new `RemoveImportTaskCommand` method to `ComponentRemove`

**Updated structures:**
```go
type RemoveImportJobParam struct {
    framework.ExecutionParam `use:"remove import-job" desc:"Remove import job from datacoord meta with specified job id" alias:"import"`

    JobID        int64 `name:"job" default:"" desc:"import job id to remove"`
    WithoutTasks bool  `name:"without-tasks" default:"false" desc:"remove job only, keep associated tasks"`
}

type RemoveImportTaskParam struct {
    framework.ExecutionParam `use:"remove import-task" desc:"Remove import task from datacoord meta with specified task id"`

    TaskID int64 `name:"task" default:"" desc:"import task id to remove"`
}
```

#### 3. `states/etcd/common/bulkinsert.go`

No changes needed - existing functions are sufficient:
- `ListPreImportTasks()` - for querying PreImportTasks
- `ListImportTasks()` - for querying ImportTaskV2 tasks

### Task Lookup Strategy

When searching for a task by ID:

1. Query PreImportTasks using `common.ListPreImportTasks` with filter:
   ```go
   func(task *models.PreImportTask) bool {
       return task.GetProto().GetTaskID() == p.TaskID
   }
   ```

2. Query ImportTaskV2 using `common.ListImportTasks` with filter:
   ```go
   func(task *models.ImportTaskV2) bool {
       return task.GetProto().GetTaskID() == p.TaskID
   }
   ```

3. Combine results and handle:
   - Found in neither: error message
   - Found in one: proceed with that task
   - Found in both: proceed with both (unlikely but possible), log warning

### Deletion Order for Job Removal

When removing job with tasks (default behavior):

1. Display job details
2. Query and list all associated PreImportTasks by job ID
3. Query and list all associated ImportTaskV2 tasks by job ID
4. Show summary of what will be deleted
5. If `--run`:
   - Delete PreImportTasks first
   - Delete ImportTaskV2 tasks second
   - Delete job last
6. Confirm each deletion step with success/failure message

## Error Handling

### etcd Connection Errors
- If etcd client fails to list/delete: display error message with context
- Use 3-second timeout for delete operations (consistent with existing `RemoveImportJobCommand`)
- Return error to stop execution if listing fails

### Task Not Found
- `show import-task`: Print "cannot find target import task: <taskID>" and return nil
- `remove import-task`: Print "cannot find target import task: <taskID>" and return nil (safe to retry)

### Multiple Tasks with Same ID (Edge Case)
- Very unlikely but possible if both PreImportTask and ImportTaskV2 have the same task ID
- Show both tasks with type labels: "[PreImportTask]" and "[ImportTaskV2]"
- For removal: delete both with warning message "Found task in both PreImportTask and ImportTaskV2, removing both"

### Partial Deletion Failures
When removing job with tasks, if a task deletion fails:
- Log the error for that specific task
- Continue attempting to delete remaining tasks
- Attempt to delete the job anyway
- Report summary of successes/failures at the end

Example output:
```
Failed to delete 2 PreImportTasks: [123, 456]
Successfully deleted 3 ImportTaskV2 tasks
Successfully deleted import job 789
```

### Dry-Run Safety
- All commands respect the `--run` flag pattern (inherited from framework)
- Without `--run`: show what would be deleted, don't delete anything
- With `--run`: perform actual deletions

## Testing Strategy

### Manual Testing Scenarios

**show import-task:**
1. Show existing PreImportTask by task ID
2. Show existing ImportTaskV2 by task ID
3. Show non-existent task ID (should display not found message)
4. Test with `--showAllFiles` flag

**remove import-task:**
1. Dry-run removal (without `--run`) for PreImportTask
2. Dry-run removal (without `--run`) for ImportTaskV2
3. Actual removal (with `--run`) for PreImportTask
4. Actual removal (with `--run`) for ImportTaskV2
5. Attempt to remove non-existent task
6. Remove already-deleted task (idempotency check)

**remove import-job with tasks:**
1. Remove job without tasks using `--without-tasks` flag (old behavior)
2. Dry-run removal of job with tasks (default behavior)
3. Actual removal of job with tasks (default behavior with `--run`)
4. Remove job that has no associated tasks (should work normally)
5. Test partial failure scenario (if possible to simulate)

**show import-job alias:**
1. Verify `show bulkinsert` still works
2. Verify `show import` alias still works (existing)
3. Verify new `show import-job` alias works with same parameters

### E2E Test Coverage

Based on existing e2e test pattern in `tests/e2e/commands/test_remove.sh`, add tests for:
- `show import-task --task <id>`
- `remove import-task --task <id>` (dry-run and actual)
- `remove import-job --job <id> --without-tasks`
- `show import-job` alias verification

### Test Data Requirements

Tests require access to Milvus instance with:
- At least one import job with associated PreImportTasks
- At least one import job with associated ImportTaskV2 tasks
- At least one import job with both task types

## Backwards Compatibility

### Breaking Changes
**`remove import-job` default behavior changes:**
- Old: Removes only the import job
- New: Removes the import job AND all associated tasks

**Migration path:**
- Users who want old behavior: add `--without-tasks` flag
- Users who want new behavior: no changes needed

This is considered an enhancement rather than a breaking change because:
1. The old behavior is still available via `--without-tasks`
2. Removing tasks along with jobs is generally the desired behavior
3. Dry-run mode (default) gives users visibility before actual deletion

### Non-Breaking Changes
- New commands (`show import-task`, `remove import-task`) don't affect existing functionality
- New alias (`show import-job`) is purely additive
- All existing commands and aliases continue to work

## Future Enhancements

Potential future additions (out of scope for this design):
- Filter tasks by state when showing/removing (e.g., remove all failed tasks)
- Batch removal of tasks by collection ID
- Remove orphaned tasks (tasks with no associated job)
- Support for removing tasks by node ID
- JSON output format for `show import-task`
