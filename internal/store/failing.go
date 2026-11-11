package store

import (
	"slices"
	"time"
)

// FailingTask is an unbroken run of failures of one task: from its first
// failure to the next run that succeeds. The daemon pushes the first failure,
// folds every further one into the same inbox entry, and pushes once more when
// the task works again. It is kept on disk, so a restart in the middle of a
// failing streak neither announces it a second time nor forgets to say that
// it ended.
type FailingTask struct {
	TaskID   string `json:"task_id"`
	TaskName string `json:"task_name"`
	// Since is when the first failure of the streak finished, Count how many
	// runs have failed since.
	Since time.Time `json:"since"`
	Count int       `json:"count"`
	// LastFailure is when the latest failure finished.
	LastFailure time.Time `json:"last_failure"`
	// Pushed is when the streak last went out to the channels: the first
	// failure, or the latest reminder.
	Pushed time.Time `json:"pushed"`
	// InboxID is the inbox entry that stands for the streak; each further
	// failure updates it instead of adding one.
	InboxID string `json:"inbox_id,omitempty"`
}

const failingFile = "failing.json"

var failingList = jsonList[FailingTask]{file: failingFile, field: "tasks", dropWhenEmpty: true}

// FailingTasks returns every open failure streak.
func (s *Store) FailingTasks() ([]FailingTask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return failingList.load(s)
}

// UpdateFailingTask atomically applies fn to the streak of one task. fn gets
// the current streak, or nil when the task is not failing, and returns the
// streak to keep, or nil to end it.
func (s *Store) UpdateFailingTask(taskID string, fn func(cur *FailingTask) *FailingTask) error {
	return failingList.update(s, func(list *[]FailingTask) error {
		i := slices.IndexFunc(*list, func(f FailingTask) bool { return f.TaskID == taskID })
		var cur *FailingTask
		if i >= 0 {
			c := (*list)[i]
			cur = &c
		}
		next := fn(cur)
		switch {
		case next == nil && i >= 0:
			*list = slices.Delete(*list, i, i+1)
		case next != nil && i >= 0:
			(*list)[i] = *next
		case next != nil:
			*list = append(*list, *next)
		}
		return nil
	})
}
