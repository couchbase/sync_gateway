package main

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

type TaskTester struct {
	t    *testing.T
	opts *SGCollectOptions
}

func (tt *TaskTester) RunTask(task SGCollectTask) (*bytes.Buffer, TaskExecutionResult) {
	var buf bytes.Buffer
	res := ExecuteTask(task, tt.opts, &buf, func(s string) {
		tt.t.Log(s)
	}, true)
	return &buf, res
}

func AssertRan(t *testing.T, ter TaskExecutionResult) {
	if ter.SkipReason != "" {
		assert.Failf(t, "Task skipped: %s", ter.SkipReason)
	}
	assert.NoError(t, ter.Error, "Task errored")
}

func NewTaskTester(t *testing.T, optOverrides SGCollectOptions) *TaskTester {
	return &TaskTester{
		t:    t,
		opts: &optOverrides,
	}
}
