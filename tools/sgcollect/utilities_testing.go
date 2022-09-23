package main

import (
	"bytes"
	"fmt"
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

func AssertDidNotFail(t *testing.T, ter TaskExecutionResult) bool {
	return assert.NoError(t, ter.Error, fmt.Sprintf("Task %s [%s] errored", ter.Task.Name(), ter.Task.Header()))
}

func AssertRan(t *testing.T, ter TaskExecutionResult) bool {
	if ter.SkipReason != "" {
		assert.Failf(t, fmt.Sprintf("Task %s [%s] skipped", ter.Task.Name(), ter.Task.Header()), ter.SkipReason)
		return false
	}
	return AssertDidNotFail(t, ter)
}

func NewTaskTester(t *testing.T, optOverrides SGCollectOptions) *TaskTester {
	return &TaskTester{
		t:    t,
		opts: &optOverrides,
	}
}
