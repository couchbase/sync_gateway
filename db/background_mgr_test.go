package db

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBackgroundManager(t *testing.T) {
	resyncManager := NewResyncManager()
	resyncManager.setRunState(BackgroundProcessStateRunning)
	status, err := resyncManager.GetStatus()
	assert.NoError(t, err)
	fmt.Println(string(status))
}
