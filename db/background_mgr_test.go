package db

import (
	"fmt"
	"testing"
)

func TestBackgroundManager(t *testing.T) {
	resyncManager := NewResyncManager()
	resyncManager.SetRunState(BackgroundProcessStateRunning)
	fmt.Println(string(resyncManager.GetStatus()))
}
