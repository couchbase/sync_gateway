package db

import (
	"sync"
	"time"
)

const (
	CompactTypeAttachment = "attachment"
	CompactTypeTombstone  = "tombstone"
)

const (
	CompactActionStart = "start"
	CompactActionStop  = "stop"
)

const (
	CompactStateRunning  = "running"
	CompactStateStopped  = "stopped"
	CompactStateStopping = "stopping"
	CompactStateError    = "error"
)

const (
	AttachmentCompactPhaseMark  = "mark"
	AttachmentCompactPhaseSweep = "sweep"
)

type DatabaseCompactor struct {
	AttachmentCompactStatus     AttachmentCompactStatus
	AttachmentCompactLastError  error
	AttachmentCompactTerminator bool

	TombstoneCompactStatus     TombstoneCompactStatus
	TombstoneCompactLastError  error
	TombstoneCompactTerminator bool

	lock sync.Mutex
}

type CompactStatus interface{}

type AttachmentCompactStatus struct {
	Type                 string        `json:"type"`
	Phase                string        `json:"phase,omitempty"`
	ID                   string        `json:"id,omitempty"`
	Status               string        `json:"status"`
	StartTime            *time.Time    `json:"start_time,omitempty"`
	Duration             time.Duration `json:"duration,omitempty"`
	DocsProcessed        int           `json:"docs_processed"`
	AttachmentsMarked    int           `json:"attachments_marked,omitempty"`
	AttachmentsCompacted int           `json:"attachments_compacted"`
	TombstonesCompacted  int           `json:"tombstones_compacted"`
	Error                string        `json:"last_error,omitempty"`
}

type TombstoneCompactStatus struct {
	Type                string        `json:"type"`
	Status              string        `json:"status"`
	StartTime           *time.Time    `json:"start_time,omitempty"`
	Duration            time.Duration `json:"duration,omitempty"`
	TombstonesCompacted int           `json:"tombstones_compacted"`
	Error               string        `json:"last_error,omitempty"`
}

func (c *DatabaseCompactor) GetStatus(compactType string) CompactStatus {
	c.lock.Lock()
	defer c.lock.Unlock()

	if compactType == CompactTypeAttachment {
		return c.attachmentCompactStatus()
	}
	return c.tombstoneCompactStatus()
}

func (c *DatabaseCompactor) tombstoneCompactStatus() *TombstoneCompactStatus {
	compactStatus := TombstoneCompactStatus{
		Type:                CompactTypeTombstone,
		Status:              c.TombstoneCompactStatus.Status,
		StartTime:           c.TombstoneCompactStatus.StartTime,
		TombstonesCompacted: c.TombstoneCompactStatus.TombstonesCompacted,
	}

	if c.TombstoneCompactStatus.StartTime != nil {
		compactStatus.Duration = time.Now().Sub(*c.TombstoneCompactStatus.StartTime)
	}

	if compactStatus.Status == "" {
		compactStatus.Status = CompactStateStopped
	}

	if c.TombstoneCompactLastError != nil {
		compactStatus.Error = c.TombstoneCompactLastError.Error()
	}

	return &compactStatus
}

func (c *DatabaseCompactor) attachmentCompactStatus() *AttachmentCompactStatus {
	compactStatus := AttachmentCompactStatus{
		Type:          CompactTypeAttachment,
		Status:        c.AttachmentCompactStatus.Status,
		StartTime:     c.AttachmentCompactStatus.StartTime,
		DocsProcessed: c.AttachmentCompactStatus.DocsProcessed,
	}

	if c.AttachmentCompactStatus.StartTime != nil {
		compactStatus.Duration = time.Now().Sub(*c.AttachmentCompactStatus.StartTime)
	}

	if c.AttachmentCompactStatus.Phase == AttachmentCompactPhaseMark {
		compactStatus.AttachmentsMarked = c.AttachmentCompactStatus.AttachmentsMarked
	}

	if c.AttachmentCompactStatus.Phase == AttachmentCompactPhaseSweep {
		compactStatus.AttachmentsCompacted = c.AttachmentCompactStatus.AttachmentsCompacted
	}

	if compactStatus.Status == "" {
		compactStatus.Status = CompactStateStopped
	}

	if c.AttachmentCompactLastError != nil {
		compactStatus.Error = c.AttachmentCompactLastError.Error()
	}

	return &compactStatus
}

// RecordAttachmentCompactStartTime records the start time of legacy attachment compaction.
func (c *DatabaseCompactor) RecordAttachmentCompactStartTime() {
	c.lock.Lock()
	defer c.lock.Unlock()

	now := time.Now()
	c.AttachmentCompactStatus.StartTime = &now
}

// RecordTombstoneCompactStartTime records the start time of tombstone compaction.
func (c *DatabaseCompactor) RecordTombstoneCompactStartTime() {
	c.lock.Lock()
	defer c.lock.Unlock()

	now := time.Now()
	c.TombstoneCompactStatus.StartTime = &now
}

func (c *DatabaseCompactor) SetAttachmentCompactStatus(status string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.AttachmentCompactStatus.Status = status
}

func (c *DatabaseCompactor) SetTombstoneCompactStatus(status string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.TombstoneCompactStatus.Status = status
}

func (c *DatabaseCompactor) SetTombstonesCompacted(tombstonesCompacted int) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.TombstoneCompactStatus.TombstonesCompacted = tombstonesCompacted
}

// ResetAttachmentCompactStatus resets the legacy attachment compaction status.
func (c *DatabaseCompactor) ResetAttachmentCompactStatus() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.AttachmentCompactStatus.DocsProcessed = 0
	c.AttachmentCompactStatus.AttachmentsMarked = 0
	c.AttachmentCompactStatus.AttachmentsCompacted = 0
	c.AttachmentCompactStatus.Error = ""
	c.AttachmentCompactTerminator = false
}

// ResetTombstoneCompactStatus resets the tombstone compaction status.
func (c *DatabaseCompactor) ResetTombstoneCompactStatus() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.TombstoneCompactStatus.TombstonesCompacted = 0
	c.AttachmentCompactStatus.Error = ""
	c.AttachmentCompactTerminator = false
}

// SetAttachmentCompactError sets the legacy attachment compaction status to an error in case of any failure.
func (c *DatabaseCompactor) SetAttachmentCompactError(err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.AttachmentCompactLastError = err
	c.AttachmentCompactStatus.Status = CompactStateError
}

// SetTombstoneCompactError sets the tombstone compaction status to an error in case of any failure.
func (c *DatabaseCompactor) SetTombstoneCompactError(err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.TombstoneCompactLastError = err
	c.TombstoneCompactStatus.Status = CompactStateError
}

// ShouldStopTombstoneCompact reports whether the ongoing tombstone compaction can be stopped.
func (c *DatabaseCompactor) ShouldStopTombstoneCompact() bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.TombstoneCompactTerminator {
		c.TombstoneCompactTerminator = false
		return true
	}

	return false
}

// ShouldStopAttachmentCompact reports whether the ongoing legacy attachment compaction can be stopped.
func (c *DatabaseCompactor) ShouldStopAttachmentCompact() bool {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.AttachmentCompactTerminator {
		c.AttachmentCompactTerminator = false
		return true
	}

	return false
}

// StopAttachmentCompact stops the ongoing legacy attachment compaction and returns status.
func (c *DatabaseCompactor) StopAttachmentCompact() CompactStatus {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.AttachmentCompactStatus.Status = CompactStateStopping
	c.AttachmentCompactTerminator = true

	return c.attachmentCompactStatus()
}

// StopTombstoneCompact stops the ongoing tombstone compaction and returns status.
func (c *DatabaseCompactor) StopTombstoneCompact() CompactStatus {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.TombstoneCompactStatus.Status = CompactStateStopping
	c.TombstoneCompactTerminator = true

	return c.tombstoneCompactStatus()
}
