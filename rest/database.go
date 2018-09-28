package rest

import "encoding/json"

// This is the object returned from http://server/<db-name>/
type Database struct {
	CommittedUpdateSeq uint64      `json:"committed_update_seq"`
	CompactRunning     bool        `json:"compact_running"`
	DbName             string      `json:"db_name"`
	DiskFormatVersion  int         `json:"disk_format_version"`
	InstanceStartTime  json.Number `json:"instance_start_time"`
	PurgeSeq           int         `json:"purge_seq"`
	State              string      `json:"state"`
	UpdateSeq          uint64      `json:"update_seq"`
	ServerUuid         string      `json:"server_uuid"`
}
