package main

// serverConfig is a subset of rest.RunTimeServerConfigResponse, copied here to keep the sgcollect binary size down.
type serverConfig struct {
	Logging struct {
		LogFilePath string `json:"log_file_path,omitempty"`
	} `json:"logging,omitempty"`
	Databases map[string]any `json:"databases"`
}
