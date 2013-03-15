package rest

import (
	"encoding/json"
	"os"
)

// JSON object that defines the server configuration.
type ServerConfig struct {
	Interface      *string // Interface to bind REST API to, default ":4984"
	AdminInterface *string // Interface to bind admin API to, default ":4985"
	BrowserID      *struct {
		Origin *string // Canonical server URL for BrowserID authentication
	}
	Log       []string // Log keywords to enable
	Pretty    bool     // Pretty-print JSON responses?
	Databases []DbConfig
}

// JSON object that defines a database configuration within the ServerConfig.
type DbConfig struct {
	Name       *string // Database name in REST API
	Server     *string // Couchbase (or Walrus) server URL, default "http://localhost:8091"
	BucketName *string // Bucket name on server; defaults to same as 'name'
	Pool       *string // Couchbase pool name, default "default"
}

func ReadConfig(path string) (*ServerConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	dec := json.NewDecoder(file)
	var config *ServerConfig
	if err := dec.Decode(&config); err != nil {
		return nil, err
	}
	return config, nil
}
