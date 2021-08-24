package rest

import (
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// DatabaseConfig is a 3.x/persisted database config that represents a config stored in the bucket.
type DatabaseConfig struct {
	// cas is the Couchbase Server CAS of the database config in the bucket
	// used to skip applying configs to SG nodes that already have an up-to-date config.
	// This value can be explicitly set to 0 before applyConfig to force reload.
	cas uint64

	// Version is a generated Rev ID used for optimistic concurrency control using ETags/If-Match headers.
	Version string `json:"version,omitempty"`

	// DbConfig embeds database config properties
	DbConfig
}

func (dbc *DatabaseConfig) Redacted() (*DatabaseConfig, error) {
	var config DatabaseConfig

	err := base.DeepCopyInefficient(&config, dbc)
	if err != nil {
		return nil, err
	}

	err = config.DbConfig.redactInPlace()
	if err != nil {
		return nil, err
	}

	if config.Guest != nil && config.Guest.Password != nil && *config.Guest.Password != "" {
		config.Guest.Password = base.StringPtr("xxxxx")
	}

	return &config, nil
}

func GenerateDatabaseConfigVersionID(previousRevID string, databaseConfig *DatabaseConfig) (string, error) {
	databaseConfig.Version = ""

	encodedBody, err := base.JSONMarshalCanonical(databaseConfig)
	if err != nil {
		return "", err
	}

	previousGen, previousRev := db.ParseRevID(previousRevID)
	generation := previousGen + 1

	hash := db.CreateRevIDWithBytes(generation, previousRev, encodedBody)
	return hash, nil
}
