package base

import (
	"fmt"

	"github.com/couchbase/cbgt"
)

// NewCbgtCfgMem runs cbgt.NewCfgMem and sets the matching version number we expect for Sync Gateway.
func NewCbgtCfgMem() (*cbgt.CfgMem, error) {
	cfg := cbgt.NewCfgMem()
	cas, err := cfg.Set(cbgt.VERSION_KEY, []byte(SGCbgtMetadataVersion), 0)
	if err != nil {
		return nil, err
	}
	expectedCas := uint64(1)
	if cas != uint64(1) {
		return nil, fmt.Errorf("Expected cas value %d, got: %d", expectedCas, cas)
	}
	return cfg, nil
}
