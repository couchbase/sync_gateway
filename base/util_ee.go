// +build cb_sg_enterprise

package base

import (
	"io"
	"time"

	"github.com/couchbaselabs/go-fleecedelta"
	jsoniter "github.com/json-iterator/go"
)

// The reason for these wrappers is to keep go-fleecedelta out of the compiled CE builds by providing no-op versions in deltas_ce.go
func init() {
	fleecedelta.StringDiffEnabled = true             // Global toggle for string diffs
	fleecedelta.StringDiffMinLength = 60             // 60 B min length to match CBL
	fleecedelta.StringDiffMaxLength = 1024 * 1024    // 1 MB max length for string diffs
	fleecedelta.StringDiffTimeout = time.Millisecond // Aggressive string diff timeout
}

var jsonIterConfig = jsoniter.ConfigCompatibleWithStandardLibrary

// Diff will return the fleece delta between old and new.
func Diff(old, new map[string]interface{}) (delta []byte, err error) {
	return fleecedelta.DiffJSON(old, new)
}

// Patch attempts to patch old with the given delta passed as a map[string]interface{}
func Patch(old *map[string]interface{}, delta map[string]interface{}) (err error) {
	return fleecedelta.PatchJSONWithUnmarshalledDelta(old, delta)
}

// JSONUnmarshal parses the JSON-encoded data and stores the result in the value pointed to by v.
func JSONUnmarshal(data []byte, v interface{}) error {
	// TODO: Fall back to stdlib json when unsupported config option is set
	if err := jsonIterConfig.Unmarshal(data, v); err != nil {
		return &JSONIterError{E: err}
	}
	return nil
}

// JSONMarshal returns the JSON encoding of v.
func JSONMarshal(v interface{}) ([]byte, error) {
	// TODO: Fall back to stdlib json when unsupported config option is set
	b, err := jsonIterConfig.Marshal(v)
	if err != nil {
		return nil, &JSONIterError{E: err}
	}
	return b, nil
}

// JSONDecoder returns a new JSON decoder implementing the JSONDecoderI interface
func JSONDecoder(r io.Reader) JSONDecoderI {
	// TODO: Fall back to stdlib json when unsupported config option is set
	return jsonIterConfig.NewDecoder(r)
}

// JSONEncoder returns a new JSON encoder implementing the JSONEncoderI interface
func JSONEncoder(w io.Writer) JSONEncoderI {
	// TODO: Fall back to stdlib json when unsupported config option is set
	return jsonIterConfig.NewEncoder(w)
}
