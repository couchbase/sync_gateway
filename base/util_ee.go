// +build cb_sg_enterprise

package base

import (
	"encoding/json"
	"io"
	"time"

	"github.com/couchbaselabs/go-fleecedelta"
)

// The reason for these wrappers is to keep go-fleecedelta out of the compiled CE builds by providing no-op versions in deltas_ce.go

func init() {
	fleecedelta.StringDiffEnabled = true             // Global toggle for string diffs
	fleecedelta.StringDiffMinLength = 60             // 60 B min length to match CBL
	fleecedelta.StringDiffMaxLength = 1024 * 1024    // 1 MB max length for string diffs
	fleecedelta.StringDiffTimeout = time.Millisecond // Aggressive string diff timeout
}

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
	// FIXME: return jsoniter.Unmarshal(data, v)
	return json.Unmarshal(data, v)
}

// JSONMarshal returns the JSON encoding of v.
func JSONMarshal(v interface{}) ([]byte, error) {
	// TODO: Fall back to stdlib json when unsupported config option is set
	// FIXME: return jsoniter.Marshal(v)
	return json.Marshal(v)
}

// JSONDecoder returns a new JSON decoder implementing the JSONDecoderI interface
func JSONDecoder(r io.Reader) JSONDecoderI {
	// TODO: Fall back to stdlib json when unsupported config option is set
	// FIXME: return jsoniter.NewDecoder(r)
	return json.NewDecoder(r)
}

// JSONEncoder returns a new JSON encoder implementing the JSONEncoderI interface
func JSONEncoder(w io.Writer) JSONEncoderI {
	// TODO: Fall back to stdlib json when unsupported config option is set
	// FIXME: return jsoniter.NewEncoder(w)
	return json.NewEncoder(w)
}
