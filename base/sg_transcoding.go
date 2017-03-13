package base

import "encoding/json"

type SGTranscoder struct {
}

// This is needed because the default transcoding.go code in gocb is relying heavily on the
// document flags (Binary/JSON,etc), however the Sync Gateway codebase is currently not "flags-aware",
// and would require a decent amount of overhaul in order to use the default gocb transcoding.
// It currently has parity with the existing go-couchbase library, namely to use the legacy json flag.

// Encode applies the default Couchbase transcoding behaviour to encode a Go type.
// Figures out how to convert the given struct into bytes and then figures out what to use for flags (uses 0 value, legacy JSON flag)
func (t SGTranscoder) Encode(value interface{}) ([]byte, uint32, error) {

	var bytes []byte
	var flags uint32 // 0 value, which corresponds to gocb.lfJson
	var err error

	switch value.(type) {
	case []byte:
		bytes = value.([]byte)
	case *[]byte:
		bytes = *value.(*[]byte)
	case string:
		bytes = []byte(value.(string))
	case *string:
		bytes = []byte(*value.(*string))
	case *interface{}:
		// calls back into this
		return t.Encode(*value.(*interface{}))
	default:
		bytes, err = json.Marshal(value)
		if err != nil {
			return nil, 0, err
		}
	}

	// No compression supported currently

	return bytes, flags, nil
}

// Decode applies the default Couchbase transcoding behaviour to decode into a Go type.
func (t SGTranscoder) Decode(bytes []byte, flags uint32, out interface{}) error {

	switch typedOut := out.(type) {
	case *[]byte:
		*typedOut = bytes
		return nil
	default:
		err := json.Unmarshal(bytes, &out)
		if err != nil {
			return err
		}
		return nil

	}

}
