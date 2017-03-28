package base

import (
	"encoding/json"

	"gopkg.in/couchbase/gocbcore.v5"
)

type SGTranscoder struct {
}

// The default transcoding.go code in gocb makes assumptions about the document
// type (binary, json) based on the incoming value (e.g. []byte as binary, interface{} as json).
// Sync Gateway needs the ability to write json as raw bytes, so defines separate transcoders for storing
// json and binary documents.

// Encode applies the default Couchbase transcoding behaviour to encode a Go type.
// Figures out how to convert the given struct into bytes and then sets the json flag.
func (t SGTranscoder) Encode(value interface{}) ([]byte, uint32, error) {

	var bytes []byte
	var err error

	flags := gocbcore.EncodeCommonFlags(gocbcore.JsonType, gocbcore.NoCompression)
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

type SGBinaryTranscoder struct {
}

// Encode applies the default Couchbase transcoding behaviour to encode a Go type, and sets the storage flag as binary.
func (t SGBinaryTranscoder) Encode(value interface{}) ([]byte, uint32, error) {

	var bytes []byte
	var flags uint32
	var err error

	flags = gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression)
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
func (t SGBinaryTranscoder) Decode(bytes []byte, flags uint32, out interface{}) error {

	valueType, _ := gocbcore.DecodeCommonFlags(flags)
	if valueType != gocbcore.BinaryType {
		Warn("Binary Transcoder used to process non-binary document")
	}

	switch typedOut := out.(type) {
	case *[]byte:
		*typedOut = bytes
		return nil
	case *interface{}:
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
