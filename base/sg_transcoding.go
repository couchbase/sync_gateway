package base

import (
	"github.com/couchbase/gocb"

	"gopkg.in/couchbase/gocbcore.v7"
)

// BinaryDocument is type alias that allows SGTranscoder to differentiate between documents that are
// intended to be written as binary docs, versus json documents that are being sent as raw bytes
// Some additional context here: https://play.golang.org/p/p4fkKiZD59
type BinaryDocument []byte

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
	switch typedValue := value.(type) {
	case BinaryDocument:
		flags = gocbcore.EncodeCommonFlags(gocbcore.BinaryType, gocbcore.NoCompression)
		bytes = []byte(typedValue)
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
		bytes, err = JSONMarshal(value)
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
		defaultTranscoder := gocb.DefaultTranscoder{}
		return defaultTranscoder.Decode(bytes, flags, out)

	}

}
