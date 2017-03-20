package base

import (
	"log"
	"encoding/json"
)

type SGTranscoder struct {
}

// Encode applies the default Couchbase transcoding behaviour to encode a Go type.
// Figures out how to convert to bytes
// Figures out what to use for flags
func (t SGTranscoder) Encode(value interface{}) ([]byte, uint32, error) {

	// TODO: will callers complain if flags is unset?

	var bytes []byte
	var flags uint32
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

	//// Check for legacy flags
	//if flags&cfMask == 0 {
	//	// Legacy Flags
	//	if flags == lfJson {
	//		// Legacy JSON
	//		flags = cfFmtJson
	//	} else {
	//		return clientError{"Unexpected legacy flags value"}
	//	}
	//}
	//
	//// Make sure compression is disabled
	//if flags&cfCmprMask != cfCmprNone {
	//	return clientError{"Unexpected value compression"}
	//}
	//
	//// Normal types of decoding
	//if flags&cfFmtMask == cfFmtBinary {
	//	switch typedOut := out.(type) {
	//	case *[]byte:
	//		*typedOut = bytes
	//		log.Printf("DefaultTranscoder Decode *[]byte with cfFmtBinary")
	//		return nil
	//	case *interface{}:
	//		*typedOut = bytes
	//		log.Printf("DefaultTranscoder Decode *interface{} with cfFmtBinary")
	//		return nil
	//	default:
	//		return clientError{"You must encode binary in a byte array or interface"}
	//	}
	//} else if flags&cfFmtMask == cfFmtString {
	//	switch typedOut := out.(type) {
	//	case *string:
	//		*typedOut = string(bytes)
	//		log.Printf("DefaultTranscoder Decode *string with cfFmtString")
	//		return nil
	//	case *interface{}:
	//		*typedOut = string(bytes)
	//		log.Printf("DefaultTranscoder Decode *interface{} with cfFmtString")
	//		return nil
	//	default:
	//		return clientError{"You must encode a string in a string or interface"}
	//	}
	//} else if flags&cfFmtMask == cfFmtJson {
	//	log.Printf("DefaultTranscoder Decode default with cfFmtJson")
	//	err := json.Unmarshal(bytes, &out)
	//	if err != nil {
	//		return err
	//	}
	//	return nil
	//}
	// return clientError{"Unexpected flags value"}

}
