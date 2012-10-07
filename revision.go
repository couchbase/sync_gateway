// revision.go

package basecouch

import (
	"crypto/md5"
	"crypto/sha1"
	"encoding/json"
    "fmt"
    "log"
)

// The body of a CouchDB document/revision as decoded from JSON.
type Body map[string]interface{}

func (db *Database) getRevision(docid, revid string, key RevKey) (Body, error) {
	var body Body
	err := db.bucket.Get(revKeyToString(key), &body)
	if err != nil {
		return nil, err
	}
    body["_id"] = docid
    body["_rev"] = revid
	return body, nil
}


func (db *Database) setRevision(body Body) (RevKey, error) {
    body = stripSpecialProperties(body)
	digester := sha1.New()
	digester.Write(canonicalEncoding(body))
	revKey := RevKey(fmt.Sprintf("%x", digester.Sum(nil)))
	return revKey, db.bucket.Set(revKeyToString(revKey), 0, body)
}

//////// HELPERS:

func revKeyToString(key RevKey) string {
    return "rev:" + string(key)
}

func createRevID(generation int, parentRevID string, body Body) string {
    // This should produce the same results as TouchDB.
	digester := md5.New()
    digester.Write([]byte{byte(len(parentRevID))})
    digester.Write([]byte(parentRevID))
	digester.Write(canonicalEncoding(stripSpecialProperties(body)))
	return fmt.Sprintf("%d-%x", generation, digester.Sum(nil))
}

func parseRevID(revid string) (int, string) {
	if revid == "" {
		return 0,""
	}
	var generation int
    var id string
	n, _ := fmt.Sscanf(revid, "%d-%s", &generation, &id)
	if n < 1 || generation < 1 {
        log.Printf("WARNING: parseRevID failed on %q", revid)
		return -1, ""
	}
	return generation, id
}

func stripSpecialProperties(body Body) Body {
    stripped := Body{}
	for key, value := range body {
		if key == "" || key[0] != '_' || key == "_attachments" || key == "_deleted" {
			stripped[key] = value
		}
	}
    return stripped
}

func canonicalEncoding(body Body) []byte {
	encoded, err := json.Marshal(body)	//FIX: Use canonical JSON encoder
    if err != nil {
        panic(fmt.Sprintf("Couldn't encode body %v", body))
    }
    return encoded
}
