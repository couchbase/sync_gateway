// revision.go

package basecouch

import (
	"crypto/md5"
	"encoding/json"
    "fmt"
    "log"
)

// The body of a CouchDB document/revision as decoded from JSON.
type Body map[string]interface{}

func (db *Database) getRevision(docid, revid string) (Body, error) {
	var body Body
	err := db.bucket.Get(keyForRevID(revid), &body)
	if err != nil {
		return nil, err
	}
	if body["_id"] != docid || body["_rev"] != revid {
        panic(fmt.Sprintf("getRevision got wrong revision! expected %s/%s; got %s/%s",
                docid, revid, body["_id"], body["_rev"]))
    }
	return body, nil
}


func (db *Database) setRevision(docid, revid string, body Body) error {
	return db.bucket.Set(keyForRevID(revid), 0, body)
}

//////// HELPERS:

func keyForRevID(revid string) string {
    return "rev:" + revid
}

func createRevID(generation int, parentRevID string, body Body) string {
	//FIX: Use canonical JSON encoding
	json, _ := json.Marshal(body)
	digester := md5.New()
    digester.Write([]byte{byte(len(parentRevID))})
    digester.Write([]byte(parentRevID))
	digester.Write(json)
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

var kPreserveProperties = map[string]bool {
    "_id": true, "_rev": true, "_deleted": true, "_attachments": true}

// Removes properties we don't want to store in the revision's body
func stripSpecialProperties(body Body) {
	for key, _ := range body {
		if key[0] == '_' && !kPreserveProperties[key] {
			delete(body, key)
		}
	}
}
