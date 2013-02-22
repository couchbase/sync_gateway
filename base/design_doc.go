package base

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"

	"github.com/couchbaselabs/go-couchbase"
)

type ViewDef struct {
	Map    string `json:"map"`
	Reduce string `json:"reduce,omitempty"`
}

type ViewMap map[string]ViewDef

type DesignDocOptions struct {
	LocalSeq      bool `json:"local_seq,omitempty"`
	IncludeDesign bool `json:"include_design,omitempty"`
}

// A Couchbase design document, which stores map/reduce function definitions.
type DesignDoc struct {
	Language string            `json:"language,omitempty"`
	Views    ViewMap           `json:"views,omitempty"`
	Options  *DesignDocOptions `json:"options,omitempty"`
}

// Saves a design document to a bucket.
func (ddoc *DesignDoc) Put(bucket *couchbase.Bucket, designDocName string) error {
	node := bucket.Nodes[rand.Intn(len(bucket.Nodes))]
	u, err := url.Parse(node.CouchAPIBase)
	if err != nil {
		fmt.Printf("Failed to parse %s", node.CouchAPIBase)
		return err
	}
	u.Path = fmt.Sprintf("/%s/_design/%s", bucket.Name, designDocName)

	payload, err := json.Marshal(ddoc)
	if err != nil {
		return err
	}

	rq, err := http.NewRequest("PUT", u.String(), bytes.NewBuffer(payload))
	rq.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(rq)

	if err == nil && response.StatusCode > 299 {
		err = &HTTPError{Status: response.StatusCode, Message: response.Status}
	}
	return err
}
