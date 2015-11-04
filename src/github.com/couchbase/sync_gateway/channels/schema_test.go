package channels

import (
	"github.com/couchbaselabs/go.assert"
	"testing"
)

func TestValidateDoc(t *testing.T) {
	//test it validates when it should
	url := "test_schema.json"
	doc := "{\"hello\": 45}"
	schemata := map[string]SchemaWrapper{}
	valid, _ := validate(schemata, doc, url)
	assert.True(t, valid)
}


func TestValidateDocFails(t *testing.T) {
	//and fails if the type is wrong
	url := "test_schema.json"
	doc := "{\"hello\": \"fail\"}"
	schemata := map[string]SchemaWrapper{}
	valid, _ := validate(schemata, doc, url)
	assert.False(t, valid)
}
