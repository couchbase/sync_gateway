package rest

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"log"
	"net/http"
	"testing"
)

func TestGetAlldocChannels(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel);}`})
	defer rt.Close()

	version := rt.PutDoc("doc", `{"channel":["CHAN1"]}`)
	updatedVersion := rt.UpdateDoc("doc", version, `{"channel":["CHAN2"]}`)
	updatedVersion = rt.UpdateDoc("doc", updatedVersion, `{"channel":["CHAN1"]}`)
	updatedVersion = rt.UpdateDoc("doc", updatedVersion, `{"channel":["CHAN1", "CHAN2"]}`)
	updatedVersion = rt.UpdateDoc("doc", updatedVersion, `{"channel":["CHAN3"]}`)
	updatedVersion = rt.UpdateDoc("doc", updatedVersion, `{"channel":["CHAN1"]}`)

	response := rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/doc/_all_channels", "")
	RequireStatus(t, response, http.StatusOK)
	log.Printf(response.BodyString())
	var channelMap map[string][]string
	err := json.Unmarshal(response.BodyBytes(), &channelMap)
	assert.NoError(t, err)
	assert.ElementsMatch(t, channelMap["CHAN1"], []string{"6-0", "1-2", "3-5"})
	assert.ElementsMatch(t, channelMap["CHAN2"], []string{"4-5", "2-3"})
	assert.ElementsMatch(t, channelMap["CHAN3"], []string{"5-6"})

}
