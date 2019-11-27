package rest

import (
	"log"
	"net/http"
	"testing"
)

func TestWriteMultipartDocument(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	reqHeaders := map[string]string{"Content-Type": "multipart/related; boundary=123"}
	bodyText := `--123
Content-Type: application/json

{"key":"foo","value":"bar"}
--123--`

	response := rt.SendRequestWithHeaders(http.MethodPut, "/db/doc1", bodyText, reqHeaders)
	assertStatus(t, response, http.StatusCreated)

	response = rt.SendRequestWithHeaders(http.MethodGet, "/db/doc1", "", reqHeaders)
	log.Printf("response: %v", string(response.BodyBytes()))
	assertStatus(t, response, http.StatusOK)
}
