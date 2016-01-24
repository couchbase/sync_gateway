//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strconv"
	"testing"

	"github.com/couchbaselabs/go.assert"
)

func unjson(j string) Body {
	var body Body
	err := json.Unmarshal([]byte(j), &body)
	if err != nil {
		panic(fmt.Sprintf("Invalid JSON: %v", err))
	}
	return body
}

func tojson(obj interface{}) string {
	j, _ := json.Marshal(obj)
	return string(j)
}

func TestAttachments(t *testing.T) {
	context, err := NewDatabaseContext("db", testBucket(), false, DatabaseContextOptions{})
	assertNoError(t, err, "Couldn't create context for database 'db'")
	defer context.Close()
	db, err := CreateDatabase(context)
	assertNoError(t, err, "Couldn't create database 'db'")

	// Test creating & updating a document:
	log.Printf("Create rev 1...")
	rev1input := `{"_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="},
                                    "bye.txt": {"data":"Z29vZGJ5ZSBjcnVlbCB3b3JsZA=="}}}`
	var body Body
	json.Unmarshal([]byte(rev1input), &body)
	revid, err := db.Put("doc1", unjson(rev1input))
	rev1id := revid
	assertNoError(t, err, "Couldn't create document")

	log.Printf("Retrieve doc...")
	rev1output := `{"_attachments":{"bye.txt":{"data":"Z29vZGJ5ZSBjcnVlbCB3b3JsZA==","digest":"sha1-l+N7VpXGnoxMm8xfvtWPbz2YvDc=","length":19,"revpos":1},"hello.txt":{"data":"aGVsbG8gd29ybGQ=","digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":1}},"_id":"doc1","_rev":"1-54f3a105fb903018c160712ffddb74dc"}`
	gotbody, err := db.GetRev("doc1", "", false, []string{})
	assertNoError(t, err, "Couldn't get document")
	assert.Equals(t, tojson(gotbody), rev1output)

	log.Printf("Create rev 2...")
	rev2str := `{"_attachments": {"hello.txt": {"stub":true, "revpos":1}, "bye.txt": {"data": "YnllLXlh"}}}`
	var body2 Body
	json.Unmarshal([]byte(rev2str), &body2)
	body2["_rev"] = revid
	revid, err = db.Put("doc1", body2)
	assertNoError(t, err, "Couldn't update document")
	assert.Equals(t, revid, "2-08b42c51334c0469bd060e6d9e6d797b")

	log.Printf("Retrieve doc...")
	rev2output := `{"_attachments":{"bye.txt":{"data":"YnllLXlh","digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A=","length":6,"revpos":2},"hello.txt":{"data":"aGVsbG8gd29ybGQ=","digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":1}},"_id":"doc1","_rev":"2-08b42c51334c0469bd060e6d9e6d797b"}`
	gotbody, err = db.GetRev("doc1", "", false, []string{})
	assertNoError(t, err, "Couldn't get document")
	assert.Equals(t, tojson(gotbody), rev2output)

	log.Printf("Retrieve doc with atts_since...")
	rev2Aoutput := `{"_attachments":{"bye.txt":{"data":"YnllLXlh","digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A=","length":6,"revpos":2},"hello.txt":{"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":1,"stub":true}},"_id":"doc1","_rev":"2-08b42c51334c0469bd060e6d9e6d797b"}`
	gotbody, err = db.GetRev("doc1", "", false, []string{"1-54f3a105fb903018c160712ffddb74dc", "1-foo", "993-bar"})
	assertNoError(t, err, "Couldn't get document")
	assert.Equals(t, tojson(gotbody), rev2Aoutput)

	log.Printf("Create rev 3...")
	rev3str := `{"_attachments": {"bye.txt": {"stub":true,"revpos":2}}}`
	var body3 Body
	json.Unmarshal([]byte(rev3str), &body3)
	body3["_rev"] = revid
	revid, err = db.Put("doc1", body3)
	assertNoError(t, err, "Couldn't update document")
	assert.Equals(t, revid, "3-252b9fa1f306930bffc07e7d75b77faf")

	log.Printf("Retrieve doc...")
	rev3output := `{"_attachments":{"bye.txt":{"data":"YnllLXlh","digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A=","length":6,"revpos":2}},"_id":"doc1","_rev":"3-252b9fa1f306930bffc07e7d75b77faf"}`
	gotbody, err = db.GetRev("doc1", "", false, []string{})
	assertNoError(t, err, "Couldn't get document")
	assert.Equals(t, tojson(gotbody), rev3output)

	log.Printf("Expire body of rev 1, then add a child...") // test fix of #498
	err = db.Bucket.Delete(oldRevisionKey("doc1", rev1id))
	assertNoError(t, err, "Couldn't compact old revision")
	rev2Bstr := `{"_attachments": {"bye.txt": {"stub":true,"revpos":1,"digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A="}}, "_rev": "2-f000"}`
	var body2B Body
	err = json.Unmarshal([]byte(rev2Bstr), &body2B)
	assertNoError(t, err, "bad JSON")
	err = db.PutExistingRev("doc1", body2B, []string{"2-f000", rev1id})
	assertNoError(t, err, "Couldn't update document")
}

// https://github.com/couchbase/couchbase-lite-ios/issues/1053
func TestReadMultipartBody(t *testing.T) {

	// This raw request body was captured by adding the following code to a Go REST server endpoint
	// which receives a PUT request with 4 multipart/related parts: 1 JSON and 3 images
	//
	//   raw, err := ioutil.ReadAll(rq.Body)
	//   ioutil.WriteFile("/tmp/multipart.data", raw, 0644)
	//
	// The request was also captured by tcpdump, and when the MIME multipart/related
	// section is displayed in wireshark, it shows 4 multipart/related parts as shown in this gist:
	//
	//    https://gist.github.com/tleyden/64def2e6c7e668247fec
	dataUrl := "http://couchbase-mobile.s3.amazonaws.com/misc/debug-push-attachment/multipart.data"

	resp, err := http.Get(dataUrl)
	if err != nil {
		t.Fatalf("err doing Get for multipart request snapshot: %v", err)
	}
	defer resp.Body.Close()
	raw, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("err reading body of multipart request snapshot: %v", err)
	}

	// this is the boundary string for the above multipart data
	boundary := "-9PWGbT-1_r1QpgvW0I-4E3"

	reader := multipart.NewReader(bytes.NewReader(raw), boundary)

	// loop over parts
	numParts := 0
	for {
		part, err := reader.NextPart()
		if err != nil {
			log.Printf("err getting nextpart: %v", err)
			break
		}
		data, err := ioutil.ReadAll(part)
		if err != nil {
			log.Printf("err getting data from part: %v", err)
			break
		}

		contentLengthStr := part.Header["Content-Length"][0]
		contentLength, err := strconv.Atoi(contentLengthStr)
		if err != nil {
			log.Printf("err reading content length mime header: %v", err)
			break
		}

		numParts += 1

		if len(data) != contentLength {
			log.Printf("Warning: part length %v != content length header: %v", len(data), contentLength)
		}

		if bytes.Contains(data, []byte(boundary)) {
			t.Errorf("Part contains the boundary, which is not expected")
			break
		}

		err = part.Close()
		if err != nil {
			log.Printf("err closing part: %v", err)
			break
		}

	}
	if numParts != 4 {
		t.Errorf("Expected 4 parts, got: %v", numParts)
	}

}

func TestReadMultipartBody2(t *testing.T) {

	// start a webserver listening on localhost:8080
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, %q", html.EscapeString(r.URL.Path))
		log.Printf("Hello.........")
		contentType, attrs, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if contentType != "multipart/related" {
			log.Panicf("unexpected content type: %v", contentType)
		}
		log.Printf("attrs: %v", attrs)
		raw, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Panicf("Error reading request body: %v", err)
		}
		boundary := attrs["boundary"]
		reader := multipart.NewReader(bytes.NewReader(raw), boundary)

		// loop over parts
		numParts := 0
		for {
			part, err := reader.NextPart()
			if err != nil {
				log.Printf("err getting nextpart: %v", err)
				break
			}
			log.Printf("Part: %v", part)
			data, err := ioutil.ReadAll(part)
			if err != nil {
				log.Printf("err getting data from part: %v", err)
				break
			}
			log.Printf("Part data length: %v", len(data))

			numParts += 1

			if bytes.Contains(data, []byte(boundary)) {
				t.Errorf("Part contains the boundary, which is not expected")
				break
			}

			err = part.Close()
			if err != nil {
				log.Printf("err closing part: %v", err)
				break
			}

		}

	})

	go func() {
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	mimeHeader := textproto.MIMEHeader{}
	mimeHeader.Set("Content-Type", "application/json")

	part, err := writer.CreatePart(mimeHeader)
	if err != nil {
		t.Fatalf("err creating part: %v", err)
	}

	_, err = part.Write([]byte("{}"))
	if err != nil {
		t.Fatalf("err writing part: %v", err)
	}

	filename := "result_image"
	imageUrl := "http://couchbase-mobile.s3.amazonaws.com/misc/debug-push-attachment/result_image.png"
	if err := createPart(writer, filename, imageUrl); err != nil {
		t.Fatalf("err creating part: %v", err)
	}

	filename = "source_image"
	imageUrl = "http://couchbase-mobile.s3.amazonaws.com/misc/debug-push-attachment/source_image.jpg"
	if err := createPart(writer, filename, imageUrl); err != nil {
		t.Fatalf("err creating part: %v", err)
	}

	filename = "style_image"
	imageUrl = "http://couchbase-mobile.s3.amazonaws.com/misc/debug-push-attachment/style_image.jpg"
	if err := createPart(writer, filename, imageUrl); err != nil {
		t.Fatalf("err creating part: %v", err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("err closing writer: %v", err)
	}

	// create a client
	client := &http.Client{}

	// create POST request
	// apiUrl := "http://ec2-54-145-244-2.compute-1.amazonaws.com:4985/deepstyle-cc/test-doc"
	apiUrl := "http://localhost:8080"
	req, err := http.NewRequest("PUT", apiUrl, bytes.NewReader(body.Bytes()))
	if err != nil {
		t.Fatalf("err creating request: %v", err)
	}

	// set content type
	contentType := fmt.Sprintf("multipart/related; boundary=%q", writer.Boundary())
	req.Header.Set("Content-Type", contentType)
	log.Printf("Writer using boundary: %v", writer.Boundary())

	// send POST request
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("err sending request: %v", err)
	}
	defer resp.Body.Close()
	log.Printf("response status code: %v", resp.StatusCode)

}

func createPart(writer *multipart.Writer, filename, imageUrl string) error {

	partHeaders := textproto.MIMEHeader{}

	partHeaders.Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%v\".", filename))

	partAttach, err := writer.CreatePart(partHeaders)
	if err != nil {
		return fmt.Errorf("err creating part: %v", err)
	}

	resp, err := http.Get(imageUrl)
	if err != nil {
		return fmt.Errorf("err doing get: %v", err)
	}
	defer resp.Body.Close()

	_, err = io.Copy(partAttach, resp.Body)
	if err != nil {
		return fmt.Errorf("err writing image multipart part: %v", err)
	}
	return nil

}
