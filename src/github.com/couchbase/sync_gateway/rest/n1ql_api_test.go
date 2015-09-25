//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	// "encoding/json"
	"fmt"
	// "net/http"
	"testing"

	// "github.com/couchbase/sg-bucket"
	// "github.com/couchbase/sync_gateway/channels"

	// "github.com/couchbaselabs/go.assert"
)

func TestSelectStarQuery(t *testing.T) {

	rt := restTester{
		syncFn: `function(doc) {channel(doc.channel)}`,
		Queries: map[string]string{
			"all": "SELECT field FROM db",
		}}

		response := rt.sendAdminRequest("GET", "/db/_n1ql/all", ``)
		assertStatus(t, response, 200)
		// why does this 200 when underlying engine doesn't support n1ql?
		fmt.Println(response.Body)


	// var result sgbucket.ViewResult

	// // Admin view query:
	// response = rt.sendAdminRequest("GET", "/db/_design/foo/_view/bar?reduce=true", ``)
	// assertStatus(t, response, 200)
	// json.Unmarshal(response.Body.Bytes(), &result)

	// // we should get 1 row with the reduce result
	// assert.Equals(t, len(result.Rows), 1)
	// row := result.Rows[0]
	// value := row.Value.(float64)
	// assert.Equals(t, value, 108.0)
}
