//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package auth

// Config options for Json Web Token validation
type JWTOptions struct {
	ValidationKey *string `json:"validation_key"`           // Key used to validate signed tokens
	SigningMethod *string `json:"signing_method,omitempty"` // Algorithm used for signing.  Can be specified for additional security to handle scenario described here: https://auth0.com/blog/2015/03/31/critical-vulnerabilities-in-json-web-token-libraries/
}
