//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package auth

// Options for OpenID Connect
type OIDCOptions struct {
	JWTOptions
	DiscoveryURL *string `json:"discovery_url,omitempty"` // OIDC OP discovery endpoint.  If present, SG will try to retrieve token and authorize endpoints from here.
	AuthorizeURL *string `json:"authorize_url,omitempty"` // OIDC OP authorize endpoint.
	TokenURL     *string `json:"token_url,omitempty"`     // OIDC OP token endpoint.
	ClientID     *string `json:"client_id",omitempty"`    // Client ID
	Register     bool    `json:"register"`                // If true, server will register new user accounts
}
