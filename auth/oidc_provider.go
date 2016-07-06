//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package auth

import (
	"fmt"
	"net/url"

	"github.com/coreos/go-oidc/oidc"
)

type OidcProviderConfiguration struct {
	Issuer                 string   `json:"issuer"`
	AuthEndpoint           string   `json:"authorization_endpoint"`
	TokenEndpoint          string   `json:"token_endpoint"`
	JwksUri                string   `json:"jwks_uri"`
	UserInfoEndpoint       string   `json:"userinfo_endpoint,omitempty"`
	RegistrationEndpoint   string   `json:"registration_endpoint,omitempty"`
	ResponseTypesSupported []string `json:"response_types_supported,omitempty"`
	SubjectTypesSupported  []string `json:"subject_types_supported,omitempty"`
	ScopesSupported        []string `json:"scopes_supported,omitempty"`
	ClaimsSupported        []string `json:"claims_supported,omitempty"`

	ResponseModesSupported []string `json:"response_modes_supported,omitempty"`
	GrantTypesSupported    []string `json:"grant_types_supported,omitempty"`
	ACRValuesSupported     []string `json:"acr_values_supported,omitempty"`

	IDTokenSigningAlgValues     []string `json:"id_token_signing_alg_values_supported,omitempty"`
	IDTokenEncryptionAlgValues  []string `json:"id_token_encryption_alg_values_supported,omitempty"`
	IDTokenEncryptionEncValues  []string `json:"id_token_encryption_enc_values_supported,omitempty"`
	UserInfoSigningAlgValues    []string `json:"userinfo_signing_alg_values_supported,omitempty"`
	UserInfoEncryptionAlgValues []string `json:"userinfo_encryption_alg_values_supported,omitempty"`
	UserInfoEncryptionEncValues []string `json:"userinfo_encryption_enc_values_supported,omitempty"`
	ReqObjSigningAlgValues      []string `json:"request_object_signing_alg_values_supported,omitempty"`
	ReqObjEncryptionAlgValues   []string `json:"request_object_encryption_alg_values_supported,omitempty"`
	ReqObjEncryptionEncValues   []string `json:"request_object_encryption_enc_values_supported,omitempty"`

	TokenEndpointAuthMethodsSupported          []string `json:"token_endpoint_auth_methods_supported,omitempty"`
	TokenEndpointAuthSigningAlgValuesSupported []string `json:"token_endpoint_auth_signing_alg_values_supported,omitempty"`

	DisplayValuesSupported        []string `json:"display_values_supported,omitempty"`
	ClaimTypesSupported           []string `json:"claim_types_supported,omitempty"`
	ServiceDocs                   string   `json:"service_documentation,omitempty"`
	ClaimsLocalsSupported         []string `json:"claims_locales_supported,omitempty"`
	UILocalsSupported             []string `json:"ui_locales_supported,omitempty"`
	ClaimsParameterSupported      bool     `json:"claims_parameter_supported,omitempty"`
	RequestParameterSupported     bool     `json:"request_parameter_supported,omitempty"`
	RequestURIParamaterSupported  bool     `json:"request_uri_parameter_supported,omitempty"`
	RequireRequestURIRegistration bool     `json:"require_request_uri_registration,omitempty"`

	Policy         string `json:"op_policy_uri,omitempty"`
	TermsOfService string `json:"op_tos_uri,omitempty"`
}

// Converts a provider config (based on the OpenID Connect spec) to the type used by coreos/go-oidc.  Used to handle scenarios
// where the provider doesn't adhere to spec.
func (pc OidcProviderConfiguration) AsProviderConfig() (oidc.ProviderConfig, error) {
	conf := oidc.ProviderConfig{
		ScopesSupported:                            pc.ScopesSupported,
		ResponseTypesSupported:                     pc.ResponseTypesSupported,
		ResponseModesSupported:                     pc.ResponseModesSupported,
		GrantTypesSupported:                        pc.GrantTypesSupported,
		ACRValuesSupported:                         pc.ACRValuesSupported,
		SubjectTypesSupported:                      pc.SubjectTypesSupported,
		IDTokenSigningAlgValues:                    pc.IDTokenSigningAlgValues,
		IDTokenEncryptionAlgValues:                 pc.IDTokenEncryptionAlgValues,
		IDTokenEncryptionEncValues:                 pc.IDTokenEncryptionEncValues,
		UserInfoSigningAlgValues:                   pc.UserInfoSigningAlgValues,
		UserInfoEncryptionAlgValues:                pc.UserInfoEncryptionAlgValues,
		UserInfoEncryptionEncValues:                pc.UserInfoEncryptionEncValues,
		ReqObjSigningAlgValues:                     pc.ReqObjSigningAlgValues,
		ReqObjEncryptionAlgValues:                  pc.ReqObjEncryptionAlgValues,
		ReqObjEncryptionEncValues:                  pc.ReqObjEncryptionEncValues,
		TokenEndpointAuthMethodsSupported:          pc.TokenEndpointAuthMethodsSupported,
		TokenEndpointAuthSigningAlgValuesSupported: pc.TokenEndpointAuthSigningAlgValuesSupported,
		DisplayValuesSupported:                     pc.DisplayValuesSupported,
		ClaimTypesSupported:                        pc.ClaimTypesSupported,
		ClaimsSupported:                            pc.ClaimsSupported,
		ClaimsLocalsSupported:                      pc.ClaimsLocalsSupported,
		UILocalsSupported:                          pc.UILocalsSupported,
		ClaimsParameterSupported:                   pc.ClaimsParameterSupported,
		RequestParameterSupported:                  pc.RequestParameterSupported,
		RequestURIParamaterSupported:               pc.RequestURIParamaterSupported,
		RequireRequestURIRegistration:              pc.RequireRequestURIRegistration,
	}

	var err error
	if conf.Issuer, err = pc.parseURI(pc.Issuer); err != nil {
		return oidc.ProviderConfig{}, err
	}
	if conf.AuthEndpoint, err = pc.parseURI(pc.AuthEndpoint); err != nil {
		return oidc.ProviderConfig{}, err
	}
	if conf.TokenEndpoint, err = pc.parseURI(pc.TokenEndpoint); err != nil {
		return oidc.ProviderConfig{}, err
	}
	if conf.UserInfoEndpoint, err = pc.parseURI(pc.UserInfoEndpoint); err != nil {
		return oidc.ProviderConfig{}, err
	}
	if conf.KeysEndpoint, err = pc.parseURI(pc.JwksUri); err != nil {
		return oidc.ProviderConfig{}, err
	}
	if conf.RegistrationEndpoint, err = pc.parseURI(pc.RegistrationEndpoint); err != nil {
		return oidc.ProviderConfig{}, err
	}
	if conf.Policy, err = pc.parseURI(pc.Policy); err != nil {
		return oidc.ProviderConfig{}, err
	}
	if conf.Policy, err = pc.parseURI(pc.Policy); err != nil {
		return oidc.ProviderConfig{}, err
	}
	if conf.TermsOfService, err = pc.parseURI(pc.TermsOfService); err != nil {
		return oidc.ProviderConfig{}, err
	}
	if conf.ServiceDocs, err = pc.parseURI(pc.ServiceDocs); err != nil {
		return oidc.ProviderConfig{}, err
	}

	return conf, nil
}

func (pc *OidcProviderConfiguration) parseURI(s string) (*url.URL, error) {

	if s == "" {
		return nil, nil
	}
	u, err := url.Parse(s)
	if err == nil {
		if u.Host == "" {
			return nil, fmt.Errorf("Host required in URI [%s]:", s)
		} else if u.Scheme != "http" && u.Scheme != "https" {
			return nil, fmt.Errorf("Invalid URI scheme [%s]:", s)
		}
	}
	return u, nil
}
