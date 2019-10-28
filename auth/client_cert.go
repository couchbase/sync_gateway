//  Copyright (c) 2019 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package auth

import (
	"crypto/x509"
	"fmt"
	"regexp"
	"strings"
)

// A pattern for extracting usernames from X.509 certificates. Found in the config file.
// The behavior is compatible with Couchbase Server's client cert configuration
// <https://docs.google.com/document/d/1sC_He6DZdiZBw63jIvOdwqD_2uGAkELzSA1BqxGuNCA/>
// extended with an optional regular-expression mode that's more powerful.
type ClientCertNamePattern struct {
	Path       string  `json:"path,omitempty"`       // Identifies which field of the cert to look in
	Prefix     *string `json:"prefix,omitempty"`     // Optional prefix to skip before the username
	Delimiters *string `json:"delimiters,omitempty"` // Optional delimiter to end the username at
	Regex      *string `json:"regex,omitempty"`      // Optional regex to match; the 1st submatch (group) is the username

	regex *regexp.Regexp
}

// Looks up a User from a client X.509 certificate, based on the rules in the patterns.
func (auth *Authenticator) GetUserByCertificate(clientCert x509.Certificate, patterns []ClientCertNamePattern) (User, error) {
	for _, pattern := range patterns {
		if username := pattern.Match(clientCert); username != nil {
			user, err := auth.GetUser(*username)
			if user == nil && err == nil {
				err = fmt.Errorf("Unknown username '%s' in X.509 client certificate", *username)
			}
			return user, err
		}
	}
	return nil, fmt.Errorf("No username found in X.509 client certificate")
}

// Checks whether the pattern parameters are correct.
func (pat *ClientCertNamePattern) Validate() error {
	if pat.Path != "subject.cn" && pat.Path != "san.uri" && pat.Path != "san.email" && pat.Path != "san.dns" {
		return fmt.Errorf(`unknown "path" value "%s"`, pat.Path)
	}
	if pat.Prefix != nil || pat.Delimiters != nil {
		if pat.Regex != nil {
			return fmt.Errorf(`"regex" cannot be combined with "prefix" or "delimiters"`)
		}
		return nil
	} else if pat.Regex != nil {
		// as a side effect, this compiles the Regexp
		var err error
		pat.regex, err = regexp.Compile(*pat.Regex)
		if pat.regex == nil {
			return err
		}
		return nil
	} else {
		return nil
	}
}

// Given a cert and a pattern, return a matching username or nil
func (pat *ClientCertNamePattern) Match(cert x509.Certificate) *string {
	for _, name := range pat.findNames(cert) {
		if username := pat.match(name); username != nil {
			return username
		}
	}
	return nil
}

// Get the name(s) from the cert based on my Path property:
func (pat *ClientCertNamePattern) findNames(cert x509.Certificate) []string {
	switch pat.Path {
	case "subject.cn":
		return []string{cert.Subject.CommonName}
	case "san.uri":
		names := make([]string, len(cert.URIs))
		for i, uri := range cert.URIs {
			names[i] = uri.String()
		}
		return names
	case "san.email":
		return cert.EmailAddresses
	case "san.dns":
		return cert.DNSNames
	default:
		return nil
	}
}

// Match a name
func (pat *ClientCertNamePattern) match(name string) *string {
	if pat.Regex != nil {
		return pat.matchByRegex(name)
	} else {
		return pat.matchByPrefix(name)
	}
}

// Use .Prefix and/or .Delimiters to match a name
func (pat *ClientCertNamePattern) matchByPrefix(name string) *string {
	if pat.Prefix != nil {
		if suffix := strings.TrimPrefix(name, *pat.Prefix); suffix != name {
			name = suffix
		} else {
			return nil // Prefix must be matched
		}
	}
	if pat.Delimiters != nil {
		if end := strings.IndexAny(name, *pat.Delimiters); end >= 0 {
			name = name[0:end]
		}
		// Delimiters don't need to be matched
	}
	if len(name) == 0 {
		return nil
	}
	return &name
}

// Use .Regex to match a name
func (pat *ClientCertNamePattern) matchByRegex(name string) *string {
	if pat.regex == nil {
		pat.regex = regexp.MustCompile(*pat.Regex) // (usually this happens during Validate)
	}
	m := pat.regex.FindStringSubmatchIndex(name)
	if len(m) >= 4 && m[2] >= 0 && m[3] > m[2] {
		// Return the first submatch, i.e. '(...)' group in the RE
		username := name[m[2]:m[3]]
		return &username
	}
	return nil
}
