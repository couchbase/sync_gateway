// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/couchbaselabs/gocbconnstr"
)

const (
	dcpBufferSizeKey = "dcp_buffer_size"
	kvBufferSizeKey  = "kv_buffer_size"
	kvPoolSizeKey    = "kv_pool_size"
)

// GoCBConnStringParams represents parameters that are passed to gocb when creating a new connection string. These are the subset of values that are changed when running with serverless mode.
type GoCBConnStringParams struct {
	KvPoolSize    int // corresponds to kv_pool_size
	KvBufferSize  int // corresponds to kv_buffer_size
	DcpBufferSize int // corresponds to dcp_buffer_size
}

// DefaultGoCBConnStringParams returns a GoCBConnStringParams with the default values, suitable for general connections.
func DefaultGoCBConnStringParams() *GoCBConnStringParams {
	return &GoCBConnStringParams{
		KvPoolSize:    DefaultGocbKvPoolSize,
		KvBufferSize:  0,
		DcpBufferSize: 0,
	}
}

// DefaultServerlessGoCBConnStringParams returns a GoCBConnStringParams with the default values for serverless deployments.
func DefaultServerlessGoCBConnStringParams() *GoCBConnStringParams {
	return &GoCBConnStringParams{
		KvPoolSize:    DefaultGocbKvPoolSizeServerless,
		KvBufferSize:  DefaultKvBufferSizeServerless,
		DcpBufferSize: DefaultDCPBufferServerless,
	}
}

// getGoCBConnSpec returns a gocb connection spec based on the server string. The provided defaults will be used only when the corresponding property is not set in the connection string.
func getGoCBConnSpec(server string, defaults *GoCBConnStringParams) (*gocbconnstr.ConnSpec, error) {
	connSpec, err := gocbconnstr.Parse(server)
	if err != nil {
		return nil, err
	}

	if connSpec.Options == nil {
		connSpec.Options = map[string][]string{}
	}
	asValues := url.Values(connSpec.Options)

	if defaults != nil {
		poolSizeFromConnStr := asValues.Get(kvPoolSizeKey)
		if poolSizeFromConnStr == "" {
			asValues.Set(kvPoolSizeKey, strconv.Itoa(defaults.KvPoolSize))
		}

		kvBufferfromConnStr := asValues.Get(kvBufferSizeKey)
		if kvBufferfromConnStr == "" && defaults.KvBufferSize != 0 {
			asValues.Set(kvBufferSizeKey, strconv.Itoa(defaults.KvBufferSize))
		}

		dcpBufferfromConnStr := asValues.Get(dcpBufferSizeKey)
		if dcpBufferfromConnStr == "" && defaults.DcpBufferSize != 0 {
			asValues.Set(dcpBufferSizeKey, strconv.Itoa(defaults.DcpBufferSize))
		}
	}
	asValues.Set("max_perhost_idle_http_connections", strconv.Itoa(DefaultHttpMaxIdleConnsPerHost))
	asValues.Set("max_idle_http_connections", DefaultHttpMaxIdleConns)
	asValues.Set("idle_http_connection_timeout", DefaultHttpIdleConnTimeoutMilliseconds)

	connSpec.Options = asValues
	return &connSpec, nil
}

// GetGoCBConnString builds a gocb connection string based on server string. This is used to set a new connection string
func GetGoCBConnStringWithDefaults(server string, defaults *GoCBConnStringParams) (string, error) {
	connSpec, err := getGoCBConnSpec(server, defaults)
	if err != nil {
		return "", err
	}
	return connSpec.String(), nil
}

// getKvPoolSize returns the kv_pool_size from the connection string, if it exists. If it doesn't exist, return nil, or an error if the string is not parseable.
func getKvPoolSize(server string) (*int, error) {
	connSpec, err := getGoCBConnSpec(server, nil)
	if err != nil {
		return nil, err
	}

	values := url.Values(connSpec.Options)

	kvPoolSizeArg := values[kvPoolSizeKey]

	if len(kvPoolSizeArg) == 0 {
		return nil, nil
	} else if len(kvPoolSizeArg) > 1 {
		return nil, fmt.Errorf("Multiple kv_pool_size values found in connection string %s", server)
	}

	kvPoolSize := kvPoolSizeArg[0]
	kvPoolSizeInt, err := strconv.Atoi(kvPoolSize)
	if err != nil {
		return nil, fmt.Errorf("Invalid kv_pool_size value %s in connection string %s, must be int", kvPoolSize, server)
	}
	return &kvPoolSizeInt, nil
}
