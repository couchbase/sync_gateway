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

	"github.com/couchbase/gocbcore/v10/connstr"
)

const (
	dcpBufferSizeKey = "dcp_buffer_size"
	kvBufferSizeKey  = "kv_buffer_size"
	kvPoolSizeKey    = "kv_pool_size"
	networkKey       = "network"
)

// GoCBConnStringParams represents parameters that are passed to gocb when creating a new connection string.
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

// getGoCBConnSpec returns a gocb connection spec based on the server string. The provided defaults will be used only when the corresponding property is not set in the connection string.
func getGoCBConnSpec(server string, defaults *GoCBConnStringParams) (*connstr.ConnSpec, error) {
	connSpec, err := connstr.Parse(server)
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
	asValues.Set("max_idle_http_connections", fmt.Sprintf("%d", DefaultHttpMaxIdleConns))
	asValues.Set("idle_http_connection_timeout", fmt.Sprintf("%d", DefaultHttpIdleConnTimeout.Milliseconds()))

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

// getConnSpecOption returns a single query parameter value from a connstr.ConnSpec, converted to type T (string or
// int). If the option isn't set, returns nil and no error. Returns an error if the option is set more than once, or
// if the value can't be converted to T.
func getConnSpecOption[T string | int](spec *connstr.ConnSpec, key string) (*T, error) {
	arg := spec.Options[key]
	if len(arg) == 0 {
		return nil, nil
	}
	if len(arg) > 1 {
		return nil, fmt.Errorf("multiple %s values found in connection string %q", key, spec.String())
	}

	var value any
	switch any(*new(T)).(type) {
	case int:
		i, err := strconv.Atoi(arg[0])
		if err != nil {
			return nil, fmt.Errorf("invalid %s value %s in connection string %q, must be int", key, arg[0], spec.String())
		}
		value = i
	case string:
		value = arg[0]
	}
	typed := value.(T)
	return &typed, nil
}

// getIntFromConnStr returns a query parameter from a connection string. If it doesn't exist,  return nil and no error. If there's an error in parsing the connection string, return an error.
func getIntFromConnStr(server, key string) (*int, error) {
	connSpec, err := getGoCBConnSpec(server, nil)
	if err != nil {
		return nil, err
	}
	return getConnSpecOption[int](connSpec, key)
}
