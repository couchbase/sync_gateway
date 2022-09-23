package main

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

func mustParseJSON(t *testing.T, val string) map[string]any {
	var ret map[string]any
	err := base.JSONUnmarshal([]byte(val), &ret)
	require.NoError(t, err)
	return ret
}

func TestRemovePasswordsAndTagUserData(t *testing.T) {
	data := mustParseJSON(t, `{
	"databases": {
		"foo": {
			"bucket": "foo",
			"username": "Administrator",
			"password": "longpassword",
			"server": "couchbase://foo:bar@cbserver:8091/invalid",
			"roles": {
				"bar": {
					"admin_channels": ["beans"]
				}
			},
			"users": {
				"GUEST": {
					"name": "guest",
					"password": "securepassword",
					"admin_channels": ["foo", "bar"],
					"admin_roles": ["baz"]
				}
			}
		}
	}
}`)
	require.NoError(t, RemovePasswordsAndTagUserData(data))
	require.Equal(t, mustParseJSON(t, `{
	"databases": {
		"foo": {
			"bucket": "foo",
			"username": "<ud>Administrator</ud>",
			"password": "*****",
			"server": "couchbase://foo:*****@cbserver:8091/invalid",
			"roles": {
				"<ud>bar</ud>": {
					"admin_channels": ["<ud>beans</ud>"]
				}
			},
			"users": {
				"<ud>GUEST</ud>": {
					"name": "<ud>guest</ud>",
					"password": "*****",
					"admin_channels": ["<ud>foo</ud>", "<ud>bar</ud>"],
					"admin_roles": ["<ud>baz</ud>"]
				}
			}
		}
	}
}`), data)
}
