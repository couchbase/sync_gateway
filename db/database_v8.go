//go:build cb_sg_v8
// +build cb_sg_v8

package db

// The default JavaScript engine if not otherwise specified in the DatabaseContextOptions.
const DefaultJavaScriptEngine = "V8" // Can be "Otto" or "V8"
