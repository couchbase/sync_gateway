/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package functions

import (
	"context"
	_ "embed"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/robertkrimen/otto"
	_ "github.com/robertkrimen/otto/underscore"
)

//////// JS RUNNER

// Number of Otto contexts to cache per function, i.e. the number of goroutines that can be simultaneously running each function.
const kUserFunctionCacheSize = 2

// Maximum nesting of JS user function calls (not the JS stack depth)
var kUserFunctionMaxCallDepth = 20

// The outermost JavaScript code. Evaluating it returns a function, which is then called by the
// Runner every time it's invoked. Contains `%s` where the actual function source code goes.
//
//go:embed js_wrapper.js
var kJavaScriptWrapper string

// Creates a JSServer instance wrapping a userJSRunner, for user JS functions and GraphQL resolvers.
func newFunctionJSServer(ctx context.Context, name string, what string, sourceCode string) (*sgbucket.JSServer, error) {
	js := fmt.Sprintf(kJavaScriptWrapper, sourceCode)
	jsServer := sgbucket.NewJSServer(ctx, js, 0, kUserFunctionCacheSize,
		func(ctx context.Context, fnSource string, timeout time.Duration) (sgbucket.JSServerTask, error) {
			return newJSRunner(ctx, name, what, fnSource)
		})
	// Call WithTask to force a task to be instantiated, which will detect syntax errors in the script. Otherwise the error only gets detected the first time a client calls the function.
	var err error
	_, err = jsServer.WithTask(ctx, func(sgbucket.JSServerTask) (any, error) {
		return nil, nil
	})
	return jsServer, err
}

// An object that runs a user JavaScript function or a GraphQL resolver.
// Not thread-safe! Owned by an sgbucket.JSServer, which arbitrates access to it.
type jsRunner struct {
	sgbucket.JSRunner                 // "Superclass"
	kind              string          // "user function", "GraphQL resolver", etc
	name              string          // Name of this function or resolver
	currentDB         *db.Database    // db.Database instance (updated before every call)
	ctx               context.Context // Context (updated before every call)
}

// Creates a jsRunner given its name and JavaScript source code.
func newJSRunner(ctx context.Context, name string, kind string, funcSource string) (*jsRunner, error) {
	runner := &jsRunner{
		name: name,
		kind: kind,
	}
	err := runner.InitWithLogging("", 0,
		func(s string) {
			base.ErrorfCtx(ctx, base.KeyJavascript.String()+": %s %s: %s", kind, name, base.UD(s))
		},
		func(s string) {
			base.InfofCtx(ctx, base.KeyJavascript, "%s %s: %s", kind, name, base.UD(s))
		})
	if err != nil {
		return nil, err
	}

	// Define the `get`, `save`, etc. function callbacks:
	runner.defineNativeCallbacks()

	// Set (and compile) the JS function:
	if _, err := runner.JSRunner.SetFunction(funcSource); err != nil {
		return nil, base.HTTPErrorf(http.StatusInternalServerError, "Error compiling %s %q: %v", kind, name, err)
	}

	// Function that runs before every call:
	runner.Before = func() {
		if runner.currentDB == nil {
			panic("javaScriptRunner can't run without a currentDB")
		}
	}
	// Function that runs after every call:
	runner.After = func(jsResult otto.Value, err error) (any, error) {
		defer func() {
			runner.currentDB = nil
		}()
		if err != nil {
			base.ErrorfCtx(ctx, base.KeyJavascript.String()+": %s %s failed: %#v", runner.kind, runner.name, err)
			return nil, runner.convertError(err)
		}
		return jsResult.Export()
	}

	return runner, nil
}

type jsRunnerContextKey string

var ctxJSCallDepthKey = jsRunnerContextKey("call depth")

// Calls a javaScriptRunner's JavaScript function.
func (runner *jsRunner) CallWithDB(db *db.Database, ctx context.Context, jsArgs ...any) (result any, err error) {
	if ctx == nil {
		panic("missing context to userJSRunner")
	}

	var timeout time.Duration
	if deadline, exists := ctx.Deadline(); exists {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			return nil, sgbucket.ErrJSTimeout
		}
	}

	var depth int
	var ok bool
	if depth, ok = ctx.Value(ctxJSCallDepthKey).(int); ok {
		if depth >= kUserFunctionMaxCallDepth {
			base.ErrorfCtx(ctx, "User function recursion too deep, calling %s", runner.name)
			return nil, base.HTTPErrorf(http.StatusLoopDetected, "User function recursion too deep")
		}
	}
	ctx = context.WithValue(ctx, ctxJSCallDepthKey, depth+1)

	runner.SetTimeout(timeout)
	runner.currentDB = db
	runner.ctx = ctx
	return runner.Call(jsArgs...)
}

// JavaScript error returned by a jsRunner
type jsError struct {
	err        error
	runnerKind string
	runnerName string
}

func (jserr *jsError) Error() string {
	return fmt.Sprintf("%v (while calling %s %q)", jserr.err, jserr.runnerKind, jserr.runnerName)
}

func (jserr *jsError) Unwrap() error {
	return jserr.err
}

var HttpErrRE = regexp.MustCompile(`^HTTP:\s*(\d+)\s+(.*)`)

func (runner *jsRunner) convertError(err error) error {
	if err == sgbucket.ErrJSTimeout {
		return base.HTTPErrorf(408, "Timeout in JavaScript")
	}
	// Unfortunately there is no API on otto.Error to get the name & message separately.
	// Instead, look for the name as a prefix. (See the `ottoResult` function below)
	str := err.Error()
	if strings.HasPrefix(str, "HTTP:") {
		m := HttpErrRE.FindStringSubmatch(str)
		status, _ := strconv.ParseInt(m[1], 10, 0)
		message := m[2]
		if status == http.StatusUnauthorized && (runner.currentDB.User() == nil || runner.currentDB.User().Name() != "") {
			status = http.StatusForbidden
		}
		return base.HTTPErrorf(int(status), "%s (while calling %s %q)", message, runner.kind, runner.name)
	}
	return &jsError{err, runner.kind, runner.name}
}
