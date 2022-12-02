//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package channels

import (
	"context"
	_ "embed"
	"fmt"
	"strings"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/js"
	v8 "rogchap.com/v8go" // Docs: https://pkg.go.dev/rogchap.com/v8go
)

// Maps user names (or role names prefixed with "role:") to arrays of channel or role names
type AccessMap map[string]base.Set

// If the provided principal name (in access grant format) is a role, returns the role name without prefix
func AccessNameToPrincipalName(accessPrincipalName string) (principalName string, isRole bool) {
	if strings.HasPrefix(accessPrincipalName, RoleAccessPrefix) {
		return accessPrincipalName[len(RoleAccessPrefix):], true
	}
	return accessPrincipalName, false
}

// Prefix used to identify roles in access grants
const RoleAccessPrefix = "role:"

/** Result of running a channel-mapper function. */
type ChannelMapperOutput struct {
	Channels  base.Set  // channels assigned to the document via channel() callback
	Roles     AccessMap // roles granted to users via role() callback
	Access    AccessMap // channels granted to users via access() callback
	Rejection error     // Error associated with failed validate (require callbacks, etc)
	Expiry    *uint32   // Expiry value specified by expiry() callback.  Standard CBS expiry format: seconds if less than 30 days, epoch time otherwise
}

// The object that runs the sync function.
type ChannelMapper struct {
	service  *js.Service
	timeout  time.Duration
	fnSource string
}

const DefaultSyncFunction = `function(doc){channel(doc.channels);}`

// Creates a ChannelMapper.
func NewChannelMapper(owner js.ServiceHost, fnSource string, timeout time.Duration) *ChannelMapper {
	service := js.NewCustomService(owner, kChannelMapperServiceName, func(base *js.BasicTemplate) (js.Template, error) {
		return createChannelService(base, fnSource, timeout)
	})
	return &ChannelMapper{
		service:  service,
		timeout:  timeout,
		fnSource: fnSource,
	}
}

// used by tests.
func newChannelMapperWithVMs(fnSource string, timeout time.Duration) *ChannelMapper {
	return NewChannelMapper(js.NewVM(), fnSource, timeout)
}

func (cm *ChannelMapper) closeVMs() {
	cm.service.Host().Close()
}

// Creates a ChannelMapper with the default sync function. (Used by tests)
func NewDefaultChannelMapper(vms *js.VMPool) *ChannelMapper {
	return NewChannelMapper(vms, DefaultSyncFunction, time.Duration(base.DefaultJavascriptTimeoutSecs)*time.Second)
}

func (mapper *ChannelMapper) Function() string {
	return mapper.fnSource
}

// This function is DEPRECATED. It's currently used in some tests that can't easily be changed.
// Its current implementation is a kludge, and it shouldn't be used in production.
func (mapper *ChannelMapper) SetFunction(fnSource string) error {
	mapper.fnSource = fnSource
	mapper.service = js.NewCustomService(mapper.service.Host(), kChannelMapperServiceName, func(base *js.BasicTemplate) (js.Template, error) {
		return createChannelService(base, fnSource, mapper.timeout)
	})
	return nil
}

// Runs the sync function. Thread-safe.
func (mapper *ChannelMapper) MapToChannelsAndAccess(body map[string]interface{}, oldBodyJSON string, metaMap map[string]interface{}, userCtx map[string]interface{}) (*ChannelMapperOutput, error) {
	result1, err := mapper.withSyncRunner(func(runner *syncRunner) (any, error) {
		return runner.call(body, oldBodyJSON, metaMap, userCtx)
	})
	if err != nil {
		return nil, err
	}
	output := result1.(*ChannelMapperOutput)
	return output, nil
}

// Creates a js.Service instance for a ChannelMapper;
// this configures the object & callback templates in a V8 VM.
func createChannelService(base *js.BasicTemplate, fnSource string, timeout time.Duration) (js.Template, error) {
	err := base.SetScript(wrappedFuncSource(fnSource))
	if err != nil {
		return nil, err
	}
	// Define the callback functions:
	base.GlobalCallback("channel", func(jsr *js.Runner, this *v8.Object, args []*v8.Value) (any, error) {
		return jsr.Client.(*syncRunner).channelCallback(args)
	})
	base.GlobalCallback("access", func(jsr *js.Runner, this *v8.Object, args []*v8.Value) (any, error) {
		return jsr.Client.(*syncRunner).accessCallback(args)
	})
	base.GlobalCallback("role", func(jsr *js.Runner, this *v8.Object, args []*v8.Value) (any, error) {
		return jsr.Client.(*syncRunner).roleCallback(args)
	})
	base.GlobalCallback("reject", func(jsr *js.Runner, this *v8.Object, args []*v8.Value) (any, error) {
		return jsr.Client.(*syncRunner).rejectCallback(args)
	})
	base.GlobalCallback("expiry", func(jsr *js.Runner, this *v8.Object, args []*v8.Value) (any, error) {
		jsr.Client.(*syncRunner).expiryCallback(args)
		return nil, nil
	})
	return base, err
}

func (mapper *ChannelMapper) withSyncRunner(fn func(*syncRunner) (any, error)) (any, error) {
	return mapper.service.WithRunner(func(jsRunner *js.Runner) (any, error) {
		goContext := context.Background()
		if mapper.timeout > 0 {
			var cancelFn context.CancelFunc
			goContext, cancelFn = context.WithTimeout(goContext, mapper.timeout)
			defer cancelFn()
		}
		jsRunner.SetContext(goContext)

		var runner *syncRunner
		if jsRunner.Client == nil {
			runner = &syncRunner{
				Runner: jsRunner,
			}
			jsRunner.Client = runner
		} else {
			runner = jsRunner.Client.(*syncRunner)
		}
		return fn(runner)
	})
}

func wrappedFuncSource(funcSource string) string {
	return fmt.Sprintf(
		funcWrapper,
		funcSource,
		base.SyncFnErrorAdminRequired,
		base.SyncFnErrorWrongUser,
		base.SyncFnErrorMissingRole,
		base.SyncFnErrorMissingChannelAccess,
	)
}

const kChannelMapperServiceName = "channelMapper"

// The JavaScript code run by the SyncRunner; the sync fn is copied into it.
// See wrappedFuncSource().
//
//go:embed sync_fn_wrapper.js
var funcWrapper string
