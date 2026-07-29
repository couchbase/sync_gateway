// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

//go:build ruleguard
// +build ruleguard

//nolint:unused // ruleguard test file
package ruleguard_test

import (
	"context"
	"errors"
	"time"
)

// should have 4 valid usages and 3 invalid usages when ruleguard is run on this file/function
func testwithcancel() {
	ctx := context.Background()

	// ok
	ctx1, cancel1 := context.WithCancelCause(ctx)
	cancel1(errors.New("done"))
	_ = ctx1
	ctx2, cancel2 := context.WithTimeout(ctx, 0)
	cancel2()
	_ = ctx2
	ctx3, cancel3 := context.WithDeadlineCause(ctx, time.Now(), errors.New("expired"))
	cancel3()
	_ = ctx3
	_ = context.WithoutCancel(ctx)

	// invalid
	ctx4, cancel4 := context.WithCancel(ctx)
	cancel4()
	_ = ctx4
	ctx5, cancel5 := context.WithCancel(context.TODO())
	cancel5()
	_ = ctx5
	ctx6, cancel6 := context.WithCancel(derive(ctx))
	cancel6()
	_ = ctx6
}

func derive(ctx context.Context) context.Context { return ctx }
