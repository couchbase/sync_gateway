// Copyright 2024-Present Couchbase, Inc.
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
	"fmt"
	"log"
	"testing"

	"ruleguard/internal/test_pkg/base"
)

// should have 25 valid usages and 25 invalid usages when ruleguard is run on this file/function
func testlogwrappederr() {
	ctx := context.Background()
	err := fmt.Errorf("test: %w", errors.New("an error"))
	id := 1234
	user := "alice"
	t := testing.T{}

	// ok
	PanicfCtx(ctx, "oops: %s", user)
	PanicfCtx(ctx, "oops: %v", err)
	PanicfCtx(ctx, "oops: %d %v", id, err)
	PanicfCtx(ctx, "oops: %d %v (%s)", id, err, user)
	PanicfCtx(ctx, "oops: %v (%s)", err, user)
	base.PanicfCtx(ctx, "oops: %v", err)
	base.PanicfCtx(ctx, "oops: %d %v", id, err)
	base.PanicfCtx(ctx, "oops: %d %v (%s)", id, err, user)
	base.PanicfCtx(ctx, "oops: %v (%s)", err, user)
	InfofCtx(ctx, "testing", "oops: %v", err)
	InfofCtx(ctx, "testing", "oops: %d %v", id, err)
	InfofCtx(ctx, "testing", "oops: %d %v (%s)", id, err, user)
	InfofCtx(ctx, "testing", "oops: %v (%s)", err, user)
	base.InfofCtx(ctx, "testing", "oops: %v", err)
	base.InfofCtx(ctx, "testing", "oops: %d %v", id, err)
	base.InfofCtx(ctx, "testing", "oops: %d %v (%s)", id, err, user)
	base.InfofCtx(ctx, "testing", "oops: %v (%s)", err, user)
	fmt.Printf("oops: %v", err)
	fmt.Printf("oops: %d %v", id, err)
	fmt.Printf("oops: %d %v (%s)", id, err, user)
	fmt.Printf("oops: %v (%s)", err, user)
	log.Printf("oops: %v", err)
	log.Printf("oops: %d %v", id, err)
	log.Printf("oops: %d %v (%s)", id, err, user)
	log.Printf("oops: %v (%s)", err, user)
	t.Logf("oops: %v (%s)", err, user)
	t.Fatalf("oops: %v (%s)", err, user)
	t.Errorf("oops: %v (%s)", err, user)
	t.Skipf("oops: %v (%s)", err, user)

	// misuse of %w
	PanicfCtx(ctx, "oops: %w", user) // not an error but still invalid
	PanicfCtx(ctx, "oops: %w", err)
	PanicfCtx(ctx, "oops: %d %w", id, err)
	PanicfCtx(ctx, "oops: %d %w (%s)", id, err, user)
	PanicfCtx(ctx, "oops: %w (%s)", err, user)
	base.PanicfCtx(ctx, "oops: %w", err)
	base.PanicfCtx(ctx, "oops: %d %w", id, err)
	base.PanicfCtx(ctx, "oops: %d %w (%s)", id, err, user)
	base.PanicfCtx(ctx, "oops: %w (%s)", err, user)
	InfofCtx(ctx, "testing", "oops: %w", err)
	InfofCtx(ctx, "testing", "oops: %d %w", id, err)
	InfofCtx(ctx, "testing", "oops: %d %w (%s)", id, err, user)
	InfofCtx(ctx, "testing", "oops: %w (%s)", err, user)
	base.InfofCtx(ctx, "testing", "oops: %w", err)
	base.InfofCtx(ctx, "testing", "oops: %d %w", id, err)
	base.InfofCtx(ctx, "testing", "oops: %d %w (%s)", id, err, user)
	base.InfofCtx(ctx, "testing", "oops: %w (%s)", err, user)
	fmt.Printf("oops: %w", err)
	fmt.Printf("oops: %d %w", id, err)
	fmt.Printf("oops: %d %w (%s)", id, err, user)
	fmt.Printf("oops: %w (%s)", err, user)
	log.Printf("oops: %w", err)
	log.Printf("oops: %d %w", id, err)
	log.Printf("oops: %d %w (%s)", id, err, user)
	log.Printf("oops: %w (%s)", err, user)
	t.Logf("oops: %w (%s)", err, user)
	t.Fatalf("oops: %w (%s)", err, user)
	t.Errorf("oops: %w (%s)", err, user)
	t.Skipf("oops: %w (%s)", err, user)
}

// non-imported variant
func PanicfCtx(ctx context.Context, format string, args ...interface{}) {
	base.PanicfCtx(ctx, format, args...)
}

// non-imported variant
func InfofCtx(ctx context.Context, logkey, format string, args ...interface{}) {
	base.InfofCtx(ctx, logkey, format, args...)
}
