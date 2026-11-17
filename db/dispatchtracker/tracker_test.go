/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package dispatchtracker

import (
	"fmt"
	"math/bits"
	"math/rand/v2"
	"runtime"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// releasedThroughFor gives each ticket a distinct releasedThrough, so a returned watermark identifies the ticket it came from.
func releasedThroughFor(ticket uint64) uint64 {
	return ticket * 10
}

// buffers hands out one distinct changed buffer per ticket and maps a buffer back to its ticket.
type buffers struct {
	byTicket map[uint64]*[]channels.ID
	ticketOf map[*[]channels.ID]uint64
}

func newBuffers(n uint64) *buffers {
	b := &buffers{byTicket: make(map[uint64]*[]channels.ID, n), ticketOf: make(map[*[]channels.ID]uint64, n)}
	for ticket := uint64(1); ticket <= n; ticket++ {
		buf := new([]channels.ID)
		b.byTicket[ticket] = buf
		b.ticketOf[buf] = ticket
	}
	return b
}

// tickets converts the passed buffers back into the tickets they belong to.
func (b *buffers) tickets(t *testing.T, passed []*[]channels.ID) []uint64 {
	out := make([]uint64, 0, len(passed))
	for _, buf := range passed {
		ticket, ok := b.ticketOf[buf]
		// assert, not require: this runs on completer goroutines in the concurrent test.
		assert.True(t, ok, "passed holds a buffer that no ticket owns")
		out = append(out, ticket)
	}
	return out
}

func newTestTracker(t *testing.T, capacity uint64) *Tracker {
	tr, err := NewForTest(capacity)
	require.NoError(t, err)
	return tr
}

func TestNewCapacity(t *testing.T) {
	for _, capacity := range []uint64{MinCapacity, 2048, MaxCapacity} {
		tr, err := NewTracker(capacity)
		require.NoError(t, err, "capacity %d", capacity)
		assert.Equal(t, capacity, tr.Capacity())
	}
	for _, capacity := range []uint64{0, 1, 512, 1000, 1025, MaxCapacity + 1, MaxCapacity * 2} {
		_, err := NewTracker(capacity)
		require.Error(t, err, "capacity %d", capacity)
	}

	for _, capacity := range []uint64{1, 2, 4, 8, MinCapacity} {
		tr, err := NewForTest(capacity)
		require.NoError(t, err, "capacity %d", capacity)
		assert.Equal(t, capacity, tr.Capacity())
		assert.Equal(t, uint64(1), tr.Head(), "tickets start at 1")
		// The slot slice is exactly capacity long, with no spare backing array to grow into.
		assert.Len(t, tr.slots, int(capacity))
		assert.Equal(t, int(capacity), cap(tr.slots))
	}
	for _, capacity := range []uint64{0, 3, 6, 1000} {
		_, err := NewForTest(capacity)
		require.Error(t, err, "capacity %d", capacity)
	}
}

func TestIsPowerOfTwo(t *testing.T) {
	for n := uint64(0); n <= 1<<12; n++ {
		want := bits.OnesCount64(n) == 1
		assert.Equal(t, want, isPowerOfTwo(n), "n=%d", n)
	}
	assert.True(t, isPowerOfTwo(1<<63))
	assert.False(t, isPowerOfTwo(1<<63+1))
}

func TestHasRoom(t *testing.T) {
	ctx := base.TestCtx(t)
	tr := newTestTracker(t, 4)
	bufs := newBuffers(5)

	for next := uint64(1); next <= 4; next++ {
		assert.True(t, tr.HasRoom(next), "next=%d", next)
		assert.Equal(t, next-1, tr.InFlight(next))
	}
	assert.False(t, tr.HasRoom(5), "tickets 1-4 outstanding fill a capacity-4 tracker")

	// Completing a ticket other than head frees nothing.
	_, _ = tr.Complete(ctx, 2, releasedThroughFor(2), bufs.byTicket[2], nil)
	assert.False(t, tr.HasRoom(5))

	// Retiring head frees its slot, and ticket 2 was already done so it retires too.
	_, _ = tr.Complete(ctx, 1, releasedThroughFor(1), bufs.byTicket[1], nil)
	assert.True(t, tr.HasRoom(5))
	assert.True(t, tr.HasRoom(6))
	assert.False(t, tr.HasRoom(7))
}

func TestCompleteInOrder(t *testing.T) {
	ctx := base.TestCtx(t)
	const capacity, n = 4, 20 // several wraps
	tr := newTestTracker(t, capacity)
	bufs := newBuffers(n)

	for ticket := uint64(1); ticket <= n; ticket++ {
		require.True(t, tr.HasRoom(ticket))
		passed, watermark := tr.Complete(ctx, ticket, releasedThroughFor(ticket), bufs.byTicket[ticket], nil)
		assert.Equal(t, []uint64{ticket}, bufs.tickets(t, passed))
		assert.Equal(t, releasedThroughFor(ticket), watermark)
		assert.Equal(t, ticket+1, tr.Head())
		assert.Nil(t, tr.slots[ticket&tr.mask].changed, "a retired slot drops its buffer")
	}
}

func TestCompleteOutOfOrder(t *testing.T) {
	ctx := base.TestCtx(t)
	tr := newTestTracker(t, 8)
	bufs := newBuffers(8)

	// Later tickets finishing first retire nothing: head (1) is still in flight.
	for _, ticket := range []uint64{3, 2, 5} {
		passed, watermark := tr.Complete(ctx, ticket, releasedThroughFor(ticket), bufs.byTicket[ticket], nil)
		assert.Empty(t, passed, "ticket %d", ticket)
		assert.Equal(t, uint64(0), watermark, "ticket %d", ticket)
		assert.Equal(t, uint64(1), tr.Head())
	}

	// Head finishing retires the consecutive run behind it, in ticket order, and stops at the gap (4).
	passed, watermark := tr.Complete(ctx, 1, releasedThroughFor(1), bufs.byTicket[1], nil)
	assert.Equal(t, []uint64{1, 2, 3}, bufs.tickets(t, passed))
	assert.Equal(t, releasedThroughFor(3), watermark)
	assert.Equal(t, uint64(4), tr.Head())

	// Filling the gap resumes the walk through the ticket that was already done.
	passed, watermark = tr.Complete(ctx, 4, releasedThroughFor(4), bufs.byTicket[4], nil)
	assert.Equal(t, []uint64{4, 5}, bufs.tickets(t, passed))
	assert.Equal(t, releasedThroughFor(5), watermark)
	assert.Equal(t, uint64(6), tr.Head())
}

func TestCompleteAppendsToScratch(t *testing.T) {
	ctx := base.TestCtx(t)
	tr := newTestTracker(t, 4)
	bufs := newBuffers(3)

	_, _ = tr.Complete(ctx, 2, releasedThroughFor(2), bufs.byTicket[2], nil)
	scratch := make([]*[]channels.ID, 0, 4)
	passed, _ := tr.Complete(ctx, 1, releasedThroughFor(1), bufs.byTicket[1], scratch)
	assert.Equal(t, []uint64{1, 2}, bufs.tickets(t, passed))
	assert.Same(t, &scratch[:1][0], &passed[0], "Complete appends into the caller's scratch slice instead of allocating")

	// Reusing the scratch slice for the next call, the way a consumer would.
	passed, _ = tr.Complete(ctx, 3, releasedThroughFor(3), bufs.byTicket[3], passed[:0])
	assert.Equal(t, []uint64{3}, bufs.tickets(t, passed))
}

// TestZeroedSlotNotDone checks why tickets start at 1: before the slice first wraps, every slot holds ticket
// 0, and that must never read as a finished head.
func TestZeroedSlotNotDone(t *testing.T) {
	ctx := base.TestCtx(t)
	for _, capacity := range []uint64{1, 2, 4} {
		t.Run(fmt.Sprintf("capacity=%d", capacity), func(t *testing.T) {
			tr := newTestTracker(t, capacity)
			bufs := newBuffers(capacity)
			if capacity > 1 {
				passed, _ := tr.Complete(ctx, capacity, releasedThroughFor(capacity), bufs.byTicket[capacity], nil)
				assert.Empty(t, passed, "untouched slots for tickets 1..%d must not count as done", capacity-1)
				assert.Equal(t, uint64(1), tr.Head())
			}
			passed, _ := tr.Complete(ctx, 1, releasedThroughFor(1), bufs.byTicket[1], nil)
			assert.NotEmpty(t, passed)
		})
	}
}

// TestCompleteRandomOrder issues tickets whenever there is room and completes a random outstanding one,
// over many wraps, checking each call against a model of which tickets are done.
func TestCompleteRandomOrder(t *testing.T) {
	ctx := base.TestCtx(t)
	for _, capacity := range []uint64{1, 2, 4, 8, 16} {
		t.Run(fmt.Sprintf("capacity=%d", capacity), func(t *testing.T) {
			const n = 2000
			rng := rand.New(rand.NewPCG(capacity, 0))
			tr := newTestTracker(t, capacity)
			bufs := newBuffers(n)

			next := uint64(1)
			head := uint64(1)
			var outstanding []uint64
			done := make(map[uint64]bool)
			for head <= n {
				for next <= n && tr.HasRoom(next) {
					outstanding = append(outstanding, next)
					next++
				}
				require.LessOrEqual(t, tr.InFlight(next), capacity)

				i := rng.IntN(len(outstanding))
				ticket := outstanding[i]
				outstanding = append(outstanding[:i], outstanding[i+1:]...)
				done[ticket] = true

				var want []uint64
				for ; done[head]; head++ {
					want = append(want, head)
				}

				passed, watermark := tr.Complete(ctx, ticket, releasedThroughFor(ticket), bufs.byTicket[ticket], nil)
				require.Equal(t, want, nilIfEmpty(bufs.tickets(t, passed)), "completing ticket %d", ticket)
				if len(want) > 0 {
					require.Equal(t, releasedThroughFor(want[len(want)-1]), watermark)
				} else {
					require.Equal(t, uint64(0), watermark)
				}
				require.Equal(t, head, tr.Head())
			}
			assert.Equal(t, uint64(n+1), tr.Head())
		})
	}
}

func nilIfEmpty(s []uint64) []uint64 {
	if len(s) == 0 {
		return nil
	}
	return s
}

// TestCompleteConcurrent runs one issuer and several completers, as the change cache will: every ticket is
// retired exactly once, each call retires a consecutive range, and its watermark is that range's last.
// Run with -race.
func TestCompleteConcurrent(t *testing.T) {
	ctx := base.TestCtx(t)
	const n, completers = 20000, 8
	for _, capacity := range []uint64{1, 8, 64} {
		t.Run(fmt.Sprintf("capacity=%d", capacity), func(t *testing.T) {
			tr := newTestTracker(t, capacity)
			bufs := newBuffers(n)

			// If head ever stops advancing the issuer would wait for room forever, so it gives up at a deadline
			// and the missing tickets fail the test instead of hanging it.
			deadline := time.Now().Add(30 * time.Second)
			issued := make(chan uint64, capacity)
			go func() {
				defer close(issued)
				for next := uint64(1); next <= n; next++ {
					for !tr.HasRoom(next) && time.Now().Before(deadline) {
						runtime.Gosched()
					}
					if !tr.HasRoom(next) {
						assert.Failf(t, "no room to issue ticket", "ticket %d: head stuck at %d", next, tr.Head())
						return
					}
					issued <- next
				}
			}()

			var mu sync.Mutex
			var retired []uint64
			var maxWatermark uint64
			var wg sync.WaitGroup
			for range completers {
				wg.Go(func() {
					var scratch []*[]channels.ID
					for ticket := range issued {
						var watermark uint64
						scratch, watermark = tr.Complete(ctx, ticket, releasedThroughFor(ticket), bufs.byTicket[ticket], scratch[:0])
						if len(scratch) == 0 {
							continue
						}
						got := bufs.tickets(t, scratch)
						for i := 1; i < len(got); i++ {
							assert.Equal(t, got[i-1]+1, got[i], "one call retires a consecutive range")
						}
						assert.Equal(t, releasedThroughFor(got[len(got)-1]), watermark)
						mu.Lock()
						retired = append(retired, got...)
						maxWatermark = max(maxWatermark, watermark)
						mu.Unlock()
					}
				})
			}
			wg.Wait()

			require.Len(t, retired, n)
			seen := make(map[uint64]bool, n)
			for _, ticket := range retired {
				assert.False(t, seen[ticket], "ticket %d retired twice", ticket)
				seen[ticket] = true
			}
			assert.Equal(t, uint64(n+1), tr.Head())
			assert.Equal(t, releasedThroughFor(n), maxWatermark)
		})
	}
}

// TestCompleteOutsideIssuingRule checks that a completion breaking the issuing rule raises an assertion and
// leaves the tracker as it was, so tickets issued correctly still retire normally.
func TestCompleteOutsideIssuingRule(t *testing.T) {
	testCases := []struct {
		name   string
		ticket func(head, capacity uint64) uint64
	}{
		{name: "ticket zero", ticket: func(_, _ uint64) uint64 { return 0 }},
		{name: "already retired", ticket: func(head, _ uint64) uint64 { return head - 1 }},
		{name: "issued without room", ticket: func(head, capacity uint64) uint64 { return head + capacity }},
		{name: "completed twice", ticket: func(head, _ uint64) uint64 { return head + 1 }},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if base.IsDevMode() {
				if len(base.AssertionFailures()) > 0 {
					t.Skip("Skipping test - other assertion failures are present and would be lost")
				}
				defer base.ClearAssertionFailures()
			}
			ctx := base.TestCtx(t)
			const capacity = 4
			tr := newTestTracker(t, capacity)
			bufs := newBuffers(20)

			// Move head to 3 so the slice holds leftovers, and complete ticket 4 so it is done but unretired.
			for ticket := uint64(1); ticket <= 2; ticket++ {
				_, _ = tr.Complete(ctx, ticket, releasedThroughFor(ticket), bufs.byTicket[ticket], nil)
			}
			_, _ = tr.Complete(ctx, 4, releasedThroughFor(4), bufs.byTicket[4], nil)
			head := tr.Head()
			require.Equal(t, uint64(3), head)
			before := slices.Clone(tr.slots)

			assertionFailCount := base.SyncGatewayStats.GlobalStats.ResourceUtilization.AssertionFailCount
			startCount := assertionFailCount.Value()
			bad := tc.ticket(head, capacity)
			stray := new([]channels.ID)
			if base.IsDevMode() {
				require.Panics(t, func() {
					_, _ = tr.Complete(ctx, bad, 999, stray, nil)
				})
			} else {
				passed, watermark := tr.Complete(ctx, bad, 999, stray, nil)
				assert.Empty(t, passed)
				assert.Equal(t, uint64(0), watermark)
			}
			assert.Equal(t, startCount+1, assertionFailCount.Value())
			assert.Equal(t, head, tr.Head())
			assert.Equal(t, before, tr.slots, "a rejected completion must not write any slot")

			// The tracker still works: head (3) completes and retires 3 and 4.
			passed, watermark := tr.Complete(ctx, 3, releasedThroughFor(3), bufs.byTicket[3], nil)
			assert.Equal(t, []uint64{3, 4}, bufs.tickets(t, passed))
			assert.Equal(t, releasedThroughFor(4), watermark)
		})
	}
}

func TestConstants(t *testing.T) {
	assert.True(t, isPowerOfTwo(MinCapacity))
	assert.True(t, isPowerOfTwo(MaxCapacity))
	assert.LessOrEqual(t, MinCapacity, MaxCapacity)
	assert.GreaterOrEqual(t, RunSize, 1)
}

func TestValidateCapacity(t *testing.T) {
	testCases := []struct {
		capacity      uint64
		maxNumPending int
		wantErr       string
	}{
		{capacity: MinCapacity, maxNumPending: 10000},
		{capacity: MinCapacity, maxNumPending: 1023 * RunSize}, // a cascade of 1,023 runs leaves one run spare
		{capacity: MinCapacity, maxNumPending: 1023*RunSize + 1, wantErr: "full cascade"},
		{capacity: 2048, maxNumPending: 1023*RunSize + 1},
		{capacity: MaxCapacity, maxNumPending: (MaxCapacity - 1) * RunSize},
		{capacity: MaxCapacity, maxNumPending: (MaxCapacity-1)*RunSize + 1, wantErr: "full cascade"},
		{capacity: 1000, maxNumPending: 10, wantErr: "power of two"},
		{capacity: 0, maxNumPending: 10, wantErr: "power of two"},
		{capacity: 512, maxNumPending: 10, wantErr: "between"},
		{capacity: MaxCapacity * 2, maxNumPending: 10, wantErr: "between"},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("capacity=%d,maxNumPending=%d", tc.capacity, tc.maxNumPending), func(t *testing.T) {
			err := ValidateCapacity(tc.capacity, tc.maxNumPending)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.wantErr)
			}
		})
	}
}

func TestCapacityFor(t *testing.T) {
	testCases := []struct {
		maxNumPending int
		want          uint64
		wantErr       bool
	}{
		{maxNumPending: 0, want: MinCapacity},
		{maxNumPending: 1, want: MinCapacity},
		{maxNumPending: 10000, want: MinCapacity},
		{maxNumPending: 1023 * RunSize, want: MinCapacity},
		{maxNumPending: 1023*RunSize + 1, want: 2048},
		{maxNumPending: 2047 * RunSize, want: 2048},
		{maxNumPending: 2047*RunSize + 1, want: 4096},
		{maxNumPending: (MaxCapacity - 1) * RunSize, want: MaxCapacity},
		{maxNumPending: (MaxCapacity-1)*RunSize + 1, wantErr: true},
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("maxNumPending=%d", tc.maxNumPending), func(t *testing.T) {
			got, err := CapacityFor(tc.maxNumPending)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestCapacityForIsSmallestValid checks that the auto-sized capacity always passes ValidateCapacity, and
// that halving it would not.
func TestCapacityForIsSmallestValid(t *testing.T) {
	for maxNumPending := 0; maxNumPending <= 200000; maxNumPending += 97 {
		capacity, err := CapacityFor(maxNumPending)
		require.NoError(t, err)
		require.NoError(t, ValidateCapacity(capacity, maxNumPending), "maxNumPending=%d", maxNumPending)
		if capacity > MinCapacity {
			require.Error(t, ValidateCapacity(capacity/2, maxNumPending), "maxNumPending=%d: %d would also do", maxNumPending, capacity/2)
		}
	}
}
