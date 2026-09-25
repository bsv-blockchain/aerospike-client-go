// Copyright 2014-2022 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

import (
	"errors"
	"flag"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
)

// testServerHostPort reads the -h/-p flags registered by the external
// aerospike_test package's TestMain (flags are process-global, so an
// internal package test can still read their already-parsed values),
// falling back to this repo's usual local test server.
func testServerHostPort() (string, int) {
	host := "127.0.0.1"
	if f := flag.Lookup("h"); f != nil && f.Value.String() != "" {
		host = f.Value.String()
	}
	port := 3000
	if f := flag.Lookup("p"); f != nil {
		if p, err := strconv.Atoi(f.Value.String()); err == nil && p != 0 {
			port = p
		}
	}
	return host, port
}

// countingRawHandler2 is a minimal, package-internal RawRecordHandler that
// counts each accepted record by digest, for exactly-once assertions.
type countingRawHandler2 struct {
	mu   *sync.Mutex
	seen map[string]int

	curDigest string
}

func (h *countingRawHandler2) BeginRecord(digest []byte, generation, expiration uint32) error {
	h.curDigest = string(digest)
	return nil
}

func (h *countingRawHandler2) Bin(name []byte, particleType int, value []byte) error { return nil }

func (h *countingRawHandler2) EndRecord() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen[h.curDigest]++
	return nil
}

func (h *countingRawHandler2) DiscardRecord() {}

// setRawExecOverride arms rawExecOverride for the duration of the calling
// test only, guaranteeing it is cleared afterward (via t.Cleanup) even if
// the test fails or the override is never consumed, so it can never leak
// into an unrelated test sharing the same test binary.
func setRawExecOverride(t *testing.T, f func(cmd *queryPartitionRawCommand) Error) {
	t.Helper()
	rawExecOverride = f
	t.Cleanup(func() { rawExecOverride = nil })
}

// TestQueryPartitionRawCommandExecuteRetriesNodeErrors is a regression test
// for Execute() failing to call tracker.shouldRetry (see the comment on
// (*queryPartitionRawCommand).Execute), and for QueryPartitionsRaw returning
// a non-nil error for a query that, after retrying, actually read every
// partition (see the comment on the `if done` branch in queryPartitionsRaw:
// this must return nil, matching the Java client).
//
// It forces the very first node command of a QueryPartitionsRaw call to fail
// with a retryable (NETWORK_ERROR) error -- via rawExecOverride, so no real
// network fault or timing is involved -- and checks that the query still
// delivers every record exactly once and returns nil, because the failed
// round's partitions get correctly retried in a second, real round against
// the live test server.
//
// Without the Execute() fix, the forced error's partitions are never marked
// unavailable, isComplete reports the query done after the very first
// (failed) round, and this test's record count comes up short. Without the
// queryPartitionsRaw fix, rawErr is non-nil despite every record having been
// delivered.
func TestQueryPartitionRawCommandExecuteRetriesNodeErrors(t *testing.T) {
	host, port := testServerHostPort()
	clnt, err := NewClient(host, port)
	if err != nil {
		t.Skipf("no local Aerospike server at %s:%d to run this integration-level test against: %v", host, port, err)
	}
	defer clnt.Close()

	ns := "test"
	set := fmt.Sprintf("qprretry%d", 1)
	if derr := clnt.Truncate(nil, ns, set, nil); derr != nil {
		t.Fatalf("Truncate: %v", derr)
	}

	const n = 300
	wpolicy := NewWritePolicy(0, 0)
	for i := 0; i < n; i++ {
		key, kerr := NewKey(ns, set, fmt.Sprintf("k-%d", i))
		if kerr != nil {
			t.Fatalf("NewKey: %v", kerr)
		}
		if perr := clnt.PutBins(wpolicy, key, NewBin("v", i)); perr != nil {
			t.Fatalf("PutBins: %v", perr)
		}
	}

	stm := NewStatement(ns, set)
	filter := NewPartitionFilterAll()

	mu := &sync.Mutex{}
	seen := make(map[string]int)
	newHandler := func() RawRecordHandler {
		return &countingRawHandler2{mu: mu, seen: seen}
	}

	// Force exactly one retryable failure, on the very first
	// (*queryPartitionRawCommand).Execute call -- i.e. the single-node
	// command of this query's first round -- then let every subsequent call
	// run for real.
	var forced bool
	var forceMu sync.Mutex
	setRawExecOverride(t, func(cmd *queryPartitionRawCommand) Error {
		forceMu.Lock()
		defer forceMu.Unlock()
		if forced {
			return cmd.execute(cmd)
		}
		forced = true
		return newError(types.NETWORK_ERROR)
	})

	policy := NewQueryPolicy()
	rawErr := clnt.QueryPartitionsRaw(policy, stm, filter, newHandler)

	forceMu.Lock()
	wasForced := forced
	forceMu.Unlock()
	if !wasForced {
		t.Fatal("forced execute error was never triggered: Execute() was not called, or no node command was constructed")
	}

	if rawErr != nil {
		t.Errorf("QueryPartitionsRaw returned %v, want nil: every partition was read (after a retry), so per Java-client semantics this must report success", rawErr)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != n {
		t.Errorf("delivered %d distinct records, want %d (a retryable node error must not lose records)", len(seen), n)
	}
	for d, c := range seen {
		if c != 1 {
			t.Errorf("record %x delivered %d times, want 1", d, c)
		}
	}
}

// TestQueryPartitionRawCommandExecuteNonRetryableErrorStillFails is the
// counterpart of TestQueryPartitionRawCommandExecuteRetriesNodeErrors: a
// node-level error that partitionTracker.shouldRetry does NOT consider
// retryable (here, PARAMETER_ERROR) must still make QueryPartitionsRaw
// return a non-nil error, even though -- per the fix above -- a *retried and
// recovered* query now returns nil. The two must not be conflated.
//
// It also guards P2-2: a non-retryable command error leaves
// nodePartitions.partsUnavailable at 0 (shouldRetry's side effect never ran
// for it), so isClusterComplete's partsUnavailable==0 branch still reports
// done=true despite the query having failed. filter.Done must be forced back
// to false in that case too, the same as the abort path already does, so
// "an error was returned" reliably implies "not Done".
func TestQueryPartitionRawCommandExecuteNonRetryableErrorStillFails(t *testing.T) {
	host, port := testServerHostPort()
	clnt, err := NewClient(host, port)
	if err != nil {
		t.Skipf("no local Aerospike server at %s:%d to run this integration-level test against: %v", host, port, err)
	}
	defer clnt.Close()

	ns := "test"
	set := "qprretry_nonretryable"
	if derr := clnt.Truncate(nil, ns, set, nil); derr != nil {
		t.Fatalf("Truncate: %v", derr)
	}
	key, kerr := NewKey(ns, set, "k-0")
	if kerr != nil {
		t.Fatalf("NewKey: %v", kerr)
	}
	if perr := clnt.PutBins(NewWritePolicy(0, 0), key, NewBin("v", 0)); perr != nil {
		t.Fatalf("PutBins: %v", perr)
	}

	stm := NewStatement(ns, set)
	filter := NewPartitionFilterAll()
	newHandler := func() RawRecordHandler { return &countingRawHandler2{mu: &sync.Mutex{}, seen: map[string]int{}} }

	setRawExecOverride(t, func(cmd *queryPartitionRawCommand) Error {
		return newError(types.PARAMETER_ERROR)
	})

	rawErr := clnt.QueryPartitionsRaw(NewQueryPolicy(), stm, filter, newHandler)
	if rawErr == nil {
		t.Fatal("expected a non-nil error: PARAMETER_ERROR is not retryable, so the query must not silently report success")
	}
	if !rawErr.Matches(types.PARAMETER_ERROR) {
		t.Errorf("rawErr = %v, want it to Match PARAMETER_ERROR", rawErr)
	}
	if filter.Done {
		t.Error("filter.Done = true, want false: a non-retryable node error means this partition range was never actually read")
	}
}

// TestQueryPartitionsRawExhaustedRetriesReturnsErrorWithCause is a guard
// test for the "must still error" half of the retry contract (see
// TestQueryPartitionRawCommandExecuteRetriesNodeErrors for the "retry then
// success returns nil" half), for both MaxRetries == 0 and > 0: a query
// whose every attempt fails with a retryable error must give up with
// MAX_RETRIES_EXCEEDED, must not silently report success, must leave
// partitionFilter.Done false (nothing was actually read), and -- per the
// P2-1 fix -- that error must still let the caller reach the swallowed
// retryable errors' own cause via errors.Is/As, not a bare
// "max retries exceeded" with no node or cause attached.
func TestQueryPartitionsRawExhaustedRetriesReturnsErrorWithCause(t *testing.T) {
	for _, maxRetries := range []int{0, 3} {
		t.Run(fmt.Sprintf("MaxRetries=%d", maxRetries), func(t *testing.T) {
			host, port := testServerHostPort()
			clnt, err := NewClient(host, port)
			if err != nil {
				t.Skipf("no local Aerospike server at %s:%d to run this integration-level test against: %v", host, port, err)
			}
			defer clnt.Close()

			ns := "test"
			set := fmt.Sprintf("qprexhausted%d", maxRetries)
			stm := NewStatement(ns, set)
			filter := NewPartitionFilterAll()

			sentinel := errors.New("always fails")
			setRawExecOverride(t, func(cmd *queryPartitionRawCommand) Error {
				return newErrorAndWrap(sentinel, types.NETWORK_ERROR)
			})

			policy := NewQueryPolicy()
			policy.MaxRetries = maxRetries
			policy.SleepBetweenRetries = time.Millisecond // keep the loop fast
			newHandler := func() RawRecordHandler {
				return &countingRawHandler2{mu: &sync.Mutex{}, seen: map[string]int{}}
			}

			rawErr := clnt.QueryPartitionsRaw(policy, stm, filter, newHandler)

			if rawErr == nil {
				t.Fatal("expected a non-nil error when retries are exhausted, got nil")
			}
			if !rawErr.Matches(types.MAX_RETRIES_EXCEEDED) {
				t.Errorf("rawErr = %v, want it to Match MAX_RETRIES_EXCEEDED", rawErr)
			}
			if !errors.Is(rawErr, sentinel) {
				t.Errorf("rawErr = %v, want errors.Is(rawErr, sentinel) true: the swallowed retryable errors' cause must survive to the give-up error", rawErr)
			}
			if filter.Done {
				t.Error("filter.Done = true, want false: retries were exhausted, so this partition range was never actually read")
			}
		})
	}
}

// TestQueryPartitionsRawTotalTimeoutReturnsTimeout is the TotalTimeout
// counterpart of TestQueryPartitionsRawExhaustedRetriesReturnsErrorWithCause:
// a query whose every attempt fails with a retryable error, and that never
// hits MaxRetries because TotalTimeout expires first, must return a TIMEOUT
// error, not nil.
func TestQueryPartitionsRawTotalTimeoutReturnsTimeout(t *testing.T) {
	host, port := testServerHostPort()
	clnt, err := NewClient(host, port)
	if err != nil {
		t.Skipf("no local Aerospike server at %s:%d to run this integration-level test against: %v", host, port, err)
	}
	defer clnt.Close()

	ns := "test"
	set := "qprtimeout"
	stm := NewStatement(ns, set)

	setRawExecOverride(t, func(cmd *queryPartitionRawCommand) Error {
		return newError(types.NETWORK_ERROR)
	})

	policy := NewQueryPolicy()
	policy.MaxRetries = 100000 // effectively unbounded: TotalTimeout must fire first
	policy.TotalTimeout = 50 * time.Millisecond
	policy.SleepBetweenRetries = 5 * time.Millisecond
	newHandler := func() RawRecordHandler {
		return &countingRawHandler2{mu: &sync.Mutex{}, seen: map[string]int{}}
	}

	start := time.Now()
	rawErr := clnt.QueryPartitionsRaw(policy, stm, nil, newHandler)
	elapsed := time.Since(start)

	if rawErr == nil {
		t.Fatal("expected a TIMEOUT error, got nil")
	}
	if !rawErr.Matches(types.TIMEOUT) {
		t.Errorf("rawErr = %v, want it to Match TIMEOUT", rawErr)
	}
	if elapsed > 5*time.Second {
		t.Errorf("took %v to give up, want well under 5s given a 50ms TotalTimeout", elapsed)
	}
}
