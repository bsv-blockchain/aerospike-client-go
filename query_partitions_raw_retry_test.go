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
	"flag"
	"fmt"
	"strconv"
	"sync"
	"testing"

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

// TestQueryPartitionRawCommandExecuteRetriesNodeErrors is a regression test
// for Execute() failing to call tracker.shouldRetry (see the comment on
// (*queryPartitionRawCommand).Execute). It forces the very first node
// command of a QueryPartitionsRaw call to fail with a retryable
// (NETWORK_ERROR) error -- via the one-shot armTestForceRawExecErr hook, so
// no real network fault or timing is involved -- and checks that the query
// still delivers every record exactly once, because the failed round's
// partitions get correctly retried in a second, real round against the live
// test server.
//
// Without the Execute() fix, the forced error's partitions are never marked
// unavailable, isComplete reports the query done after the very first
// (failed) round, and this test's record count comes up short.
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

	// Arm exactly one forced, retryable failure for the very next
	// (*queryPartitionRawCommand).Execute call -- i.e. the single-node
	// command of this query's first round.
	armTestForceRawExecErr(newError(types.NETWORK_ERROR))

	policy := NewQueryPolicy()
	rawErr := clnt.QueryPartitionsRaw(policy, stm, filter, newHandler)

	// The hook must actually have been consumed by this call; otherwise the
	// test isn't exercising anything (e.g. no node was available at all).
	if _, stillArmed := takeTestForceRawExecErr(); stillArmed {
		t.Fatal("forced execute error was never consumed: Execute() was not called, or no node command was constructed")
	}

	// Per the executor's existing (pre-existing, shared with QueryPartitions)
	// behavior, a query that needed a retry can return a non-nil error for
	// its first, failed round even though a later round completed the query
	// successfully -- so we deliberately do not assert rawErr == nil here.
	// What must hold is that every record was still delivered, exactly once.
	_ = rawErr

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
