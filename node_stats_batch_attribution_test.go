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
	"encoding/binary"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
)

// batchRecordHeader builds a single 22-byte batch-response record header
// (_MSG_REMAINING_HEADER_SIZE) with no fields and no ops, as parsed by the
// batch command parseRecordResults loops.
func batchRecordHeader(batchIndex uint32, resultCode byte, last bool) []byte {
	h := make([]byte, int(_MSG_REMAINING_HEADER_SIZE))
	if last {
		h[3] = byte(_INFO3_LAST)
	}
	h[5] = resultCode
	binary.BigEndian.PutUint32(h[14:18], batchIndex) // batchIndex
	// fieldCount [18:20] = 0, opCount [20:22] = 0
	return h
}

// totalForNamespace sums every result-code count recorded for a namespace
// across all command types.
func totalForNamespace(counts map[string]map[string]map[string]int, ns string) int {
	total := 0
	for _, rcMap := range counts[ns] {
		for _, c := range rcMap {
			total += c
		}
	}
	return total
}

// TestBatchGetStatsAttributedToOwnNamespace drives the real
// batchCommandGet.parseRecordResults loop over a batch spanning two namespaces
// (nsA x3, nsB x2). Each record's result code must be counted against that
// record's OWN namespace exactly once. Issue #1001: the per-record call passed
// the whole-batch namespace iterator, so each record was counted against every
// namespace in the batch (O(N^2), inflated and mis-attributed counts).
func TestBatchGetStatsAttributedToOwnNamespace(t *testing.T) {
	mp := DefaultMetricsPolicy()
	ns := newNodeStats(mp)
	node := &Node{
		cluster: &Cluster{metricsEnabled: true},
		stats:   *ns,
	}

	nsByIndex := []string{"nsA", "nsA", "nsA", "nsB", "nsB"}
	keys := make([]*Key, len(nsByIndex))
	for i, n := range nsByIndex {
		k, err := NewKey(n, "set", i)
		if err != nil {
			t.Fatalf("NewKey(%q): %v", n, err)
		}
		keys[i] = k
	}

	// Wire response: one data header per record, then a terminator.
	var buf []byte
	for i := range keys {
		buf = append(buf, batchRecordHeader(uint32(i), 0, false)...)
	}
	buf = append(buf, batchRecordHeader(0, 0, true)...)

	conn := &Connection{dataBuffer: buf}

	cmd := &batchCommandGet{
		keys:    keys,
		records: make([]*Record, len(keys)),
	}
	cmd.node = node
	cmd.bc = bufferedConn{conn: conn, tail: len(buf)}

	if _, err := cmd.parseRecordResults(cmd, len(buf)); err != nil {
		t.Fatalf("parseRecordResults: %v", err)
	}

	counts := node.stats.marshalResultCodeCounts()
	if got := totalForNamespace(counts, "nsA"); got != 3 {
		t.Errorf("nsA total = %d, want 3 (one per nsA record)", got)
	}
	if got := totalForNamespace(counts, "nsB"); got != 2 {
		t.Errorf("nsB total = %d, want 2 (one per nsB record)", got)
	}

	// Result code rows must be ttBatchRead/0 only — no spurious cross-namespace counts.
	wantA := map[string]int{types.ResultCode(0).String(): 3}
	if got := counts["nsA"][ttBatchRead.String()]; !reflect.DeepEqual(got, wantA) {
		t.Errorf("nsA result-code counts = %v, want %v", got, wantA)
	}
	wantB := map[string]int{types.ResultCode(0).String(): 2}
	if got := counts["nsB"][ttBatchRead.String()]; !reflect.DeepEqual(got, wantB) {
		t.Errorf("nsB result-code counts = %v, want %v", got, wantB)
	}
}

// TestIncResultCodeConcurrentNoLostUpdates verifies the result-code increment
// is atomic. Issue #1001: the previous Get-then-Set read-modify-write lost
// updates when concurrent batchers incremented the same per-node counter.
func TestIncResultCodeConcurrentNoLostUpdates(t *testing.T) {
	mp := DefaultMetricsPolicy()
	ns := newNodeStats(mp)

	const goroutines = 50
	const perGoroutine = 2000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				ns.incResultCode("nsX", ttBatchRead, types.ResultCode(0))
			}
		}()
	}
	wg.Wait()

	counts := ns.marshalResultCodeCounts()
	want := goroutines * perGoroutine
	if got := counts["nsX"][ttBatchRead.String()][types.ResultCode(0).String()]; got != want {
		t.Errorf("concurrent increments lost updates: got %d, want %d", got, want)
	}
}

// BenchmarkBatchGetStatsAggregation drives the real batchCommandGet parse loop
// over batches of increasing size to show per-record stat aggregation scales
// linearly (issue #1001 fixed the prior O(N^2) per-batch cost).
func BenchmarkBatchGetStatsAggregation(b *testing.B) {
	for _, n := range []int{64, 256, 1024} {
		b.Run(fmt.Sprintf("records_%d", n), func(b *testing.B) {
			mp := DefaultMetricsPolicy()
			ns := newNodeStats(mp)
			node := &Node{cluster: &Cluster{metricsEnabled: true}, stats: *ns}

			keys := make([]*Key, n)
			for i := range keys {
				k, err := NewKey("bench", "set", i)
				if err != nil {
					b.Fatalf("NewKey: %v", err)
				}
				keys[i] = k
			}

			var buf []byte
			for i := 0; i < n; i++ {
				buf = append(buf, batchRecordHeader(uint32(i), 0, false)...)
			}
			buf = append(buf, batchRecordHeader(0, 0, true)...)
			conn := &Connection{dataBuffer: buf}

			cmd := &batchCommandGet{keys: keys, records: make([]*Record, n)}
			cmd.node = node

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cmd.bc = bufferedConn{conn: conn, tail: len(buf)}
				cmd.dataOffset = 0
				if _, err := cmd.parseRecordResults(cmd, len(buf)); err != nil {
					b.Fatalf("parseRecordResults: %v", err)
				}
			}
		})
	}
}
