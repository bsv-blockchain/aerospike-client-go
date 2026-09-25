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

package aerospike_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	ptype "github.com/bsv-blockchain/aerospike-client-go/v8/types/particle_type"
	buf "github.com/bsv-blockchain/aerospike-client-go/v8/utils/buffer"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// decodeRawBinValue decodes the subset of particle types written by this test
// suite (int, string, bytes, bool) the same way the standard client does, so
// results captured via RawRecordHandler can be compared against
// Record.Bins built by QueryPartitions.
func decodeRawBinValue(particleType int, value []byte) (any, error) {
	switch particleType {
	case ptype.INTEGER:
		return int(buf.VarBytesToInt64(value, 0, len(value))), nil
	case ptype.STRING:
		return string(value), nil
	case ptype.BLOB:
		cp := make([]byte, len(value))
		copy(cp, value)
		return cp, nil
	case ptype.BOOL:
		return buf.BytesToBool(value, 0, len(value)), nil
	default:
		return nil, fmt.Errorf("unexpected particle type %d", particleType)
	}
}

// capturingRawHandler records every accepted record as digest -> bins,
// mimicking what QueryPartitions/Results() would hand back for the same
// query, for equality comparisons in tests. Multiple instances (one per node
// command) share the same results map/mutex/counters.
type capturingRawHandler struct {
	mu        *sync.Mutex
	results   map[string]map[string]any
	discarded *atomic.Int64

	// per-record scratch state
	curDigest string
	curBins   map[string]any
	curErr    error
}

func newCapturingRawHandlers() (map[string]map[string]any, *atomic.Int64, func() as.RawRecordHandler) {
	results := make(map[string]map[string]any)
	mu := &sync.Mutex{}
	discarded := &atomic.Int64{}

	newHandler := func() as.RawRecordHandler {
		return &capturingRawHandler{mu: mu, results: results, discarded: discarded}
	}

	return results, discarded, newHandler
}

func (h *capturingRawHandler) BeginRecord(digest []byte, generation, expiration uint32) error {
	// digest is only valid for the duration of this call: copy it.
	h.curDigest = string(digest)
	h.curBins = make(map[string]any)
	h.curErr = nil
	return nil
}

func (h *capturingRawHandler) Bin(name []byte, particleType int, value []byte) error {
	v, err := decodeRawBinValue(particleType, value)
	if err != nil {
		h.curErr = err
		return nil
	}
	// name/value are only valid for the duration of this call: copy the name.
	h.curBins[string(name)] = v
	return nil
}

func (h *capturingRawHandler) EndRecord() error {
	if h.curErr != nil {
		return h.curErr
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.results[h.curDigest]; exists {
		return fmt.Errorf("digest %x delivered more than once", h.curDigest)
	}
	h.results[h.curDigest] = h.curBins
	return nil
}

func (h *capturingRawHandler) DiscardRecord() {
	h.discarded.Add(1)
}

// errorRawHandler always fails on the Nth bin delivered across all handler
// instances, to exercise abort semantics.
type errorRawHandler struct {
	failAfter *atomic.Int64
	sentinel  error
}

func (h *errorRawHandler) BeginRecord(digest []byte, generation, expiration uint32) error {
	return nil
}

func (h *errorRawHandler) Bin(name []byte, particleType int, value []byte) error {
	if h.failAfter.Add(-1) == 0 {
		return h.sentinel
	}
	return nil
}

func (h *errorRawHandler) EndRecord() error { return nil }

func (h *errorRawHandler) DiscardRecord() {}

// countingRawHandler increments a shared per-digest counter for every record
// accepted, to verify exactly-once delivery across disjoint partition
// filters.
type countingRawHandler struct {
	mu   *sync.Mutex
	seen map[string]int

	curDigest string
}

func (h *countingRawHandler) BeginRecord(digest []byte, generation, expiration uint32) error {
	h.curDigest = string(digest)
	return nil
}

func (h *countingRawHandler) Bin(name []byte, particleType int, value []byte) error { return nil }

func (h *countingRawHandler) EndRecord() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seen[h.curDigest]++
	return nil
}

func (h *countingRawHandler) DiscardRecord() {}

// decodeParticleHandler forwards every (name, particleType, value) triple of
// the record(s) it sees to onBin, for tests that want to run values through
// as.DecodeParticle themselves.
type decodeParticleHandler struct {
	onBin func(name string, particleType int, value []byte)
}

func (h *decodeParticleHandler) BeginRecord(digest []byte, generation, expiration uint32) error {
	return nil
}

func (h *decodeParticleHandler) Bin(name []byte, particleType int, value []byte) error {
	h.onBin(string(name), particleType, value)
	return nil
}

func (h *decodeParticleHandler) EndRecord() error { return nil }

func (h *decodeParticleHandler) DiscardRecord() {}

var _ = gg.Describe("QueryPartitionsRaw operations", func() {

	var ns = *namespace
	var set string
	var wpolicy = as.NewWritePolicy(0, 0)

	const keyCount = 2000
	var indexName string

	makeBins := func(i int) []*as.Bin {
		return []*as.Bin{
			as.NewBin("BinInt", i),
			as.NewBin("BinStr", fmt.Sprintf("value-%d", i)),
			as.NewBin("BinBytes", []byte{byte(i), byte(i >> 8), 0xFF}),
			as.NewBin("BinBool", i%2 == 0),
			as.NewBin("BinFilter", i),
		}
	}

	gg.BeforeEach(func() {
		set = randString(50)

		for i := 0; i < keyCount; i++ {
			key, err := as.NewKey(ns, set, fmt.Sprintf("raw-%d", i))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(client.PutBins(wpolicy, key, makeBins(i)...)).ToNot(gm.HaveOccurred())
		}

		// a few records outside the [0, keyCount) filter range
		for i := keyCount; i < keyCount+10; i++ {
			key, err := as.NewKey(ns, set, fmt.Sprintf("raw-%d", i))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(client.PutBins(wpolicy, key, makeBins(i*1000)...)).ToNot(gm.HaveOccurred())
		}

		indexName = set + "BinFilter"
		createIndex(wpolicy, ns, set, indexName, "BinFilter", as.NUMERIC)
	})

	gg.AfterEach(func() {
		gm.Expect(client.DropIndex(nil, ns, set, indexName)).ToNot(gm.HaveOccurred())
	})

	gg.It("delivers every matching record exactly once, matching QueryPartitions bin-for-bin", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter("BinFilter", 0, keyCount-1))

		// Reference: what QueryPartitions returns for the same query.
		refPolicy := as.NewQueryPolicy()
		refRecordset, err := client.QueryPartitions(refPolicy, stm, nil)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		expected := make(map[string]map[string]any, keyCount)
		for res := range refRecordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			expected[string(res.Record.Key.Digest())] = res.Record.Bins
		}
		gm.Expect(expected).To(gm.HaveLen(keyCount))

		// Actual: RawRecordHandler-based query.
		results, discarded, newHandler := newCapturingRawHandlers()

		rawPolicy := as.NewQueryPolicy()
		rawErr := client.QueryPartitionsRaw(rawPolicy, stm, nil, newHandler)
		gm.Expect(rawErr).ToNot(gm.HaveOccurred())

		gm.Expect(discarded.Load()).To(gm.BeNumerically("==", 0))
		gm.Expect(results).To(gm.HaveLen(keyCount))
		gm.Expect(results).To(gm.Equal(expected))
	})

	gg.It("covers every record exactly once across disjoint partition filter subranges", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter("BinFilter", 0, keyCount-1))

		total := 4096
		half := total / 2

		var mu sync.Mutex
		seen := make(map[string]int)

		for _, pf := range []*as.PartitionFilter{
			as.NewPartitionFilterByRange(0, half),
			as.NewPartitionFilterByRange(half, total-half),
		} {
			newHandler := func() as.RawRecordHandler {
				return &countingRawHandler{mu: &mu, seen: seen}
			}

			rawPolicy := as.NewQueryPolicy()
			rawErr := client.QueryPartitionsRaw(rawPolicy, stm, pf, newHandler)
			gm.Expect(rawErr).ToNot(gm.HaveOccurred())
		}

		mu.Lock()
		defer mu.Unlock()
		gm.Expect(seen).To(gm.HaveLen(keyCount))
		for k, v := range seen {
			gm.Expect(v).To(gm.Equal(1), "record %x delivered %d times", k, v)
		}
	})

	gg.It("aborts and returns the handler's error", func() {
		// Default MaxConcurrentNodes (all nodes run concurrently): on this
		// single-node test cluster that still means one command per round,
		// but must not rely on the MaxConcurrentNodes=1 restriction that
		// used to pin this test to a single in-flight command -- see
		// TestQueryPartitionsRawResultPrefersHandlerErrorRegardlessOfOrder
		// for the deterministic, order-covering version of this assertion.
		// Repeated a few times for extra confidence.
		for i := 0; i < 10; i++ {
			stm := as.NewStatement(ns, set)
			stm.SetFilter(as.NewRangeFilter("BinFilter", 0, keyCount-1))

			sentinel := errors.New("boom")
			failAfter := &atomic.Int64{}
			failAfter.Store(50)

			newHandler := func() as.RawRecordHandler {
				return &errorRawHandler{failAfter: failAfter, sentinel: sentinel}
			}

			policy := as.NewQueryPolicy()
			err := client.QueryPartitionsRaw(policy, stm, nil, newHandler)

			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(errors.Is(err, sentinel)).To(gm.BeTrue(), "iteration %d", i)
		}
	})

	gg.It("respects MaxConcurrentNodes without breaking delivery", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter("BinFilter", 0, keyCount-1))

		results, discarded, newHandler := newCapturingRawHandlers()

		policy := as.NewQueryPolicy()
		policy.MaxConcurrentNodes = 1
		err := client.QueryPartitionsRaw(policy, stm, nil, newHandler)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		gm.Expect(discarded.Load()).To(gm.BeNumerically("==", 0))
		gm.Expect(results).To(gm.HaveLen(keyCount))
	})

	gg.It("DecodeParticle decodes list bins the same way QueryPartitions' BinMap does", func() {
		dpSet := randString(50)
		key, err := as.NewKey(ns, dpSet, "decode-particle")
		gm.Expect(err).ToNot(gm.HaveOccurred())

		intList := []any{1, 2, 3, 4, 5, math.MaxInt32}
		byteList := []any{[]byte{0x01, 0x02, 0x03}, []byte{0xFF, 0xFE}, []byte{}}

		gm.Expect(client.PutBins(wpolicy, key,
			as.NewBin("IntList", intList),
			as.NewBin("ByteList", byteList),
			as.NewBin("DPFilter", 1),
		)).ToNot(gm.HaveOccurred())

		dpIndexName := dpSet + "DPFilter"
		createIndex(wpolicy, ns, dpSet, dpIndexName, "DPFilter", as.NUMERIC)
		defer func() {
			gm.Expect(client.DropIndex(nil, ns, dpSet, dpIndexName)).ToNot(gm.HaveOccurred())
		}()

		stm := as.NewStatement(ns, dpSet)
		stm.SetFilter(as.NewRangeFilter("DPFilter", 1, 1))

		// Reference: QueryPartitions' own decoded BinMap.
		refRecordset, refErr := client.QueryPartitions(as.NewQueryPolicy(), stm, nil)
		gm.Expect(refErr).ToNot(gm.HaveOccurred())

		var expectedIntList, expectedByteList any
		found := false
		for res := range refRecordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			expectedIntList = res.Record.Bins["IntList"]
			expectedByteList = res.Record.Bins["ByteList"]
			found = true
		}
		gm.Expect(found).To(gm.BeTrue())

		// Actual: raw particle bytes run through as.DecodeParticle.
		var gotIntList, gotByteList any
		gotFound := false

		newHandler := func() as.RawRecordHandler {
			return &decodeParticleHandler{
				onBin: func(name string, particleType int, value []byte) {
					v, derr := as.DecodeParticle(particleType, value)
					gm.Expect(derr).ToNot(gm.HaveOccurred())
					switch name {
					case "IntList":
						gotIntList = v
						gotFound = true
					case "ByteList":
						gotByteList = v
						gotFound = true
					}
				},
			}
		}

		rawErr := client.QueryPartitionsRaw(as.NewQueryPolicy(), stm, nil, newHandler)
		gm.Expect(rawErr).ToNot(gm.HaveOccurred())

		gm.Expect(gotFound).To(gm.BeTrue())
		gm.Expect(gotIntList).To(gm.Equal(expectedIntList))
		gm.Expect(gotByteList).To(gm.Equal(expectedByteList))
	})

	gg.It("resumes correctly across paginated calls sharing a PartitionFilter (exercises the per-command reused Key's resume digest)", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter("BinFilter", 0, keyCount-1))

		// All partitions, small MaxRecords: this deterministically forces
		// keyCount/MaxRecords pages (~40 here) regardless of how this run's
		// random key digests happen to be distributed across partitions
		// (unlike restricting to a handful of partitions up front, whose
		// record count -- and therefore whether pagination happens at all --
		// would depend on that same random distribution). With that many
		// pages over only 4096 partitions, at least some pages are
		// overwhelmingly likely to contain multiple records for the same
		// partition, processed back to back by a single node command --
		// exactly the scenario in which reusing one Key struct across
		// records (instead of allocating a fresh one per record) would have
		// corrupted every partition's resume digest but the last one
		// written, had partitionTracker.setLast/setDigest still aliased the
		// Key's backing array instead of copying it (see
		// PartitionStatus.setDigest).
		pf := as.NewPartitionFilterAll()

		results, discarded, newHandler := newCapturingRawHandlers()

		pages := 0
		for {
			pages++
			gm.Expect(pages).To(gm.BeNumerically("<", 1000), "pagination did not converge")

			policy := as.NewQueryPolicy()
			policy.MaxRecords = 50 // force many small pages, many resumes
			rawErr := client.QueryPartitionsRaw(policy, stm, pf, newHandler)
			gm.Expect(rawErr).ToNot(gm.HaveOccurred())

			if pf.Done {
				break
			}
		}

		gm.Expect(pages).To(gm.BeNumerically(">", 1),
			"test is only meaningful if pagination actually spans multiple calls")
		gm.Expect(discarded.Load()).To(gm.BeNumerically("==", 0))

		// Ground truth: every record QueryPartitions (unpaginated) sees for
		// the same filter.
		refRecordset, err := client.QueryPartitions(as.NewQueryPolicy(), stm, nil)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		expectedCount := 0
		for res := range refRecordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			expectedCount++
		}

		// If the resume digest had been corrupted, this would show up as
		// either duplicates (capturingRawHandler.EndRecord errors on a
		// digest seen twice, which would have surfaced as rawErr above) or
		// missing records (fewer results than expectedCount, because a
		// later page skipped past records past some other record's wrongly
		// "resumed from" digest).
		gm.Expect(results).To(gm.HaveLen(expectedCount))
	})

	gg.It("QueryPartitionsRawContext returns promptly on cancellation, with no handler calls afterward", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter("BinFilter", 0, keyCount-1))

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var calls atomic.Int64
		var cancelled atomic.Bool
		newHandler := func() as.RawRecordHandler {
			return &cancelingRawHandler{
				calls: &calls,
				onBin: func() {
					// Cancel partway through, exactly once.
					if calls.Load() > 20 && cancelled.CompareAndSwap(false, true) {
						cancel()
					}
				},
			}
		}

		start := time.Now()
		err := client.QueryPartitionsRawContext(ctx, as.NewQueryPolicy(), stm, nil, newHandler)
		elapsed := time.Since(start)

		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(errors.Is(err, context.Canceled)).To(gm.BeTrue())
		// Well under what fully draining keyCount records would take: this
		// is a bound on the whole call, not a race against a real deadline.
		gm.Expect(elapsed).To(gm.BeNumerically("<", 5*time.Second))

		callsAtReturn := calls.Load()
		gm.Consistently(func() int64 { return calls.Load() }, "100ms", "10ms").Should(gm.Equal(callsAtReturn))
	})

	gg.It("QueryPartitionsRawContext does not leak its context-watcher goroutine", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter("BinFilter", 0, keyCount-1))

		before := runtime.NumGoroutine()

		for i := 0; i < 20; i++ {
			_, discarded, newHandler := newCapturingRawHandlers()
			err := client.QueryPartitionsRawContext(context.Background(), as.NewQueryPolicy(), stm, nil, newHandler)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(discarded.Load()).To(gm.BeNumerically("==", 0))
		}

		runtime.GC()
		var after int
		gm.Eventually(func() int {
			after = runtime.NumGoroutine()
			return after
		}, "2s", "20ms").Should(gm.BeNumerically("<=", before+2),
			"goroutine count grew from %d to %d after 20 QueryPartitionsRawContext calls: context watcher goroutine leak?", before, after)
	})
})

// cancelingRawHandler counts every Bin call and invokes onBin after each one,
// for tests driving external cancellation from inside a handler callback.
type cancelingRawHandler struct {
	calls *atomic.Int64
	onBin func()
}

func (h *cancelingRawHandler) BeginRecord(digest []byte, generation, expiration uint32) error {
	return nil
}

func (h *cancelingRawHandler) Bin(name []byte, particleType int, value []byte) error {
	h.calls.Add(1)
	if h.onBin != nil {
		h.onBin()
	}
	return nil
}

func (h *cancelingRawHandler) EndRecord() error { return nil }

func (h *cancelingRawHandler) DiscardRecord() {}
