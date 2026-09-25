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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

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
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter("BinFilter", 0, keyCount-1))

		sentinel := errors.New("boom")
		failAfter := &atomic.Int64{}
		failAfter.Store(50)

		newHandler := func() as.RawRecordHandler {
			return &errorRawHandler{failAfter: failAfter, sentinel: sentinel}
		}

		policy := as.NewQueryPolicy()
		policy.MaxConcurrentNodes = 1
		err := client.QueryPartitionsRaw(policy, stm, nil, newHandler)

		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(errors.Is(err, sentinel)).To(gm.BeTrue())
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
})
