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
	"fmt"
	"testing"

	as "github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
)

// noopRawHandler is the raw-path counterpart of draining Recordset.Results()
// and discarding every record: it does the minimum amount of work needed to
// actually touch every bin, without allocating anything.
type noopRawHandler struct{}

func (noopRawHandler) BeginRecord(digest []byte, generation, expiration uint32) error { return nil }
func (noopRawHandler) Bin(name []byte, particleType int, value []byte) error          { return nil }
func (noopRawHandler) EndRecord() error                                               { return nil }
func (noopRawHandler) DiscardRecord()                                                 {}

func newNoopRawHandler() as.RawRecordHandler { return noopRawHandler{} }

func benchSetupQueryPartitionsRawData(b *testing.B, n int) (ns, set, indexName string) {
	b.Helper()

	ns = *namespace
	set = fmt.Sprintf("benchqpr%d", n)
	wpolicy := as.NewWritePolicy(0, 0)

	for i := 0; i < n; i++ {
		key, err := as.NewKey(ns, set, fmt.Sprintf("k-%d", i))
		if err != nil {
			b.Fatal(err)
		}
		err = client.PutBins(wpolicy, key,
			as.NewBin("BinInt", i),
			as.NewBin("BinStr", fmt.Sprintf("value-%d", i)),
			as.NewBin("BinBytes", []byte{byte(i), byte(i >> 8), 0xFF}),
			as.NewBin("BinBool", i%2 == 0),
			as.NewBin("BinFilter", i),
		)
		if err != nil {
			b.Fatal(err)
		}
	}

	indexName = set + "BinFilter"
	idxTask, err := client.CreateIndex(wpolicy, ns, set, indexName, "BinFilter", as.NUMERIC)
	if err == nil {
		if ierr := <-idxTask.OnComplete(); ierr != nil {
			b.Fatal(ierr)
		}
	} else if !err.Matches(types.INDEX_FOUND) {
		b.Fatal(err)
	}

	return ns, set, indexName
}

// BenchmarkQueryPartitions_Drain measures the existing QueryPartitions API:
// a channel-based Recordset drained by the calling goroutine, decoding every
// bin into a BinMap.
func BenchmarkQueryPartitions_Drain(b *testing.B) {
	const n = 20000
	ns, set, indexName := benchSetupQueryPartitionsRawData(b, n)
	defer client.DropIndex(nil, ns, set, indexName)

	stm := as.NewStatement(ns, set)
	stm.SetFilter(as.NewRangeFilter("BinFilter", 0, n-1))
	policy := as.NewQueryPolicy()

	b.ReportAllocs()
	b.ResetTimer()

	total := 0
	for i := 0; i < b.N; i++ {
		recordset, err := client.QueryPartitions(policy, stm, nil)
		if err != nil {
			b.Fatal(err)
		}
		for res := range recordset.Results() {
			if res.Err != nil {
				b.Fatal(res.Err)
			}
			total++
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/record")
	_ = total
}

// BenchmarkQueryPartitionsRaw_Noop measures QueryPartitionsRaw with a
// zero-allocation handler that does the minimum work to touch every bin.
func BenchmarkQueryPartitionsRaw_Noop(b *testing.B) {
	const n = 20000
	ns, set, indexName := benchSetupQueryPartitionsRawData(b, n)
	defer client.DropIndex(nil, ns, set, indexName)

	stm := as.NewStatement(ns, set)
	stm.SetFilter(as.NewRangeFilter("BinFilter", 0, n-1))
	policy := as.NewQueryPolicy()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := client.QueryPartitionsRaw(policy, stm, nil, newNoopRawHandler); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/record")
}
