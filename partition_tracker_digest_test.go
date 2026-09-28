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
	"bytes"
	"testing"
)

// newTestPartitionTracker builds a minimal partitionTracker covering every
// partition, without requiring a real cluster/Node, for unit-testing
// setLast/setDigest/allowRecord bookkeeping in isolation.
func newTestPartitionTracker() *partitionTracker {
	pt := &partitionTracker{partitionBegin: 0}
	pt.partitions = pt.initPartitions(nil, _PARTITIONS, nil)
	return pt
}

// TestPartitionTrackerSetLastCopiesDigest verifies that setLast does not
// retain a reference into the Key's backing digest array: a caller that
// reuses a single Key across many records (as the QueryPartitionsRaw hot
// path does, to avoid a per-record allocation) must not corrupt previously
// recorded resume digests for other partitions once it overwrites that Key
// for the next record.
func TestPartitionTrackerSetLastCopiesDigest(t *testing.T) {
	pt := newTestPartitionTracker()
	np := &nodePartitions{}

	const n = 32
	var key Key
	want := make([][20]byte, n)

	for i := 0; i < n; i++ {
		var d [20]byte
		for b := range d {
			d[b] = byte(i*7 + b + 1)
		}
		want[i] = d

		// Reuse the same Key value for every record, exactly like the raw
		// record path's per-command Key buffer.
		key.digest = d
		pt.setLast(np, &key, nil)
	}

	// Overwrite the shared Key's digest one more time, simulating the
	// command moving on to a further record after the loop above. If setLast
	// had aliased key.digest instead of copying it, every partition's stored
	// digest would now read back as this value.
	key.digest = [20]byte{0xEE, 0xEE, 0xEE, 0xEE}

	for i := 0; i < n; i++ {
		d := want[i]
		partitionId := (&Key{digest: d}).PartitionId()
		got := pt.partitions[partitionId].Digest
		if !bytes.Equal(got, d[:]) {
			t.Fatalf("record %d: partition %d resume digest = %x, want %x (looks aliased/corrupted)", i, partitionId, got, d)
		}
	}
}

// TestPartitionTrackerSetDigestCopiesDigest is the setDigest (scan-path)
// counterpart of TestPartitionTrackerSetLastCopiesDigest.
func TestPartitionTrackerSetDigestCopiesDigest(t *testing.T) {
	pt := newTestPartitionTracker()
	np := &nodePartitions{}

	const n = 32
	var key Key
	want := make([][20]byte, n)

	for i := 0; i < n; i++ {
		var d [20]byte
		for b := range d {
			d[b] = byte(i*11 + b + 3)
		}
		want[i] = d

		key.digest = d
		pt.setDigest(np, &key)
	}

	key.digest = [20]byte{0xDD, 0xDD, 0xDD, 0xDD}

	for i := 0; i < n; i++ {
		d := want[i]
		partitionId := (&Key{digest: d}).PartitionId()
		got := pt.partitions[partitionId].Digest
		if !bytes.Equal(got, d[:]) {
			t.Fatalf("record %d: partition %d digest = %x, want %x (looks aliased/corrupted)", i, partitionId, got, d)
		}
	}
}

// TestPartitionStatusSetDigestReusesBackingArray checks that repeated calls
// reuse the existing Digest backing array (the actual allocation-avoidance
// property the raw record path depends on), while still producing correct,
// independent content.
func TestPartitionStatusSetDigestReusesBackingArray(t *testing.T) {
	ps := newPartitionStatus(0)

	ps.setDigest([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	first := ps.Digest
	if cap(first) < 20 {
		t.Fatalf("expected Digest to be allocated with cap >= 20, got %d", cap(first))
	}

	ps.setDigest([]byte{20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1})
	if &first[0] != &ps.Digest[0] {
		t.Fatalf("expected setDigest to reuse the existing backing array when it fits")
	}
	if !bytes.Equal(ps.Digest, []byte{20, 19, 18, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}) {
		t.Fatalf("unexpected digest content after reuse: %x", ps.Digest)
	}
}

// TestInitPartitionsDoesNotAliasCallerDigest guards against a regression
// found while adding PartitionStatus.setDigest's backing-array reuse:
// initPartitions used to assign the caller-supplied starting digest
// (typically a PartitionFilter's Digest field, itself often an alias of a
// user's *Key.digest -- see NewPartitionFilterByKey) directly to
// PartitionStatus.Digest, with no copy. Once setDigest started reusing an
// existing same-or-larger-capacity backing array in place instead of always
// allocating fresh, that direct assignment meant the very first record found
// for the partition would silently overwrite the *caller's own* Key/
// PartitionFilter digest bytes. Concretely, this broke
// "Scan/Query ... for a specified key" (query_test.go/scan_test.go), whose
// PartitionFilter is built from a live *Key via NewPartitionFilterByKey and
// which asserts that key's digest is excluded from, and unmutated by, the
// results.
func TestInitPartitionsDoesNotAliasCallerDigest(t *testing.T) {
	var keyDigest [20]byte
	for i := range keyDigest {
		keyDigest[i] = byte(i + 1)
	}
	original := append([]byte(nil), keyDigest[:]...)

	key := &Key{digest: keyDigest}
	filter := NewPartitionFilterByKey(key)

	pt := &partitionTracker{partitionBegin: filter.Begin}
	partitions := pt.initPartitions(nil, filter.Count, filter.Digest)

	// Simulate the first record found for this partition updating the
	// resume digest, exactly as setLast/setDigest do mid-scan.
	var nextDigest [20]byte
	for i := range nextDigest {
		nextDigest[i] = byte(0xA0 + i)
	}
	partitions[0].setDigest(nextDigest[:])

	if !bytes.Equal(key.digest[:], original) {
		t.Fatalf("initPartitions/setDigest mutated the caller's Key digest in place: got %x, want %x", key.digest[:], original)
	}
	if !bytes.Equal(filter.Digest, original) {
		t.Fatalf("initPartitions/setDigest mutated the caller's PartitionFilter.Digest in place: got %x, want %x", filter.Digest, original)
	}
	if !bytes.Equal(partitions[0].Digest, nextDigest[:]) {
		t.Fatalf("partitions[0].Digest = %x, want %x", partitions[0].Digest, nextDigest[:])
	}
}
