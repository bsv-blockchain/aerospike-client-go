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
	"encoding/binary"
	"errors"
	"testing"

	atmc "github.com/bsv-blockchain/aerospike-client-go/v8/internal/atomic"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
)

// queryRecordDigestOnly builds a single query-response record
// (_MSG_REMAINING_HEADER_SIZE header, one DIGEST_RIPE field, no ops), as
// parsed by baseMultiCommand.parseRecordResults / parseKey / parseKeyDigest.
func queryRecordDigestOnly(digest []byte, last bool) []byte {
	h := make([]byte, int(_MSG_REMAINING_HEADER_SIZE))
	if last {
		h[3] = byte(_INFO3_LAST)
	}
	// h[5] = resultCode = 0
	// generation [6:10], expiration [10:14] left as 0
	binary.BigEndian.PutUint16(h[18:20], 1) // fieldCount
	// opCount [20:22] = 0

	field := make([]byte, 4+1+len(digest))
	binary.BigEndian.PutUint32(field[0:4], uint32(len(digest)+1))
	field[4] = byte(DIGEST_RIPE)
	copy(field[5:], digest)

	return append(h, field...)
}

// queryRecordNoFields builds a query-response record with no fields at all
// (fieldCount=0, no DIGEST_RIPE) and no ops.
func queryRecordNoFields() []byte {
	h := make([]byte, int(_MSG_REMAINING_HEADER_SIZE))
	// fieldCount [18:20] = 0, opCount [20:22] = 0
	return h
}

// queryEndMarker builds the zero-field, zero-op terminator record that
// signals the end of a query response (_INFO3_LAST), as sent after the last
// real record.
func queryEndMarker() []byte {
	h := make([]byte, int(_MSG_REMAINING_HEADER_SIZE))
	h[3] = byte(_INFO3_LAST)
	return h
}

// fakeRawHandler is a minimal RawRecordHandler for driving
// parseRecordResults directly in unit tests, without a live connection.
type fakeRawHandler struct {
	onBegin   func(digest []byte, generation, expiration uint32) error
	onBin     func(name []byte, particleType int, value []byte) error
	onEnd     func() error
	onDiscard func()
}

func (h *fakeRawHandler) BeginRecord(digest []byte, generation, expiration uint32) error {
	if h.onBegin != nil {
		return h.onBegin(digest, generation, expiration)
	}
	return nil
}

func (h *fakeRawHandler) Bin(name []byte, particleType int, value []byte) error {
	if h.onBin != nil {
		return h.onBin(name, particleType, value)
	}
	return nil
}

func (h *fakeRawHandler) EndRecord() error {
	if h.onEnd != nil {
		return h.onEnd()
	}
	return nil
}

func (h *fakeRawHandler) DiscardRecord() {
	if h.onDiscard != nil {
		h.onDiscard()
	}
}

// samePartitionDigests returns two 20-byte digests that hash to the same
// partition id (PartitionId only looks at the first 4 bytes), but are
// otherwise different, so records built from them are distinguishable.
func samePartitionDigests() (d1, d2 [20]byte, partitionId int) {
	for i := 0; i < 4; i++ {
		d1[i] = 0x11
		d2[i] = 0x11
	}
	for i := 4; i < 20; i++ {
		d1[i] = byte(i)
		d2[i] = byte(i + 100)
	}
	return d1, d2, (&Key{digest: d1}).PartitionId()
}

// newRawTestCommand builds a real queryPartitionRawCommand (so it satisfies
// the full command interface parseRecordResults is invoked through) wired up
// to read buf as its connection response, with the given tracker/handler,
// ready for parseRecordResults to be called directly without a live
// connection.
func newRawTestCommand(buf []byte, tracker *partitionTracker, np *nodePartitions, handler RawRecordHandler) *queryPartitionRawCommand {
	node := &Node{cluster: &Cluster{}}
	np.node = node

	statement := NewStatement("test", "set")
	recordset := newRecordset(0, 1)

	cmd := newQueryPartitionRawCommand(NewQueryPolicy(), tracker, np, statement, recordset, handler)
	cmd.node = node

	conn := &Connection{dataBuffer: buf}
	cmd.bc = bufferedConn{conn: conn, tail: len(buf)}

	return cmd
}

// TestRawRecordDiscardDoesNotAdvanceResumeDigest exercises allowRecord
// returning false mid-stream (pt.recordCount != nil and maxRecords reached),
// directly at the wire-parsing level: real Aerospike clusters only expose
// this path client-side when maxRecords < the number of distinct server
// nodes in a round (see partitionTracker.assignPartitionsToNodes), which a
// single-node test cluster can never produce.
//
// Two records for the *same* partition are delivered in one response: the
// first is accepted (recordCount 0 -> 1, within maxRecords=1), the second is
// rejected (recordCount 1 -> 2, over the limit) and must be discarded
// without moving the partition's resume digest past it -- otherwise a
// following page would skip the discarded record entirely.
func TestRawRecordDiscardDoesNotAdvanceResumeDigest(t *testing.T) {
	digest1, digest2, partitionId := samePartitionDigests()

	buf := queryRecordDigestOnly(digest1[:], false)
	buf = append(buf, queryRecordDigestOnly(digest2[:], false)...)
	buf = append(buf, queryEndMarker()...)

	tracker := &partitionTracker{
		partitionBegin: partitionId,
		partitions:     []*PartitionStatus{newPartitionStatus(partitionId)},
		recordCount:    atmc.NewInt(0),
		maxRecords:     1,
	}
	np := &nodePartitions{}

	var ended, discarded int
	handler := &fakeRawHandler{
		onEnd:     func() error { ended++; return nil },
		onDiscard: func() { discarded++ },
	}

	cmd := newRawTestCommand(buf, tracker, np, handler)
	if _, err := cmd.parseRecordResults(cmd, len(buf)); err != nil {
		t.Fatalf("parseRecordResults: %v", err)
	}

	if ended != 1 {
		t.Errorf("EndRecord called %d times, want 1", ended)
	}
	if discarded != 1 {
		t.Errorf("DiscardRecord called %d times, want 1", discarded)
	}

	got := tracker.partitions[0].Digest
	if !bytes.Equal(got, digest1[:]) {
		t.Errorf("resume digest = %x, want %x (record 1's digest; record 2 was discarded and must not move the cursor past it)", got, digest1[:])
	}
	if np.recordCount != 1 {
		t.Errorf("nodePartitions.recordCount = %d, want 1 (discarded record must not count)", np.recordCount)
	}
}

// TestRawKeyDigestDoesNotLeakAcrossRecords is a regression test for the
// reused cmd.rawKey.digest array retaining a previous record's bytes when a
// later record has no (or a short) DIGEST_RIPE field: BeginRecord must see
// an all-zero digest for such a record, not the previous one's.
func TestRawKeyDigestDoesNotLeakAcrossRecords(t *testing.T) {
	digest1, _, partitionId := samePartitionDigests()

	buf := queryRecordDigestOnly(digest1[:], false)
	buf = append(buf, queryRecordNoFields()...)
	buf = append(buf, queryEndMarker()...)

	tracker := &partitionTracker{
		partitionBegin: partitionId,
		partitions:     []*PartitionStatus{newPartitionStatus(partitionId)},
	}
	np := &nodePartitions{}

	var digests [][]byte
	handler := &fakeRawHandler{
		onBegin: func(digest []byte, generation, expiration uint32) error {
			d := make([]byte, len(digest))
			copy(d, digest)
			digests = append(digests, d)
			return nil
		},
	}

	cmd := newRawTestCommand(buf, tracker, np, handler)
	if _, err := cmd.parseRecordResults(cmd, len(buf)); err != nil {
		t.Fatalf("parseRecordResults: %v", err)
	}

	if len(digests) != 2 {
		t.Fatalf("BeginRecord called %d times, want 2", len(digests))
	}
	if !bytes.Equal(digests[0], digest1[:]) {
		t.Errorf("record 1 digest = %x, want %x", digests[0], digest1[:])
	}
	var zero [20]byte
	if !bytes.Equal(digests[1], zero[:]) {
		t.Errorf("record 2 (no DIGEST_RIPE field) digest = %x, want all-zero %x (leaked record 1's digest)", digests[1], zero[:])
	}
}

// TestQueryPartitionsRawResultPrefersHandlerErrorRegardlessOfOrder is a
// regression test for the handler error being lost from
// QueryPartitionsRaw's returned error depending on which of possibly
// several concurrently aborting node commands' errors chainErrors happens
// to see last (see queryPartitionsRawResult/Recordset.abort). It drives the
// exact mechanism directly and deterministically -- no real concurrency,
// network, or timing involved -- in both possible arrival orders.
func TestQueryPartitionsRawResultPrefersHandlerErrorRegardlessOfOrder(t *testing.T) {
	sentinel := errors.New("handler boom")
	handlerErr := newNodeError(&Node{}, newCommonError(sentinel, "RawRecordHandler.Bin failed"))
	siblingErr := ErrQueryTerminated.err().setNode(&Node{})

	t.Run("handler error observed first", func(t *testing.T) {
		rs := newRecordset(0, 1)
		rs.abort(handlerErr)

		// Simulate a second, sibling node command finishing after this one
		// and chaining its own (different) termination error on top, as
		// werrGroup.execute does for every command's returned error.
		errs := chainErrors(siblingErr, handlerErr)

		got := queryPartitionsRawResult(rs, errs)
		if !errors.Is(got, sentinel) {
			t.Errorf("errors.Is(got, sentinel) = false, want true (got: %v)", got)
		}
	})

	t.Run("handler error observed last", func(t *testing.T) {
		rs := newRecordset(0, 1)
		rs.abort(handlerErr)

		// Same two errors, chained in the opposite order: this is exactly
		// the ordering the review identified as dropping the handler error
		// out of the chain when relying on chainErrors alone.
		errs := chainErrors(handlerErr, siblingErr)

		got := queryPartitionsRawResult(rs, errs)
		if !errors.Is(got, sentinel) {
			t.Errorf("errors.Is(got, sentinel) = false, want true (got: %v)", got)
		}
	})

	t.Run("first abort call wins when multiple handler errors race", func(t *testing.T) {
		sentinel2 := errors.New("second handler boom")
		handlerErr2 := newNodeError(&Node{}, newCommonError(sentinel2, "RawRecordHandler.Bin failed"))

		rs := newRecordset(0, 1)
		rs.abort(handlerErr)
		rs.abort(handlerErr2) // a second, concurrent abort: must not replace the first

		got := queryPartitionsRawResult(rs, nil)
		if !errors.Is(got, sentinel) {
			t.Errorf("errors.Is(got, sentinel) = false, want true: first abort() call must win (got: %v)", got)
		}
		if errors.Is(got, sentinel2) {
			t.Errorf("errors.Is(got, sentinel2) = true, want false: second, later abort() call must not win")
		}
	})
}

// sanity: chainErrors itself really does exhibit the order-dependent
// dropping behavior the fix works around, so the test above is exercising a
// real mechanism and not a strawman.
func TestChainErrorsCanDropAnEarlierWrappedSentinel(t *testing.T) {
	sentinel := errors.New("boom")
	handlerErr := newNodeError(&Node{}, newCommonError(sentinel, "handler failed"))
	siblingErr := newError(types.QUERY_TERMINATED).setNode(&Node{})

	// chainErrors(outer, inner) copies outer's shell but overwrites its
	// wrapped chain with inner, discarding whatever outer used to wrap.
	// werrGroup accumulates via chainErrors(thisCmdsErr, accumulatedSoFar):
	// when the handler's own error is that "outer" (i.e. it finishes after
	// something else already accumulated), its own wrapped chain --
	// commonErr -> sentinel -- is what gets discarded.
	dropped := chainErrors(handlerErr, siblingErr)
	if errors.Is(dropped, sentinel) {
		t.Fatalf("test assumption violated: chainErrors(handlerErr, siblingErr) still finds sentinel; the mechanism this fix targets no longer applies")
	}

	// The other arrival order -- handler error accumulated first, chained as
	// "inner" once a sibling's error follows -- happens to preserve it,
	// since inner is kept as-is (with its own full chain) rather than
	// stripped down to just its own fields. This is exactly the "depends on
	// arrival order" the fix removes the dependency on.
	kept := chainErrors(siblingErr, handlerErr)
	if !errors.Is(kept, sentinel) {
		t.Fatalf("test assumption violated: chainErrors(siblingErr, handlerErr) lost sentinel too")
	}
}
