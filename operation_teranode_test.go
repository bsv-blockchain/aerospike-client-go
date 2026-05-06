// Copyright 2026 BSV Association.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package aerospike

import (
	"bytes"
	"testing"
)

// TestTeranodeModifyOpWireBytes verifies that TeranodeModifyOp produces
// an Operation whose wire opcode byte is 200 — the BSV vendor-reserved
// AS_MSG_OP_TERANODE_MODIFY value defined in server-private's
// as/include/base/proto.h. A regression here would be a wire-protocol
// break.
func TestTeranodeModifyOpWireBytes(t *testing.T) {
	payload := []byte{0x92, 0x04, 0x90} // msgpack: [4, []]  (sub_op_id=4, empty args)

	op := TeranodeModifyOp("blockIDs", payload)

	if op.opType.op != 200 {
		t.Fatalf("TeranodeModifyOp wire opcode = %d, want 200", op.opType.op)
	}
	if !op.opType.isWrite {
		t.Fatalf("TeranodeModifyOp isWrite = false, want true")
	}
	if op.binName != "blockIDs" {
		t.Fatalf("TeranodeModifyOp binName = %q, want %q", op.binName, "blockIDs")
	}

	bv, ok := op.binValue.(BytesValue)
	if !ok {
		t.Fatalf("TeranodeModifyOp binValue type = %T, want BytesValue", op.binValue)
	}
	if !bytes.Equal(bv, payload) {
		t.Fatalf("TeranodeModifyOp binValue = %x, want %x", bv, payload)
	}
}

// TestTeranodeReadOpWireBytes is the read-side counterpart.
func TestTeranodeReadOpWireBytes(t *testing.T) {
	op := TeranodeReadOp("anyBin", []byte{0xc0})

	if op.opType.op != 201 {
		t.Fatalf("TeranodeReadOp wire opcode = %d, want 201", op.opType.op)
	}
	if op.opType.isWrite {
		t.Fatalf("TeranodeReadOp isWrite = true, want false")
	}
}

// TestTeranodeReadOpRoutesAsRead confirms that an operate request
// containing TeranodeReadOp does NOT get classified as a write by
// newOperateArgs. Without an explicit case for _TERANODE_READ in the
// operation-type switch, the read op falls into the default branch,
// sets _INFO2_WRITE / hasWrite=true, and routes via PartitionForWrite
// — a real bug surfaced by the PR review on the initial commit.
func TestTeranodeReadOpRoutesAsRead(t *testing.T) {
	key, err := NewKey("ns", "set", "key")
	if err != nil {
		t.Fatalf("NewKey: %v", err)
	}

	args, oerr := newOperateArgs(
		nil, // cluster nil — the partition lookup is guarded
		NewWritePolicy(0, 0),
		key,
		[]*Operation{TeranodeReadOp("anyBin", []byte{0xc0})},
	)
	if oerr != nil {
		t.Fatalf("newOperateArgs: %v", oerr)
	}

	if args.hasWrite {
		t.Errorf("TeranodeReadOp got classified as a write (hasWrite=true)")
	}
	if args.readAttr&_INFO1_READ == 0 {
		t.Errorf("TeranodeReadOp didn't set _INFO1_READ (readAttr=0x%x)", args.readAttr)
	}
	if args.writeAttr&_INFO2_WRITE != 0 {
		t.Errorf("TeranodeReadOp wrongly set _INFO2_WRITE (writeAttr=0x%x)", args.writeAttr)
	}
}

// TestTeranodeModifyOpRoutesAsWrite is the symmetric check —
// TeranodeModifyOp must classify as a write so the request takes the
// PartitionForWrite path and the wire header has _INFO2_WRITE.
func TestTeranodeModifyOpRoutesAsWrite(t *testing.T) {
	key, err := NewKey("ns", "set", "key")
	if err != nil {
		t.Fatalf("NewKey: %v", err)
	}

	args, oerr := newOperateArgs(
		nil,
		NewWritePolicy(0, 0),
		key,
		[]*Operation{TeranodeModifyOp("anyBin", []byte{0x90})},
	)
	if oerr != nil {
		t.Fatalf("newOperateArgs: %v", oerr)
	}

	if !args.hasWrite {
		t.Errorf("TeranodeModifyOp didn't classify as a write (hasWrite=false)")
	}
	if args.writeAttr&_INFO2_WRITE == 0 {
		t.Errorf("TeranodeModifyOp didn't set _INFO2_WRITE (writeAttr=0x%x)", args.writeAttr)
	}
}

// TestTeranodeReadOpInMixedBatch confirms that when a batch contains
// both a TeranodeReadOp and a write op, the batch attr correctly sets
// _INFO1_READ on the read side. Without the fix, the read in the
// batch silently drops to the default (no-read) branch, so the wire
// header doesn't request bin data back.
func TestTeranodeReadOpInMixedBatch(t *testing.T) {
	ba := &batchAttr{}
	ba.adjustWrite([]*Operation{
		TeranodeModifyOp("modifyBin", []byte{0x90}),
		TeranodeReadOp("readBin", []byte{0xc0}),
	})

	if ba.readAttr&_INFO1_READ == 0 {
		t.Errorf("mixed batch with TeranodeReadOp didn't set _INFO1_READ "+
			"(readAttr=0x%x)", ba.readAttr)
	}
}

// TestTeranodeReadOpInBatchAttrNew exercises the matching switch in
// newBatchAttrOps (the constructor path used by BatchOperate).
func TestTeranodeReadOpInBatchAttrNew(t *testing.T) {
	ba := newBatchAttrOps(
		NewBatchPolicy(),
		NewBatchWritePolicy(),
		[]*Operation{TeranodeReadOp("readBin", []byte{0xc0})},
	)

	if ba.readAttr&_INFO1_READ == 0 {
		t.Errorf("newBatchAttrOps with TeranodeReadOp didn't set _INFO1_READ "+
			"(readAttr=0x%x)", ba.readAttr)
	}
}

// TestTeranodeOpEnumDistUnique guards against accidentally giving the
// new ops the same enumDist as an existing OperationType, which would
// break upstream's enum-distinction trick.
func TestTeranodeOpEnumDistUnique(t *testing.T) {
	used := map[byte]string{
		_READ.enumDist:        "_READ",
		_READ_HEADER.enumDist: "_READ_HEADER",
		_WRITE.enumDist:       "_WRITE",
		_CDT_READ.enumDist:    "_CDT_READ",
		_CDT_MODIFY.enumDist:  "_CDT_MODIFY",
		_MAP_READ.enumDist:    "_MAP_READ",
		_MAP_MODIFY.enumDist:  "_MAP_MODIFY",
		_ADD.enumDist:         "_ADD",
		_EXP_READ.enumDist:    "_EXP_READ",
		_EXP_MODIFY.enumDist:  "_EXP_MODIFY",
		_APPEND.enumDist:      "_APPEND",
		_PREPEND.enumDist:     "_PREPEND",
		_TOUCH.enumDist:       "_TOUCH",
		_BIT_READ.enumDist:    "_BIT_READ",
		_BIT_MODIFY.enumDist:  "_BIT_MODIFY",
		_DELETE.enumDist:      "_DELETE",
		_HLL_READ.enumDist:    "_HLL_READ",
		_HLL_MODIFY.enumDist:  "_HLL_MODIFY",
	}

	if name, conflict := used[_TERANODE_MODIFY.enumDist]; conflict {
		t.Fatalf("_TERANODE_MODIFY enumDist=%d collides with %s",
			_TERANODE_MODIFY.enumDist, name)
	}
	used[_TERANODE_MODIFY.enumDist] = "_TERANODE_MODIFY"

	if name, conflict := used[_TERANODE_READ.enumDist]; conflict {
		t.Fatalf("_TERANODE_READ enumDist=%d collides with %s",
			_TERANODE_READ.enumDist, name)
	}
}
