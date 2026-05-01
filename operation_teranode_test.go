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
