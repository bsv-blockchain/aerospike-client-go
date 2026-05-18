// Copyright 2026 BSV Association.
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

// TeranodeModifyOp builds an operate-path operation that invokes a
// mod-teranode native function on the server. The payload must be a
// MessagePack-encoded array of two elements:
//
//	[ sub_op_id: int,  args: list ]
//
// where sub_op_id selects the mod-teranode function (1..13 — see the
// SUBOP_TABLE in modules/mod-teranode/src/main/mod_teranode_native_op.c
// in the server-private repo) and args is the argument list passed
// verbatim to that function. The wire opcode (200) sits in a vendor-
// reserved range above the upstream ceiling.
func TeranodeModifyOp(binName string, payload []byte) *Operation {
	return &Operation{
		opType:   _TERANODE_MODIFY,
		binName:  binName,
		binValue: NewBytesValue(payload),
	}
}

// TeranodeReadOp is the read-side counterpart of TeranodeModifyOp. The
// server currently has no read-side sub-ops defined; this constructor
// is provided for forward compatibility once any are added.
//
// binName must be non-empty. Unlike _READ, an empty binName does not
// imply "all bins" for a Teranode op — the server dispatches by
// sub_op_id, not by bin enumeration, and writes the result back to the
// named bin. batchAttr.adjustRead therefore deliberately does not set
// _INFO1_GET_ALL for an empty-binName TeranodeReadOp.
//
// TeranodeReadOp is not classified as a "basic read" by
// OperationType.isBasicRead(), so the client-side check in command.go
// rejects it inside secondary-index query projections when the target
// server is older than 8.1.2 ("Only basic read operations are supported
// for query operations projection..."). The BSV server fork post-dates
// 8.1.2, so this is not a constraint inside the Teranode dispatch path
// — flagged only for reuse outside it.
func TeranodeReadOp(binName string, payload []byte) *Operation {
	return &Operation{
		opType:   _TERANODE_READ,
		binName:  binName,
		binValue: NewBytesValue(payload),
	}
}
