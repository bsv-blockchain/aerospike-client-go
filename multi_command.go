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
	"fmt"
	"iter"
	"math/rand"
	"reflect"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	Buffer "github.com/bsv-blockchain/aerospike-client-go/v8/utils/buffer"
)

type baseMultiCommand struct {
	baseCommand

	rawCDT bool

	namespace string
	recordset *Recordset

	isOperation bool

	// Used in correct Scans/Queries
	tracker        *partitionTracker
	nodePartitions *nodePartitions

	// rawHandler, when set, causes parseRecordResults to deliver records
	// directly to it instead of going through a Recordset channel or the
	// reflection-based selectCases path. See QueryPartitionsRaw.
	rawHandler RawRecordHandler

	// rawNameBuf is scratch space used by the rawHandler path to hold the
	// current bin's name. It must be copied out of the connection's read
	// buffer before the particle bytes are read: bufferedConn.read may shift
	// unread bytes to the head of the (shared, reused) buffer in place,
	// which would otherwise silently corrupt a name slice captured earlier
	// and only used after that next read.
	rawNameBuf [255]byte

	// rawKey is a single Key reused across every record on the rawHandler
	// path, to avoid allocating a *Key per record. Only its digest field is
	// ever populated (see parseKeyDigest): the raw path has no use for
	// namespace/setName/userKey (RawRecordHandler.BeginRecord is handed the
	// digest directly, and partitionTracker.setLast/setDigest only read
	// Key.digest). Reusing it is safe only because those tracker methods
	// copy the digest bytes into their own storage instead of aliasing this
	// array (see PartitionStatus.setDigest) -- otherwise every partition's
	// resume digest but the last one written would be corrupted.
	rawKey Key

	terminationErrorType types.ResultCode

	resObjType     reflect.Type
	resObjMappings map[string][]int
	selectCases    []reflect.SelectCase

	bc bufferedConn
}

var multiObjectParser func(
	cmd *baseMultiCommand,
	obj reflect.Value,
	opCount int,
	fieldCount int,
	generation uint32,
	expiration uint32,
) Error

var prepareReflectionData func(cmd *baseMultiCommand)

func newMultiCommand(node *Node, recordset *Recordset, isOperation bool) *baseMultiCommand {
	cmd := &baseMultiCommand{
		baseCommand: baseCommand{
			node: node,
		},
		recordset:   recordset,
		isOperation: isOperation,
	}

	if prepareReflectionData != nil {
		prepareReflectionData(cmd)
	}
	return cmd
}

func newStreamingMultiCommand(node *Node, recordset *Recordset, namespace string, isOperation bool) *baseMultiCommand {
	cmd := &baseMultiCommand{
		baseCommand: baseCommand{
			node:    node,
			oneShot: true,
		},
		namespace:   namespace,
		recordset:   recordset,
		isOperation: isOperation,
	}

	if prepareReflectionData != nil {
		prepareReflectionData(cmd)
	}
	return cmd
}

func newCorrectStreamingMultiCommand(recordset *Recordset, namespace string) *baseMultiCommand {
	cmd := &baseMultiCommand{
		baseCommand: baseCommand{
			oneShot: true,
		},
		namespace: namespace,
		recordset: recordset,
	}

	if prepareReflectionData != nil {
		prepareReflectionData(cmd)
	}
	return cmd
}

func (cmd *baseMultiCommand) getNode(ifc command) (*Node, Error) {
	return cmd.node, nil
}

func (cmd *baseMultiCommand) prepareRetry(ifc command, isTimeout bool) bool {
	return false
}

func (cmd *baseMultiCommand) getConnection(policy Policy) (*Connection, Error) {
	return cmd.node.getConnectionWithHint(policy.GetBasePolicy().TotalTimeout, policy.GetBasePolicy().SocketTimeout, byte(rand.Int63()&0xff), policy.GetBasePolicy().TimeoutDelay)
}

func (cmd *baseMultiCommand) putConnection(conn *Connection) {
	cmd.node.putConnectionWithHint(conn, byte(rand.Int63()&0xff))
}

func (cmd *baseMultiCommand) parseResult(ifc command, conn *Connection) Error {
	// Read socket into receive buffer one record at a time.  Do not read entire receive size
	// because the receive buffer would be too big.
	status := true

	var err Error

	cmd.bc = newBufferedConn(conn, 0)
	for status {
		if err = cmd.conn.initInflater(false, 0); err != nil {
			return newError(types.PARSE_ERROR, "Error setting up zlib inflater:", err.Error()).setNode(cmd.node)
		}
		cmd.bc.reset(8)

		// Read header.
		if cmd.dataBuffer, err = cmd.bc.read(8); err != nil {
			return err
		}

		proto := Buffer.BytesToInt64(cmd.dataBuffer, 0)
		receiveSize := int(proto & 0xFFFFFFFFFFFF)
		if receiveSize <= 0 {
			continue
		}

		if compressedSize := cmd.compressedSize(); compressedSize > 0 {
			cmd.bc.reset(8)
			// Read header.
			if cmd.dataBuffer, err = cmd.bc.read(8); err != nil {
				return err
			}

			receiveSize = int(Buffer.BytesToInt64(cmd.dataBuffer, 0)) - 8
			if err = cmd.conn.initInflater(true, compressedSize-8); err != nil {
				return newError(types.PARSE_ERROR, fmt.Sprintf("Error setting up zlib inflater for size `%d`: %s", compressedSize-8, err.Error())).setNode(cmd.node)
			}

			// getting compressed received size
			cmd.receiveSize = int64(receiveSize)

			// read the first 8 bytes
			cmd.bc.reset(8)
			if cmd.dataBuffer, err = cmd.bc.read(8); err != nil {
				return err
			}
		} else {
			// getting un-compressed received size
			cmd.receiveSize = int64(receiveSize)
		}

		// Validate header to make sure we are at the beginning of a message
		proto = Buffer.BytesToInt64(cmd.dataBuffer, 0)
		if err = cmd.validateHeader(proto); err != nil {
			return err
		}

		if receiveSize > 0 {
			cmd.bc.reset(receiveSize)

			status, err = ifc.parseRecordResults(ifc, receiveSize)
			if err != nil {
				cmd.bc.drainConn()
				return err
			}
		} else {
			status = false
		}
	}

	// if the buffer has been resized, put it back so that it will be reassigned to the connection.
	cmd.dataBuffer = cmd.bc.buf()

	return nil
}

func (cmd *baseMultiCommand) parseKey(fieldCount int, bval *int64) (*Key, Error) {
	var digest [20]byte
	var namespace, setName string
	var userKey Value
	var err Error

	for i := 0; i < fieldCount; i++ {
		if err = cmd.readBytes(4); err != nil {
			return nil, err
		}

		fieldlen := int(Buffer.BytesToUint32(cmd.dataBuffer, 0))
		if err = cmd.readBytes(fieldlen); err != nil {
			return nil, err
		}

		fieldtype := FieldType(cmd.dataBuffer[0])
		size := fieldlen - 1

		switch fieldtype {
		case DIGEST_RIPE:
			copy(digest[:], cmd.dataBuffer[1:size+1])
		case NAMESPACE:
			namespace = string(cmd.dataBuffer[1 : size+1])
		case TABLE:
			setName = string(cmd.dataBuffer[1 : size+1])
		case KEY:
			if userKey, err = bytesToKeyValue(int(cmd.dataBuffer[1]), cmd.dataBuffer, 2, size-1); err != nil {
				return nil, err.setNode(cmd.node)
			}
		case BVAL_ARRAY:
			if bval != nil {
				v := Buffer.LittleBytesToInt64(cmd.dataBuffer, 1)
				*bval = v
			}
		}
	}

	return &Key{namespace: namespace, setName: setName, digest: digest, userKey: userKey}, nil
}

// parseKeyDigest is the RawRecordHandler counterpart of parseKey: it reads
// exactly the same fields off the wire (so the connection stays in sync),
// but only extracts the digest and bval, skipping the namespace/setName
// string allocations and the user-key value decode. This is safe because
// the raw record path hands the digest to the caller directly (it never
// constructs a *Key for its consumer), and partitionTracker.setLast/
// setDigest -- the only other digest readers -- don't touch namespace,
// setName or the user key either.
func (cmd *baseMultiCommand) parseKeyDigest(fieldCount int, digest *[20]byte, bval *int64) Error {
	// digest is reused across records (baseMultiCommand.rawKey); clear it so
	// a record with no (or a short) DIGEST_RIPE field can't leak the
	// previous record's digest instead of reading as all-zero, matching
	// parseKey's fresh `var digest [20]byte` per call.
	*digest = [20]byte{}

	for i := 0; i < fieldCount; i++ {
		if err := cmd.readBytes(4); err != nil {
			return err
		}

		fieldlen := int(Buffer.BytesToUint32(cmd.dataBuffer, 0))
		if err := cmd.readBytes(fieldlen); err != nil {
			return err
		}

		fieldtype := FieldType(cmd.dataBuffer[0])
		size := fieldlen - 1

		switch fieldtype {
		case DIGEST_RIPE:
			copy(digest[:], cmd.dataBuffer[1:size+1])
		case BVAL_ARRAY:
			if bval != nil {
				*bval = Buffer.LittleBytesToInt64(cmd.dataBuffer, 1)
			}
		}
	}

	return nil
}

// abortRawHandlerErr records a RawRecordHandler failure on the recordset
// (broadcasting the abort to every concurrently running node command) and
// returns the error this command itself should return.
//
// It deliberately builds two independent *AerospikeError instances -- one
// stored via recordset.abort, one returned here -- rather than storing and
// returning the same object. baseCommand.executeAt mutates whatever error a
// command returns in place (chainErrors(err, nil) returns err itself, then
// .iter()/.setNode()/.setInDoubt() write fields on it), and the stored
// instance is read by every other concurrently running node command via
// queryPartitionsRawResult/firstAbortErr. Returning the same instance a
// sibling could be executeAt-mutating at the same time is a data race.
func (cmd *baseMultiCommand) abortRawHandlerErr(err error, context string) Error {
	cmd.recordset.abort(newNodeError(cmd.node, newCommonError(err, context)))
	return newNodeError(cmd.node, newCommonError(err, context))
}

func (cmd *baseMultiCommand) parseVersion(fieldCount int) (*uint64, Error) {
	var version *uint64

	for i := 0; i < fieldCount; i++ {
		if err := cmd.readBytes(4); err != nil {
			return nil, err
		}

		fieldlen := int(Buffer.BytesToUint32(cmd.dataBuffer, 0))
		if err := cmd.readBytes(fieldlen); err != nil {
			return nil, err
		}

		fieldType := FieldType(cmd.dataBuffer[0])
		size := fieldlen - 1

		if fieldType == RECORD_VERSION && size == 7 {
			version = Buffer.VersionBytesToUint64(cmd.dataBuffer, 1)
		}
	}
	return version, nil
}

func (cmd *baseMultiCommand) parseFieldsRead(fieldCount int, key *Key) (err Error) {
	if cmd.txn != nil {
		version, err := cmd.parseVersion(fieldCount)
		if err != nil {
			return err
		}
		cmd.txn.OnRead(key, version)
		return nil
	} else {
		return cmd.skipKey(fieldCount)
	}
}

func (cmd *baseMultiCommand) parseFieldsBatch(resultCode types.ResultCode, fieldCount int, br BatchRecordIfc) (err Error) {
	if cmd.txn != nil {
		version, err := cmd.parseVersion(fieldCount)
		if err != nil {
			return err
		}

		if br.BatchRec().hasWrite {
			cmd.txn.OnWrite(br.BatchRec().Key, version, resultCode)
		} else {
			cmd.txn.OnRead(br.BatchRec().Key, version)
		}
		return nil
	} else {
		return cmd.skipKey(fieldCount)
	}
}

func (cmd *baseMultiCommand) parseFieldsWrite(resultCode types.ResultCode, fieldCount int, key *Key) (err Error) {
	if cmd.txn != nil {
		version, err := cmd.parseVersion(fieldCount)
		if err != nil {
			return err
		}

		cmd.txn.OnWrite(key, version, resultCode)
		return nil
	}
	return cmd.skipKey(fieldCount)
}

func (cmd *baseMultiCommand) skipKey(fieldCount int) (err Error) {
	for i := 0; i < fieldCount; i++ {
		if err = cmd.readBytes(4); err != nil {
			return err
		}

		fieldlen := int(Buffer.BytesToUint32(cmd.dataBuffer, 0))
		if err = cmd.readBytes(fieldlen); err != nil {
			return err
		}
	}

	return nil
}

func (cmd *baseMultiCommand) readBytes(length int) (err Error) {
	// Corrupted data streams can result in a huge length.
	// Do a sanity check here.
	if length > MaxBufferSize || length < 0 {
		return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid readBytes length: %d", length)).setNode(cmd.node)
	}

	cmd.dataBuffer, err = cmd.bc.read(length)
	if err != nil {
		return err
	}
	cmd.dataOffset += length

	return nil
}

func (cmd *baseMultiCommand) parseRecordResults(ifc command, receiveSize int) (bool, Error) {
	// Read/parse remaining message bytes one record at a time.
	cmd.dataOffset = 0

	for cmd.dataOffset < receiveSize {
		if err := cmd.readBytes(int(_MSG_REMAINING_HEADER_SIZE)); err != nil {
			err = newNodeError(cmd.node, err)
			return false, err
		}
		resultCode := types.ResultCode(cmd.dataBuffer[5] & 0xFF)

		// Aggregate metrics
		metricsEnabled := cmd.node.cluster.metricsEnabled
		if metricsEnabled {
			cmd.node.stats.updateOrInsert(cmd.getNamespace(), cmd.getNamespaces(), ifc.commandType(), resultCode)
		}

		if resultCode != 0 && resultCode != types.PARTITION_UNAVAILABLE {
			if resultCode == types.KEY_NOT_FOUND_ERROR || resultCode == types.FILTERED_OUT {
				return false, nil
			}
			err := newError(resultCode)
			err = newNodeError(cmd.node, err)
			return false, err
		}

		info3 := int(cmd.dataBuffer[3])

		// If cmd is the end marker of the response, do not proceed further
		if (info3 & _INFO3_LAST) == _INFO3_LAST {
			return false, nil
		}

		generation := Buffer.BytesToUint32(cmd.dataBuffer, 6)
		expiration := types.TTL(Buffer.BytesToUint32(cmd.dataBuffer, 10))
		fieldCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 18))
		opCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 20))

		// rawBval is a plain stack variable: parseKeyDigest and setLast both
		// provably don't retain its address, so it never allocates. bval,
		// used by the traditional (non-raw) path below, does need to be
		// heap-allocated: it's sent on a channel inside a *Result. It's
		// declared as a *int64, explicitly allocated with new() only inside
		// the non-raw branch, rather than as a plain `var bval int64` that
		// Go's escape analysis would force onto the heap on every single
		// call to this function -- including raw-path calls, where that
		// branch never runs -- since escape decisions are per-declaration,
		// not per-branch.
		var rawBval int64
		var bval *int64
		var key *Key
		var err Error
		var bvalPtr *int64
		if cmd.rawHandler != nil {
			// Reuse a single per-command Key (just its digest) instead of
			// allocating a fresh *Key for every record.
			err = cmd.parseKeyDigest(fieldCount, &cmd.rawKey.digest, &rawBval)
			key = &cmd.rawKey
			bvalPtr = &rawBval
		} else {
			bval = new(int64)
			key, err = cmd.parseKey(fieldCount, bval)
			bvalPtr = bval
		}
		if err != nil {
			err = newNodeError(cmd.node, err)
			return false, err
		}

		// Partition is done, don't go further
		if (info3 & _INFO3_PARTITION_DONE) != 0 {
			// When an error code is received, mark partition as unavailable
			// for the current round. Unavailable partitions will be retried
			// in the next round. Generation is overloaded as partitionId.
			if resultCode != 0 && cmd.tracker != nil {
				cmd.tracker.partitionUnavailable(cmd.nodePartitions, int(generation))
			}
			continue
		}

		// if there is a recordset, process the record traditionally
		// otherwise, it is supposed to be a record channel
		if cmd.rawHandler != nil {
			// Non-blocking check: a sibling node command (or this one, on a
			// previous record) may have aborted the whole query already.
			select {
			case <-cmd.recordset.cancelled:
				// Each of these builds a fresh *AerospikeError (constAerospikeError.err()
				// / newError() copy their receiver), never the instance stored on
				// recordset.abortErr: queryPartitionsRawResult already gives that stored
				// handler/ctx error priority over whatever any individual sibling command
				// returns, so returning it here too would just be handing the SAME
				// *AerospikeError instance back from multiple concurrently-executing
				// commands -- executeAt mutates its returned error in place (iter/setNode/
				// setInDoubt), which is a real data race across those siblings.
				switch cmd.terminationErrorType {
				case types.SCAN_TERMINATED:
					return false, ErrScanTerminated.err().setNode(cmd.node)
				case types.QUERY_TERMINATED:
					return false, ErrQueryTerminated.err().setNode(cmd.node)
				default:
					return false, newError(cmd.terminationErrorType).setNode(cmd.node)
				}
			default:
			}

			if err := cmd.rawHandler.BeginRecord(key.digest[:], generation, expiration); err != nil {
				return false, cmd.abortRawHandlerErr(err, "RawRecordHandler.BeginRecord failed")
			}

			for i := 0; i < opCount; i++ {
				if err = cmd.readBytes(8); err != nil {
					return false, newNodeError(cmd.node, err)
				}

				opSize := int(Buffer.BytesToUint32(cmd.dataBuffer, 0))
				particleType := int(cmd.dataBuffer[5])
				nameSize := int(cmd.dataBuffer[7])

				if err = cmd.readBytes(nameSize); err != nil {
					return false, newNodeError(cmd.node, err)
				}
				// Copy the name out now: the next readBytes call below may
				// shift the shared connection buffer in place, invalidating
				// any slice of it captured before the shift.
				copy(cmd.rawNameBuf[:nameSize], cmd.dataBuffer[:nameSize])

				particleBytesSize := opSize - (4 + nameSize)
				if err = cmd.readBytes(particleBytesSize); err != nil {
					return false, newNodeError(cmd.node, err)
				}

				if err := cmd.rawHandler.Bin(cmd.rawNameBuf[:nameSize], particleType, cmd.dataBuffer[:particleBytesSize]); err != nil {
					return false, cmd.abortRawHandlerErr(err, "RawRecordHandler.Bin failed")
				}
			}

			if cmd.tracker.allowRecord(cmd.nodePartitions) {
				if err := cmd.rawHandler.EndRecord(); err != nil {
					return false, cmd.abortRawHandlerErr(err, "RawRecordHandler.EndRecord failed")
				}
			} else {
				cmd.rawHandler.DiscardRecord()
				// Matches the other branches' `if !allowRecord { continue }`:
				// a discarded record must not advance this partition's
				// resume digest/bval or count towards nodePartitions'
				// recordCount, or the next page would resume past a record
				// the handler never actually saw.
				continue
			}
		} else if cmd.selectCases == nil {
			// Parse bins.
			var bins BinMap

			for i := 0; i < opCount; i++ {
				if err = cmd.readBytes(8); err != nil {
					return false, newNodeError(cmd.node, err)
				}

				opSize := int(Buffer.BytesToUint32(cmd.dataBuffer, 0))
				particleType := int(cmd.dataBuffer[5])
				nameSize := int(cmd.dataBuffer[7])

				if err = cmd.readBytes(nameSize); err != nil {
					return false, newNodeError(cmd.node, err)
				}
				name := string(cmd.dataBuffer[:nameSize])

				particleBytesSize := opSize - (4 + nameSize)
				if err = cmd.readBytes(particleBytesSize); err != nil {
					return false, newNodeError(cmd.node, err)
				}
				value, err := bytesToParticleRaw(particleType, cmd.dataBuffer, 0, particleBytesSize, cmd.rawCDT)
				if err != nil {
					return false, newNodeError(cmd.node, err)
				}

				if bins == nil {
					bins = make(BinMap, opCount)
				}

				if cmd.isOperation {
					if prev, ok := bins[name]; ok {
						if prev2, ok := prev.(OpResults); ok {
							bins[name] = append(prev2, value)
						} else {
							bins[name] = OpResults{prev, value}
						}
					} else {
						bins[name] = value
					}
				} else {
					bins[name] = value
				}
			}

			if !cmd.tracker.allowRecord(cmd.nodePartitions) {
				continue
			}

			// If the channel is full and it blocks, we don't want this command to
			// block forever, or panic in case the channel is closed in the meantime.
			select {
			// send back the result on the async channel
			case cmd.recordset.records <- &Result{Record: newRecord(cmd.node, key, bins, generation, expiration), Err: nil, BVal: bval}:
			case <-cmd.recordset.cancelled:
				switch cmd.terminationErrorType {
				case types.SCAN_TERMINATED:
					return false, ErrScanTerminated.err().setNode(cmd.node)
				case types.QUERY_TERMINATED:
					return false, ErrQueryTerminated.err().setNode(cmd.node)
				default:
					return false, newError(cmd.terminationErrorType).setNode(cmd.node)
				}
			}
		} else if multiObjectParser != nil {
			obj := reflect.New(cmd.resObjType)
			if err := multiObjectParser(cmd, obj, opCount, fieldCount, generation, expiration); err != nil {
				err = newNodeError(cmd.node, err)
				return false, err
			}

			if !cmd.tracker.allowRecord(cmd.nodePartitions) {
				continue
			}

			// set the object to send
			cmd.selectCases[0].Send = obj

			chosen, _, _ := reflect.Select(cmd.selectCases)
			switch chosen {
			case 0: // object sent
			case 1: // cancel channel is closed
				return false, newError(cmd.terminationErrorType).setNode(cmd.node)
			}
		}

		if fieldCount > 0 && cmd.tracker != nil {
			// only if there is key returned. Commands like QueryAggregate do not return keys
			if cmd.terminationErrorType == types.SCAN_TERMINATED {
				cmd.tracker.setDigest(cmd.nodePartitions, key)
			} else {
				cmd.tracker.setLast(cmd.nodePartitions, key, bvalPtr)
			}
		}
	}

	return true, nil
}

func (cmd *baseMultiCommand) canPutConnBack() bool {
	return false
}

func (cmd *baseMultiCommand) execute(ifc command) Error {

	/***************************************************************************
	IMPORTANT: 	No need to send the error here to the recordset.Error channel.
				It is being sent from the downstream command from the result
				returned from the function.
	****************************************************************************/

	return cmd.baseCommand.execute(ifc)
}

func (cmd *baseMultiCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *baseMultiCommand) getNamespace() *string {
	return &cmd.namespace
}

func (cmd *baseMultiCommand) salvageConn(timeoutDelay time.Duration, conn *Connection, node *Node) {
	conn.Close()
}
