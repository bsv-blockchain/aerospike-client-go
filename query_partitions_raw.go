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
	"iter"
	"time"

	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
)

// RawRecordHandler receives the records of a partition query inline, in the
// goroutine of the node command that read them off the wire. One handler
// instance is created (via the factory passed to QueryPartitionsRaw) per node
// command, so a handler implementation need not be goroutine-safe itself,
// although different handler instances created by the same factory will run
// concurrently with each other.
//
// All byte slices passed to Bin are only valid for the duration of the call;
// implementations that need to retain the data must copy it.
//
// Returning a non-nil error from any method aborts the whole query (across
// all nodes) as promptly as possible. The first such error is returned from
// QueryPartitionsRaw, wrapped in an Error but recoverable via errors.Is/As.
type RawRecordHandler interface {
	// BeginRecord is called once for every record read from the server,
	// before any of its bins are delivered via Bin. digest is the record's
	// 20-byte key digest, only valid for the duration of the call.
	BeginRecord(digest []byte, generation, expiration uint32) error

	// Bin is called once for every bin of the current record. name and value
	// are only valid for the duration of the call.
	Bin(name []byte, particleType int, value []byte) error

	// EndRecord is called once BeginRecord and all the Bin calls for the
	// current record have completed, when the record is accepted by the
	// partition tracker (i.e. it counts towards the query's MaxRecords, if
	// any).
	EndRecord() error

	// DiscardRecord is called instead of EndRecord when the partition
	// tracker rejects the record, e.g. because its partition was already
	// completed in a previous round, or because MaxRecords was reached.
	DiscardRecord()
}

// DecodeParticle decodes a raw bin value as delivered to a RawRecordHandler's
// Bin method (particleType, value) into the same Go value that
// QueryPartitions/Query would have put in the corresponding Record.Bins
// entry. It is a convenience for RawRecordHandler implementations that want
// to decode occasional complex bins (maps, lists, etc.) without
// reimplementing the wire's msgpack format themselves; simple bins (int,
// string, bool, blob, float) can just as easily be decoded inline by the
// handler.
//
// value must be a slice previously passed to Bin, or a copy of one; it is
// not retained.
//
// DecodeParticle always decodes MAP/LIST bins (matching the default
// QueryPolicy.RawCDT == false behavior). A query run with RawCDT == true
// returns MAP/LIST bins undecoded, as a RawBlobValue, in QueryPartitions'
// own BinMap; DecodeParticle does not reproduce that -- it always decodes.
func DecodeParticle(particleType int, value []byte) (any, Error) {
	return bytesToParticle(particleType, value, 0, len(value))
}

// QueryPartitionsRaw executes a query for the specified partitions (or all
// partitions, if partitionFilter is nil), delivering every record inline to a
// RawRecordHandler created by newHandler, in the goroutine of the node
// command that read it. Unlike QueryPartitions, there is no record channel:
// QueryPartitionsRaw blocks until the query completes and returns nil, or
// returns the first error encountered (either a query/network error, or an
// error returned by the handler).
//
// newHandler is called once per node command (i.e. concurrently, and
// possibly more than once per node across retries) to create the
// RawRecordHandler that will receive that command's records.
//
// This method is only supported by Aerospike 4.9+ servers.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) QueryPartitionsRaw(policy *QueryPolicy, statement *Statement, partitionFilter *PartitionFilter, newHandler func() RawRecordHandler) Error {
	policy = clnt.getUsableQueryPolicy(policy)

	if newHandler == nil {
		return newError(types.PARAMETER_ERROR, "newHandler must not be nil")
	}

	nodes := clnt.cluster.GetNodes()
	if len(nodes) == 0 {
		return ErrClusterIsEmpty.err()
	}

	var tracker *partitionTracker
	if partitionFilter == nil {
		tracker = newPartitionTrackerForNodes(&policy.MultiPolicy, nodes)
	} else {
		tracker = newPartitionTracker(&policy.MultiPolicy, partitionFilter, nodes)
	}

	// The records channel is never used by the raw path (see
	// queryPartitionRawCommand.Execute/parseRecordResults' rawHandler
	// branch): records are delivered inline to the handler instead. A real
	// (if unbuffered and unused) Recordset is still needed for the
	// cancellation/completion bookkeeping shared with the regular query/scan
	// executors, and because baseMultiCommand's reflection setup always
	// dereferences recordset.objChan.
	recordset := newRecordset(0, 1)

	return clnt.queryPartitionsRaw(policy, tracker, statement, recordset, newHandler)
}

// queryPartitionsRaw mirrors queryPartitions (query_executor.go): same
// partition tracker, same retry/backoff loop, same weighted concurrency via
// werrGroup. The only difference is that node commands deliver records
// directly to a RawRecordHandler instead of a Recordset channel, and errors
// are returned directly instead of being routed through recordset.sendError.
func (clnt *Client) queryPartitionsRaw(policy *QueryPolicy, tracker *partitionTracker, statement *Statement, recordset *Recordset, newHandler func() RawRecordHandler) Error {
	defer recordset.signalEnd()

	// for exponential backoff
	interval := policy.SleepBetweenRetries

	var errs Error
	for {
		list, err := tracker.assignPartitionsToNodes(clnt.Cluster(), statement.Namespace)
		if err != nil {
			errs = chainErrors(err, errs)
			tracker.partitionError()
			return errs
		}

		maxConcurrentNodes := policy.MaxConcurrentNodes
		if maxConcurrentNodes <= 0 {
			maxConcurrentNodes = len(list)
		}

		if recordset.IsActive() {
			weg := newWeightedErrGroup(maxConcurrentNodes)
			for _, nodePartition := range list {
				cmd := newQueryPartitionRawCommand(policy, tracker, nodePartition, statement, recordset, newHandler())
				weg.execute(cmd)
			}
			errs = chainErrors(weg.wait(), errs)
		}

		done, err := tracker.isClusterComplete(clnt.Cluster(), &policy.BasePolicy)
		if !recordset.IsActive() || done || err != nil {
			errs = chainErrors(err, errs)
			if errs != nil {
				tracker.partitionError()
			}
			return errs
		}

		if policy.SleepBetweenRetries > 0 {
			// Sleep before trying again.
			time.Sleep(interval)

			if policy.SleepMultiplier > 1 {
				interval = time.Duration(float64(interval) * policy.SleepMultiplier)
			}
		}

		recordset.resetTaskID()
	}
}

// queryPartitionRawCommand is the RawRecordHandler-delivering counterpart of
// queryPartitionCommand.
type queryPartitionRawCommand queryCommand

func newQueryPartitionRawCommand(
	policy *QueryPolicy,
	tracker *partitionTracker,
	nodePartitions *nodePartitions,
	statement *Statement,
	recordset *Recordset,
	handler RawRecordHandler,
) *queryPartitionRawCommand {
	cmd := &queryPartitionRawCommand{
		baseMultiCommand: *newCorrectStreamingMultiCommand(recordset, statement.Namespace),
		policy:           policy,
		writePolicy:      nil,
		statement:        statement,
		operations:       statement.Operations,
	}
	cmd.rawCDT = policy.RawCDT
	cmd.terminationErrorType = statement.terminationError()
	cmd.tracker = tracker
	cmd.nodePartitions = nodePartitions
	cmd.node = nodePartitions.node
	cmd.rawHandler = handler

	return cmd
}

func (cmd *queryPartitionRawCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *queryPartitionRawCommand) writeBuffer(ifc command) Error {
	return cmd.setQuery(cmd.policy, cmd.writePolicy, cmd.statement, cmd.recordset.TaskId(), cmd.operations, cmd.writePolicy != nil, cmd.nodePartitions)
}

func (cmd *queryPartitionRawCommand) shouldRetry(e Error) bool {
	return cmd.tracker != nil && cmd.tracker.shouldRetry(cmd.nodePartitions, e)
}

func (cmd *queryPartitionRawCommand) commandType() commandType {
	return ttQuery
}

// Execute runs the command. Unlike queryPartitionCommand.Execute, it never
// routes the error through recordset.sendError: nobody drains that channel
// for a raw query (there is no consumer goroutine), so doing so could block
// forever. The error is instead returned directly and collected by the
// werrGroup in queryPartitionsRaw.
func (cmd *queryPartitionRawCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *queryPartitionRawCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *queryPartitionRawCommand) getNamespace() *string {
	return &cmd.statement.Namespace
}
