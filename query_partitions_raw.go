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
	"context"
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
//
// If a command fails partway through a record (a network error, an abort
// from a sibling command's handler error, or ctx being done), BeginRecord
// and some number of Bin calls may already have happened for that record
// with neither EndRecord nor DiscardRecord following: an implementation that
// buffers per-record state in BeginRecord must be prepared to simply discard
// it if the query then returns an error, rather than assuming every
// BeginRecord is eventually paired with EndRecord or DiscardRecord.
type RawRecordHandler interface {
	// BeginRecord is called once for every record read from the server,
	// before any of its bins are delivered via Bin. digest is the record's
	// 20-byte key digest, only valid for the duration of the call.
	BeginRecord(digest []byte, generation, expiration uint32) error

	// Bin is called once for every bin of the current record, in the order
	// the server sends them. name and value are only valid for the duration
	// of the call.
	//
	// For a Statement with Operations set, an operation that reads the same
	// bin more than once makes the server return one result per operation,
	// not one per bin: Bin is called once per result, with the same name
	// repeated, in operation order. QueryPartitions' BinMap silently keeps
	// only the last such value (query commands never set the batch/operate
	// "isOperation" flag that would otherwise collect repeats into an
	// OpResults slice); a RawRecordHandler sees every one of them and must
	// decide for itself how to combine repeated names, if at all.
	Bin(name []byte, particleType int, value []byte) error

	// EndRecord is called once BeginRecord and all the Bin calls for the
	// current record have completed, when the record is accepted by the
	// partition tracker (i.e. it counts towards the query's MaxRecords, if
	// any).
	EndRecord() error

	// DiscardRecord is called instead of EndRecord when the partition
	// tracker rejects the record because the query's MaxRecords limit has
	// already been reached. A discarded record does not advance this
	// partition's resume position: it (or a record after it, depending on
	// server-side ordering) will be delivered again on a subsequent page.
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
// QueryPartitionsRaw blocks until the query completes.
//
// Return value: nil means every partition was successfully read, even if a
// node-level error (TIMEOUT, NETWORK_ERROR, SERVER_NOT_AVAILABLE,
// INDEX_NOTFOUND) along the way triggered a retry that then succeeded --
// matching the Java client. A non-nil error means either the query's
// retries were exhausted without reading every partition (e.g.
// MAX_RETRIES_EXCEEDED, a timeout, or a non-retryable node error), or the
// query was aborted by a RawRecordHandler method returning an error (that
// error, wrapped, recoverable via errors.Is/As) or by ctx being done (see
// QueryPartitionsRawContext).
//
// newHandler is called once per node command (i.e. concurrently, and
// possibly more than once per node across retries) to create the
// RawRecordHandler that will receive that command's records.
//
// This method is only supported by Aerospike 4.9+ servers.
// If the policy is nil, the default relevant policy will be used.
//
// QueryPartitionsRaw is a thin wrapper around QueryPartitionsRawContext using
// context.Background(): it cannot be aborted except by a RawRecordHandler
// error. Use QueryPartitionsRawContext to also support external
// cancellation/deadlines.
func (clnt *Client) QueryPartitionsRaw(policy *QueryPolicy, statement *Statement, partitionFilter *PartitionFilter, newHandler func() RawRecordHandler) Error {
	return clnt.QueryPartitionsRawContext(context.Background(), policy, statement, partitionFilter, newHandler)
}

// QueryPartitionsRawContext is QueryPartitionsRaw that also aborts the query
// when ctx is done, returning an Error that errors.Is ctx.Err(). Cancellation
// is delivered through the same path as a RawRecordHandler error (see
// Recordset.abort): node commands notice at their next record and stop
// promptly, but not necessarily instantly (up to the current record's
// processing, or the socket timeout if blocked on a read).
//
// An abort (handler error or ctx) never leaves partitionFilter.Done set on a
// partitionFilter passed in: partitions assigned to the round that was
// aborted are marked for retry instead, so a caller paginating with the same
// PartitionFilter (or simply retrying it) does not silently skip them.
//
// The goroutine watching ctx exits (and is fully joined) before this method
// returns, whether the query finished on its own or was cancelled: this
// never leaks a goroutine regardless of ctx's own lifetime.
func (clnt *Client) QueryPartitionsRawContext(ctx context.Context, policy *QueryPolicy, statement *Statement, partitionFilter *PartitionFilter, newHandler func() RawRecordHandler) Error {
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

	stopWatching := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-ctx.Done():
			recordset.abort(newCommonError(ctx.Err(), "QueryPartitionsRawContext: context done"))
		case <-stopWatching:
		}
	}()

	err := clnt.queryPartitionsRaw(policy, tracker, statement, recordset, newHandler)

	close(stopWatching)
	<-watcherDone

	return err
}

// queryPartitionsRawResult picks the Error that a queryPartitionsRaw call
// should return: a RawRecordHandler error recorded via Recordset.abort always
// takes priority over errs (the chainErrors-aggregated node/cluster errors
// from this call), regardless of the order in which concurrently aborting
// node commands happened to finish. See abortErr's docs on objectset
// (recordset.go) for why relying on chainErrors alone for this is
// order-dependent and can silently drop the handler's error from the chain.
func queryPartitionsRawResult(recordset *Recordset, errs Error) Error {
	if abortErr := recordset.firstAbortErr(); abortErr != nil {
		return abortErr
	}
	return errs
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
			return queryPartitionsRawResult(recordset, errs)
		}

		maxConcurrentNodes := policy.MaxConcurrentNodes
		if maxConcurrentNodes <= 0 {
			maxConcurrentNodes = len(list)
		}

		if recordset.IsActive() {
			weg := newWeightedErrGroup(maxConcurrentNodes)
			for _, nodePartition := range list {
				// With MaxConcurrentNodes < len(list), an abort (e.g. from a
				// RawRecordHandler error on an already-dispatched command)
				// should stop further dispatches from this round, not start
				// every remaining node command only to have each one
				// connect, read a record, and immediately terminate.
				if !recordset.IsActive() {
					break
				}
				cmd := newQueryPartitionRawCommand(policy, tracker, nodePartition, statement, recordset, newHandler())
				weg.execute(cmd)
			}
			errs = chainErrors(weg.wait(), errs)
		}

		if !recordset.IsActive() {
			// Aborted (RawRecordHandler error or ctx) before this round ever
			// dispatched a command (already-cancelled ctx racing this check),
			// or partway through it (the break above, or a command aborting
			// mid weg.wait()). Either way, this round did not run to actual
			// completion, so isClusterComplete must not get to revise
			// partition state as if it had: GetNodeQuery already cleared
			// Retry for every partition assigned this round the moment they
			// were assigned, and with partsUnavailable still 0 (an abort
			// doesn't set it), isComplete would otherwise set
			// PartitionFilter.Done = true / Retry = false despite those
			// partitions never having been read -- silently losing them for
			// any caller resuming from this filter.
			tracker.partitionError()
			if tracker.partitionFilter != nil {
				tracker.partitionFilter.Done = false
			}
			return queryPartitionsRawResult(recordset, errs)
		}

		done, err := tracker.isClusterComplete(clnt.Cluster(), &policy.BasePolicy)
		if done || err != nil {
			// errs, at this point, only ever holds non-retryable command
			// errors (see (*queryPartitionRawCommand).Execute: a retryable
			// error is swallowed there once shouldRetry has marked it for
			// retry) or isClusterComplete's own give-up error (e.g.
			// MAX_RETRIES_EXCEEDED, a timeout). A query that read every
			// partition, even after retrying a node error along the way,
			// therefore correctly returns nil here -- matching the Java
			// client -- rather than a stale error from an earlier,
			// successfully-recovered round.
			errs = chainErrors(err, errs)
			if errs != nil {
				tracker.partitionError()
			}
			return queryPartitionsRawResult(recordset, errs)
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
//
// Calls cmd.shouldRetry(err) on failure, like queryPartitionCommand.Execute:
// shouldRetry has the side effect (partitionTracker.markRetrySequence /
// nodePartitions.partsUnavailable) of marking this command's partitions for
// retry on the next round. Without it, a node-level TIMEOUT/NETWORK_ERROR/
// SERVER_NOT_AVAILABLE/INDEX_NOTFOUND left partsUnavailable at 0, so
// isComplete saw no unavailable partitions and reported the round (and, with
// a PartitionFilter, the whole query via pf.Done) complete despite the
// affected partitions never having been read.
//
// Unlike queryPartitionCommand.Execute, a retryable error (shouldRetry
// returns true) is swallowed here rather than returned: this matches the
// Java client, and means a query that reads every partition -- even after
// retrying a node error along the way -- reports success (a nil error from
// QueryPartitionsRaw/QueryPartitionsRawContext), not a stale error from an
// earlier, successfully-recovered round. A non-retryable error, or the
// tracker itself giving up (queryPartitionsRaw's own isClusterComplete
// error, e.g. MAX_RETRIES_EXCEEDED or a timeout), still surfaces.
func (cmd *queryPartitionRawCommand) Execute() Error {
	var err Error
	if rawExecOverride != nil {
		// Test-only: see rawExecOverride.
		err = rawExecOverride(cmd)
	} else {
		err = cmd.execute(cmd)
	}
	if err != nil && cmd.shouldRetry(err) {
		return nil
	}
	return err
}

// rawExecOverride, when non-nil, replaces cmd.execute(cmd)'s result for
// every (*queryPartitionRawCommand).Execute call, without touching the
// network. This lets tests verify Execute's tracker.shouldRetry bookkeeping
// deterministically, without a real network fault or relying on timing.
// Always nil (a single, uncontended function-pointer read) in production;
// set only from _test.go files, which must reset it via t.Cleanup so it
// cannot leak into an unrelated test.
var rawExecOverride func(cmd *queryPartitionRawCommand) Error

func (cmd *queryPartitionRawCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *queryPartitionRawCommand) getNamespace() *string {
	return &cmd.statement.Namespace
}
