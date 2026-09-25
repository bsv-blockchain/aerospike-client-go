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
	"sync"
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

		done, err := tracker.isClusterComplete(clnt.Cluster(), &policy.BasePolicy)
		if !recordset.IsActive() || done || err != nil {
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
// Mirrors queryPartitionCommand.Execute in calling cmd.shouldRetry(err) on
// failure: shouldRetry has the side effect (partitionTracker.
// markRetrySequence / nodePartitions.partsUnavailable) of marking this
// command's partitions for retry on the next round. Without it, a node-level
// TIMEOUT/NETWORK_ERROR/SERVER_NOT_AVAILABLE/INDEX_NOTFOUND left
// partsUnavailable at 0, so isComplete saw no unavailable partitions and
// reported the round (and, with a PartitionFilter, the whole query via
// pf.Done) complete despite the affected partitions never having been read.
func (cmd *queryPartitionRawCommand) Execute() Error {
	var err Error
	if forced, ok := takeTestForceRawExecErr(); ok {
		// Test-only: see takeTestForceRawExecErr.
		err = forced
	} else {
		err = cmd.execute(cmd)
	}
	if err != nil {
		cmd.shouldRetry(err)
	}
	return err
}

// testForceRawExecErr, when armed via takeTestForceRawExecErr's setter,
// replaces the result of the very next (*queryPartitionRawCommand).Execute
// call across the whole process with the given error, without running
// cmd.execute(cmd) (i.e. without touching the network) -- consumed exactly
// once, then nil again. This lets tests verify Execute's tracker.shouldRetry
// bookkeeping deterministically, without a real network fault or relying on
// timing. Always nil (a single mutex-guarded pointer read) in production.
var (
	testForceRawExecMu  sync.Mutex
	testForceRawExecErr Error
)

func takeTestForceRawExecErr() (Error, bool) {
	testForceRawExecMu.Lock()
	defer testForceRawExecMu.Unlock()
	if testForceRawExecErr == nil {
		return nil, false
	}
	err := testForceRawExecErr
	testForceRawExecErr = nil
	return err, true
}

// armTestForceRawExecErr arms the one-shot override consumed by
// takeTestForceRawExecErr. Test-only.
func armTestForceRawExecErr(err Error) {
	testForceRawExecMu.Lock()
	defer testForceRawExecMu.Unlock()
	testForceRawExecErr = err
}

func (cmd *queryPartitionRawCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *queryPartitionRawCommand) getNamespace() *string {
	return &cmd.statement.Namespace
}
