// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package leveldb

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sorter"
	"github.com/pingcap/ticdc/cdc/sorter/encoding"
	"github.com/pingcap/ticdc/cdc/sorter/leveldb/message"
	"github.com/pingcap/ticdc/pkg/actor"
	actormsg "github.com/pingcap/ticdc/pkg/actor/message"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/db"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	// Capacity of leveldb sorter input and output channels.
	sorterInputCap, sorterOutputCap = 64, 64
	// Max size of received event batch.
	batchReceiveEventSize = 32
)

var levelDBSorterIDAlloc uint32 = 0

func allocID() uint32 {
	return atomic.AddUint32(&levelDBSorterIDAlloc, 1)
}

// Sorter accepts out-of-order raw kv entries and output sorted entries
type Sorter struct {
	actorID actor.ID
	router  *actor.Router
	uid     uint32
	tableID uint64
	serde   *encoding.MsgPackGenSerde

	iterAliveDuration time.Duration

	lastSentResolvedTs uint64
	lastEvent          *model.PolymorphicEvent

	inputCh  chan *model.PolymorphicEvent
	outputCh chan *model.PolymorphicEvent

	closed int32

	metricTotalEventsKV         prometheus.Counter
	metricTotalEventsResolvedTs prometheus.Counter
	metricIterReadDuration      prometheus.ObserverVec
}

// NewLevelDBSorter creates a new LevelDBSorter
func NewLevelDBSorter(
	ctx context.Context, tableID int64, startTs uint64,
	router *actor.Router, actorID actor.ID, cfg *config.DBConfig,
) *Sorter {
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	return &Sorter{
		actorID:            actorID,
		router:             router,
		uid:                allocID(),
		tableID:            uint64(tableID),
		lastSentResolvedTs: startTs,
		serde:              &encoding.MsgPackGenSerde{},
		iterAliveDuration:  time.Duration(cfg.IteratorMaxAliveDuration) * time.Millisecond,

		inputCh:  make(chan *model.PolymorphicEvent, sorterInputCap),
		outputCh: make(chan *model.PolymorphicEvent, sorterOutputCap),

		metricTotalEventsKV:         sorter.EventCount.WithLabelValues(captureAddr, changefeedID, "kv"),
		metricTotalEventsResolvedTs: sorter.EventCount.WithLabelValues(captureAddr, changefeedID, "resolved"),
		metricIterReadDuration:      sorterIterReadDurationHistogram.MustCurryWith(prometheus.Labels{"capture": captureAddr, "id": changefeedID}),
	}
}

func (ls *Sorter) waitInput(ctx context.Context) (*model.PolymorphicEvent, error) {
	select {
	case <-ctx.Done():
		return nil, errors.Trace(ctx.Err())
	case ev := <-ls.inputCh:
		return ev, nil
	}
}

func (ls *Sorter) waitInputOutput(
	ctx context.Context,
) (*model.PolymorphicEvent, error) {
	// A dummy event for detecting whether output is available.
	dummyEvent := model.NewResolvedPolymorphicEvent(0, 0)
	select {
	// Prefer receiving input events.
	case ev := <-ls.inputCh:
		return ev, nil
	default:
		select {
		case <-ctx.Done():
			return nil, errors.Trace(ctx.Err())
		case ev := <-ls.inputCh:
			return ev, nil
		case ls.outputCh <- dummyEvent:
			return nil, nil
		}
	}
}

// wait input or output becomes available.
// It returns
//   1) the max commit ts of received new events,
//   2) the max resolved ts of new resolvedTs events,
//   3) number of received new events,
//   4) error.
//
// If input is available, it batches newly received events.
// If output available, it sends a dummy resolved ts event and returns.
func (ls *Sorter) wait(
	ctx context.Context, waitOutput bool, events []*model.PolymorphicEvent,
) (uint64, uint64, int, error) {
	batchSize := len(events)
	if batchSize <= 0 {
		log.Panic("batch size must be larger than 0")
	}
	maxCommitTs, maxResolvedTs := uint64(0), uint64(0)
	inputCount, kvEventCount, resolvedEventCount := 0, 0, 0
	appendInputEvent := func(ev *model.PolymorphicEvent) {
		if ls.lastSentResolvedTs != 0 && ev.CRTs < ls.lastSentResolvedTs {
			log.Warn("commit ts < resolved ts, drop",
				zap.Uint64("lastSentResolvedTs", ls.lastSentResolvedTs),
				zap.Any("event", ev), zap.Uint64("regionID", ev.RegionID()))
			return
		}
		if ev.RawKV.OpType == model.OpTypeResolved {
			if maxResolvedTs < ev.CRTs {
				maxResolvedTs = ev.CRTs
			}
			resolvedEventCount++
		} else {
			if maxCommitTs < ev.CRTs {
				maxCommitTs = ev.CRTs
			}
			events[inputCount] = ev
			inputCount++
			kvEventCount++
		}
	}

	if waitOutput {
		// Wait intput and output.
		ev, err := ls.waitInputOutput(ctx)
		if err != nil {
			atomic.StoreInt32(&ls.closed, 1)
			close(ls.outputCh)
			return 0, 0, 0, errors.Trace(ctx.Err())
		}
		if ev == nil {
			// No input event and output is available.
			return maxCommitTs, maxResolvedTs, 0, nil
		}
		appendInputEvent(ev)
	} else {
		// Wait input only.
		ev, err := ls.waitInput(ctx)
		if err != nil {
			atomic.StoreInt32(&ls.closed, 1)
			close(ls.outputCh)
			return 0, 0, 0, errors.Trace(ctx.Err())
		}
		appendInputEvent(ev)
	}

	// Batch receive events
BATCH:
	for inputCount < batchSize {
		select {
		case ev := <-ls.inputCh:
			appendInputEvent(ev)
		default:
			break BATCH
		}
	}
	ls.metricTotalEventsKV.Add(float64(kvEventCount))
	ls.metricTotalEventsResolvedTs.Add(float64(resolvedEventCount))

	// Release buffered events to help GC reclaim memory.
	for i := inputCount; i < batchSize; i++ {
		events[i] = nil
	}
	return maxCommitTs, maxResolvedTs, inputCount, nil
}

// asyncWrite writes events and delete keys asynchronously.
// It returns a channel to notify caller when write is done,
// if needSnap is true, caller receives a snapshot and reads all resolved
// events, up to the maxResolvedTs.
func (ls *Sorter) asyncWrite(
	ctx context.Context, events []*model.PolymorphicEvent, deleteKeys []message.Key, needSnap bool,
) (map[message.Key][]byte, error) {
	writes := make(map[message.Key][]byte)
	for i := range events {
		event := events[i]
		if event.RawKV.OpType == model.OpTypeResolved {
			continue
		}

		key := encoding.EncodeKey(ls.uid, ls.tableID, event)
		value := []byte{}
		var err error
		value, err = ls.serde.Marshal(event, value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		writes[message.Key(key)] = value
	}

	// Delete keys of outputted resolved events.
	for i := range deleteKeys {
		writes[deleteKeys[i]] = []byte{}
	}

	return writes, nil
}

// output nonblocking outputs an event. Caller should retry when it returns false.
func (ls *Sorter) output(event *model.PolymorphicEvent) bool {
	if ls.lastEvent == nil {
		ls.lastEvent = event
	}
	if ls.lastEvent.CRTs > event.CRTs {
		log.Panic("regression",
			zap.Any("lastEntry", ls.lastEvent), zap.Any("event", event),
			zap.Uint64("regionID", event.RegionID()))
	}
	select {
	case ls.outputCh <- event:
		ls.lastEvent = event
		return true
	default:
		return false
	}
}

// outputResolvedTs nonblocking outputs a resolved ts event.
func (ls *Sorter) outputResolvedTs(rts model.Ts) {
	ok := ls.output(model.NewResolvedPolymorphicEvent(0, rts))
	if ok {
		ls.lastSentResolvedTs = rts
	}
}

// outputBufferedResolvedEvents nonblocking output resolved events and
// resolved ts that are buffered in outputBuffer.
// It pops outputted events in the buffer and append their key to deleteKeys.
func (ls *Sorter) outputBufferedResolvedEvents(
	buffer *outputBuffer, sendResolvedTsHint bool,
) {
	hasRemainEvents := false
	// Index of remaining output events
	remainIdx := 0
	// Commit ts of the last outputted events.
	lastCommitTs := uint64(0)
	for idx := range buffer.resolvedEvents {
		event := buffer.resolvedEvents[idx]
		ok := ls.output(event)
		if !ok {
			hasRemainEvents = true
			break
		}
		lastCommitTs = event.CRTs

		// Delete sent events.
		key := encoding.EncodeKey(ls.uid, ls.tableID, event)
		buffer.appendDeleteKey(message.Key(key))
		remainIdx = idx + 1
	}
	// Remove outputted events.
	buffer.shiftResolvedEvents(remainIdx)

	// If all buffered resolved events are sent, send its resolved ts too.
	if sendResolvedTsHint && lastCommitTs != 0 && !hasRemainEvents {
		ls.outputResolvedTs(lastCommitTs)
	}
}

// outputIterEvents nonblocking output resolved events that are buffered
// in leveldb.
// It appends outputted events's key to outputBuffer deleteKeys to delete them
// later, and appends not-yet-send resolved events to outputBuffer resolvedEvents
// to send them later.
// outputBuffer must be empty.
func (ls *Sorter) outputIterEvents(
	iter db.Iterator, buffer *outputBuffer, maxResolvedTs uint64,
) (bool, uint64, error) {
	lenResolvedEvents, lenDeleteKeys := buffer.len()
	if lenDeleteKeys > 0 || lenResolvedEvents > 0 {
		log.Panic("buffer is not empty",
			zap.Int("deleteKeys", lenDeleteKeys),
			zap.Int("resolvedEvents", lenResolvedEvents))
	}

	start := time.Now()
	// The commit ts of buffered resolved events.
	lastCommitTs := uint64(0)
	iterHasNext := iter.Valid()
	lastNext := time.Now()
	hasReadNext := true
SEEK_SEND:
	for ; iterHasNext; iterHasNext = iter.Next() {
		now := time.Now()
		ls.metricIterReadDuration.
			WithLabelValues("next").Observe(now.Sub(lastNext).Seconds())
		lastNext = now
		event := new(model.PolymorphicEvent)
		_, err := ls.serde.Unmarshal(event, iter.Value())
		if err != nil {
			return hasReadNext, 0, errors.Trace(err)
		}
		if lastCommitTs > event.CRTs || lastCommitTs > maxResolvedTs {
			log.Panic("event commit ts is less than previous event or larger than resolved ts",
				zap.Any("event", event), zap.Stringer("key", message.Key(iter.Key())),
				zap.Uint64("ts", lastCommitTs), zap.Uint64("resolvedTs", maxResolvedTs))
		}

		if lastCommitTs == 0 {
			lastCommitTs = event.CRTs
		}
		// Group resolved events that has the same commit ts.
		if lastCommitTs == event.CRTs {
			buffer.appendResolvedEvent(event)
			continue
		}
		// Output buffered events. The current event belongs to a new group.
		ls.outputBufferedResolvedEvents(buffer, true)
		lenResolvedEvents, _ = buffer.len()
		if lenResolvedEvents > 0 {
			// Output blocked, break and free iterator.
			hasReadNext = false
			break SEEK_SEND
		}

		// Append new events to the buffer.
		lastCommitTs = event.CRTs
		buffer.appendResolvedEvent(event)
	}
	elapsed := time.Since(start)
	ls.metricIterReadDuration.WithLabelValues("total").Observe(elapsed.Seconds())

	// Try shrink buffer to release memory.
	buffer.maybeShrink()

	// Events have not been sent, buffer them and output them later.
	// Do not let outputBufferedResolvedEvents output resolved ts, instead we
	// output resolved ts here.
	sendResolvedTsHint := false
	ls.outputBufferedResolvedEvents(buffer, sendResolvedTsHint)
	lenResolvedEvents, _ = buffer.len()

	// Skip output resolved ts if there is any buffered resolved event.
	if lenResolvedEvents != 0 {
		return hasReadNext, 0, nil
	}

	if !iterHasNext && maxResolvedTs != 0 {
		// Iter is exhausted and there is no resolved event (up to max
		// resolved ts), output max resolved ts and return an exhausted
		// resolved ts.
		ls.outputResolvedTs(maxResolvedTs)
		return hasReadNext, maxResolvedTs, nil
	}
	if lastCommitTs != 0 {
		// All buffered resolved events are outputted,
		// output last commit ts.
		ls.outputResolvedTs(lastCommitTs)
	}

	return hasReadNext, 0, nil
}

type pollState struct {
	// Buffer for receiveing new events from AddEntry.
	eventsBuf []*model.PolymorphicEvent
	// Buffer for resolved events and to-be-deleted events.
	outputBuf *outputBuffer
	// The maximum commit ts for all events.
	maxCommitTs uint64
	// The maximum commit ts for all resolved ts events.
	maxResolvedTs uint64
	// All resolved events before the resolved ts are outputted.
	exhaustedResolvedTs uint64

	maxIterResolvedTs uint64
	availableIter     *message.LimitedIterator
	iterCh            chan *message.LimitedIterator
	aliveTime         time.Time
	hasReadNext       bool
}

func (state *pollState) hasResolvedEvents() bool {
	// It has resolved events, if 1) it has buffer resolved events,
	lenResolvedEvents, _ := state.outputBuf.len()
	if lenResolvedEvents > 0 {
		return true
	}
	// or 2) there are some events that can be resolved.
	// -------|-----------------|-------------|-------> time
	// exhaustedResolvedTs
	//                     maxCommitTs
	//                                   maxResolvedTs
	// -------|-----------------|-------------|-------> time
	// exhaustedResolvedTs
	//                     maxResolvedTs
	//                                   maxCommitTs
	if state.exhaustedResolvedTs < state.maxCommitTs &&
		state.exhaustedResolvedTs < state.maxResolvedTs {
		return true
	}

	// Otherwise, there is no event can be resolved.
	// -------|-----------------|-------------|-------> time
	//   maxCommitTs
	//                 exhaustedResolvedTs
	//                                   maxResolvedTs
	return false
}

func (state *pollState) advanceMaxTs(maxCommitTs, maxResolvedTs uint64) {
	// The max commit ts of all received events.
	if maxCommitTs > state.maxCommitTs {
		state.maxCommitTs = maxCommitTs
	}
	// The max resolved ts of all received resolvedTs events.
	if maxResolvedTs > state.maxResolvedTs {
		state.maxResolvedTs = maxResolvedTs
	}
}

func (ls *Sorter) poll(ctx context.Context, state *pollState) error {
	// Wait input or output becomes available.
	waitOutput := state.hasResolvedEvents()
	maxCommitTs, maxResolvedTs, n, err :=
		ls.wait(ctx, waitOutput, state.eventsBuf)
	if err != nil {
		return errors.Trace(err)
	}
	// The max commit ts and resolved ts of all received events.
	state.advanceMaxTs(maxCommitTs, maxResolvedTs)
	// Length of buffered resolved events.
	lenResolvedEvents, _ := state.outputBuf.len()
	if n == 0 && lenResolvedEvents != 0 {
		// No new received events, it means output channel is available.
		// output resolved events as much as possible.
		ls.outputBufferedResolvedEvents(state.outputBuf, true)
		lenResolvedEvents, _ = state.outputBuf.len()
	}
	// New received events.
	newEvents := state.eventsBuf[:n]

	// It can only acquire a snapshot when
	// 1. No buffered resolved events, they must be sent before
	//    sending further resolved events from snapshot.
	needSnap := lenResolvedEvents == 0
	// 2. There are some events that can be resolved.
	needSnap = needSnap && state.hasResolvedEvents()

	// Write new events and delete sent keys.
	writes, err :=
		ls.asyncWrite(ctx, newEvents, state.outputBuf.deleteKeys, needSnap)
	if err != nil {
		return errors.Trace(err)
	}
	// Reset buffer as delete keys are scheduled.
	state.outputBuf.resetDeleteKey()
	// Try shrink buffer to release memory.
	state.outputBuf.maybeShrink()

	// Write and sort events.
	tk := message.Task{
		UID:     ls.uid,
		TableID: ls.tableID,
		Events:  writes,
	}

	if !needSnap {
		// No new events and no resolved events.
		if !state.hasResolvedEvents() && state.maxResolvedTs != 0 {
			ls.outputResolvedTs(state.maxResolvedTs)
		}
		if state.availableIter != nil {
			start := time.Now()
			state.availableIter.Release()
			ls.metricIterReadDuration.
				WithLabelValues("release").Observe(time.Since(start).Seconds())
			state.availableIter = nil
			state.hasReadNext = true
			state.iterCh = nil
		}
		// Send write task to leveldb.
		return ls.router.SendB(ctx, ls.actorID, actormsg.SorterMessage(tk))
	}

	if state.availableIter != nil && state.iterCh != nil {
		log.Panic("assert failed",
			zap.Any("availableIter", state.availableIter),
			zap.Uint64("tableID", ls.tableID))
	}
	// Read and send resolved events from iterator.
	if state.availableIter == nil {
		if state.iterCh == nil {
			// We haven't send iter request, send it.
			iterCh := make(chan *message.LimitedIterator, 1)
			tk.Iter = &message.IterParam{
				UID:     ls.uid,
				TableID: ls.tableID,
				Range: [2][]byte{
					encoding.EncodeTsKey(ls.uid, ls.tableID, 0),
					encoding.EncodeTsKey(ls.uid, ls.tableID, state.maxResolvedTs+1)},
				ResolvedTs: state.maxResolvedTs,
				IterCh:     iterCh,
			}
			state.iterCh = iterCh
		}

		// Try receive iterator.
		var iter *message.LimitedIterator
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case iter = <-state.iterCh:
		default:
		}

		if iter != nil {
			// Iterator received, reset state.iterCh
			state.iterCh = nil
			state.availableIter = iter
			start := time.Now()
			state.aliveTime = start
			state.maxIterResolvedTs = iter.ResolvedTs
			state.availableIter.First()
			ls.metricIterReadDuration.
				WithLabelValues("first").Observe(time.Since(start).Seconds())
		}
	} else {
		if state.hasReadNext {
			state.availableIter.Next()
		}
	}

	// Send write/read task to leveldb.
	err = ls.router.SendB(ctx, ls.actorID, actormsg.SorterMessage(tk))
	if err != nil {
		return errors.Trace(err)
	}

	// Iterator not received yet.
	if state.availableIter == nil {
		return nil
	}

	// Read and send resolved events from iterator.
	hasReadNext, exhaustedResolvedTs, err :=
		ls.outputIterEvents(state.availableIter, state.outputBuf, state.maxIterResolvedTs)
	if err != nil {
		return errors.Trace(err)
	}
	if exhaustedResolvedTs > state.exhaustedResolvedTs {
		state.exhaustedResolvedTs = exhaustedResolvedTs
	}
	state.hasReadNext = hasReadNext
	if !state.availableIter.Valid() || time.Since(state.aliveTime) > ls.iterAliveDuration {
		start := time.Now()
		state.availableIter.Release()
		ls.metricIterReadDuration.
			WithLabelValues("release").Observe(time.Since(start).Seconds())
		state.availableIter = nil
		state.hasReadNext = true

		if state.iterCh != nil {
			log.Panic("assert failed, must not schedule itert",
				zap.Any("availableIter", state.availableIter),
				zap.Uint64("tableID", ls.tableID))
		}
	}

	return nil
}

// Run runs LevelDBSorter
func (ls *Sorter) Run(ctx context.Context) error {
	state := &pollState{
		eventsBuf: make([]*model.PolymorphicEvent, batchReceiveEventSize),
		outputBuf: newOutputBuffer(batchReceiveEventSize),

		maxCommitTs:         uint64(0),
		maxResolvedTs:       uint64(0),
		exhaustedResolvedTs: uint64(0),
	}
	for {
		err := ls.poll(ctx, state)
		if err != nil {
			return errors.Trace(err)
		}
	}
}

// AddEntry adds an RawKVEntry to the EntryGroup
func (ls *Sorter) AddEntry(ctx context.Context, event *model.PolymorphicEvent) {
	if atomic.LoadInt32(&ls.closed) != 0 {
		return
	}
	select {
	case <-ctx.Done():
	case ls.inputCh <- event:
	}
}

// TryAddEntry tries to add an RawKVEntry to the EntryGroup
func (ls *Sorter) TryAddEntry(
	ctx context.Context, event *model.PolymorphicEvent,
) (bool, error) {
	if atomic.LoadInt32(&ls.closed) != 0 {
		return false, nil
	}
	select {
	case <-ctx.Done():
		return false, errors.Trace(ctx.Err())
	case ls.inputCh <- event:
		return true, nil
	default:
		return false, nil
	}
}

// Output returns the sorted raw kv output channel
func (ls *Sorter) Output() <-chan *model.PolymorphicEvent {
	return ls.outputCh
}

// CleanupTask returns a clean up task that delete sorter's data.
func (ls *Sorter) CleanupTask() actormsg.Message {
	return actormsg.SorterMessage(message.NewCleanupTask(ls.uid, ls.tableID))
}
