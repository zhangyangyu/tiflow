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
	"container/list"
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/sorter/leveldb/message"
	"github.com/pingcap/ticdc/pkg/actor"
	actormsg "github.com/pingcap/ticdc/pkg/actor/message"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/db"
	cerrors "github.com/pingcap/ticdc/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

type iterQueue struct {
	// Queue of IteratorParams
	list.List
	// TableID set.
	tables map[tableKey]struct{}
}

type tableKey struct {
	UID     uint32
	TableID uint64
}

func (q *iterQueue) push(params *message.IterParam) {
	key := tableKey{UID: params.UID, TableID: params.TableID}
	_, ok := q.tables[key]
	if ok {
		return
	}
	q.tables[key] = struct{}{}
	q.List.PushBack(params)
}

func (q *iterQueue) pop() (*message.IterParam, bool) {
	item := q.List.Front()
	if item == nil {
		return nil, false
	}
	q.List.Remove(item)
	param := item.Value.(*message.IterParam)
	delete(q.tables, tableKey{UID: param.UID, TableID: param.TableID})
	return param, true
}

// DBActor is a db actor, it reads, writes and deletes key value pair in its db.
type DBActor struct {
	id      actor.ID
	db      db.DB
	wb      db.Batch
	wbSize  int
	wbCap   int
	iterSem *semaphore.Weighted
	iterQ   iterQueue

	deleteCount int
	compact     *CompactScheduler

	closedWg *sync.WaitGroup

	metricWriteDuration prometheus.Observer
	metricWriteBytes    prometheus.Observer
}

var _ actor.Actor = (*DBActor)(nil)

// NewDBActor returns a db actor.
func NewDBActor(
	id int, db db.DB, cfg *config.DBConfig, compact *CompactScheduler,
	wg *sync.WaitGroup, captureAddr string,
) (*DBActor, actor.Mailbox, error) {
	idTag := strconv.Itoa(id)
	// Write batch size should be larger than block size to save CPU.
	const writeBatchSizeFactor = 16
	wbSize := cfg.BlockSize * writeBatchSizeFactor
	// Double batch capacity to avoid memory reallocation.
	const writeBatchCapFactor = 2
	wbCap := wbSize * writeBatchCapFactor
	wb := db.Batch(wbCap)
	// IterCount limits the total number of opened iterators to release db
	// resources in time.
	iterSema := semaphore.NewWeighted(int64(cfg.Concurrency))
	mb := actor.NewMailbox(actor.ID(id), cfg.Concurrency)
	wg.Add(1)

	dba := &DBActor{
		id:      actor.ID(id),
		db:      db,
		wb:      wb,
		iterSem: iterSema,
		iterQ:   iterQueue{},
		wbSize:  wbSize,
		wbCap:   wbCap,
		compact: compact,

		closedWg: wg,

		metricWriteDuration: sorterWriteDurationHistogram.WithLabelValues(captureAddr, idTag),
		metricWriteBytes:    sorterWriteBytesHistogram.WithLabelValues(captureAddr, idTag),
	}
	dba.iterQ.tables = make(map[tableKey]struct{})
	dba.iterQ.Init()
	return dba, mb, nil
}

// Schedule a compact task when there are too many deletion.
func (ldb *DBActor) maybeScheduleCompact() {
	if ldb.compact.maybeCompact(ldb.id, ldb.deleteCount) {
		// Reset delete key count if schedule compaction successfully.
		ldb.deleteCount = 0
		return
	}
}

func (ldb *DBActor) close(err error) {
	log.Info("db actor quit", zap.Uint64("ID", uint64(ldb.id)), zap.Error(err))
	ldb.closedWg.Done()
}

func (ldb *DBActor) maybeWrite(force bool) (bool, error) {
	bytes := len(ldb.wb.Repr())
	if bytes >= ldb.wbSize || (force && bytes != 0) {
		startTime := time.Now()
		err := ldb.wb.Commit()
		if err != nil {
			return false, cerrors.ErrLevelDBSorterError.GenWithStackByArgs(err)
		}
		ldb.metricWriteDuration.Observe(time.Since(startTime).Seconds())
		ldb.metricWriteBytes.Observe(float64(bytes))

		// Reset write batch or reclaim memory if it grows too large.
		if cap(ldb.wb.Repr()) <= ldb.wbCap {
			ldb.wb.Reset()
		} else {
			ldb.wb = ldb.db.Batch(ldb.wbCap)
		}
		return true, nil
	}
	return false, nil
}

// Poll implements actor.Actor.
// It handles tasks by writing kv, deleting kv and taking iterators.
func (ldb *DBActor) Poll(ctx context.Context, tasks []actormsg.Message) bool {
	select {
	case <-ctx.Done():
		ldb.close(ctx.Err())
		return false
	default:
	}
	requireIter := false
	for i := range tasks {
		var task message.Task
		msg := tasks[i]
		switch msg.Tp {
		case actormsg.TypeSorterTask:
			task = msg.SorterTask
		case actormsg.TypeStop:
			ldb.close(nil)
			return false
		default:
			log.Panic("unexpected message", zap.Any("message", msg))
		}
		events, params := task.Events, task.Iter

		for k, v := range events {
			if len(v) != 0 {
				ldb.wb.Put([]byte(k), v)
			} else {
				// Delete the key if value is empty
				ldb.wb.Delete([]byte(k))
				ldb.deleteCount++
			}

			// Do not force write, batching for efficiency.
			if _, err := ldb.maybeWrite(false); err != nil {
				log.Panic("db error", zap.Error(err))
			}
		}
		if params != nil {
			// Append to slice for later batch acquiring iterators.
			ldb.iterQ.push(params)
			requireIter = true
		}
	}

	// Force write only if there is a task requires an iterator.
	wrote, err := ldb.maybeWrite(requireIter)
	if err != nil {
		log.Panic("db error", zap.Error(err))
	}
	if wrote {
		// Schedule compact if there are many deletion.
		ldb.maybeScheduleCompact()
	}
	// Batch acquire iterators.
	for {
		succeed := ldb.iterSem.TryAcquire(1)
		if !succeed {
			break
		}
		params, ok := ldb.iterQ.pop()
		if !ok {
			ldb.iterSem.Release(1)
			break
		}

		iterCh := params.IterCh
		iterRange := params.Range
		snap, err := ldb.db.Snapshot()
		if err != nil {
			log.Panic("db error", zap.Error(err))
		}
		iter := snap.Iterator(iterRange[0], iterRange[1])
		iterCh <- &message.LimitedIterator{
			Iterator:   iter,
			Sema:       ldb.iterSem,
			ResolvedTs: params.ResolvedTs,
		}
		snap.Release()
		// close(iterCh)
	}

	return true
}
