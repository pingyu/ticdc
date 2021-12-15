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

package sink

import (
	"context"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
)

const (
	defaultMetricInterval = time.Second * 15
)

// Manager manages table sinks, maintains the relationship between table sinks and backendSink
type Manager struct {
	backendSink  Sink
	checkpointTs model.Ts
	tableSinks   map[model.TableID]*tableSink
	eventSinks   map[uint64]*tableSink
	tableSinksMu sync.Mutex

	flushMu sync.Mutex

	drawbackChan chan drawbackMsg
}

// NewManager creates a new Sink manager
func NewManager(ctx context.Context, backendSink Sink, errCh chan error, checkpointTs model.Ts) *Manager {
	drawbackChan := make(chan drawbackMsg, 16)
	return &Manager{
		backendSink:  newBufferSink(ctx, backendSink, errCh, checkpointTs, drawbackChan),
		checkpointTs: checkpointTs,
		tableSinks:   make(map[model.TableID]*tableSink),
		eventSinks:   make(map[uint64]*tableSink),
		drawbackChan: drawbackChan,
	}
}

// CreateTableSink creates a table sink
func (m *Manager) CreateTableSink(tableID model.TableID, checkpointTs model.Ts) Sink {
	m.tableSinksMu.Lock()
	defer m.tableSinksMu.Unlock()
	if _, exist := m.tableSinks[tableID]; exist {
		log.Panic("the table sink already exists", zap.Uint64("tableID", uint64(tableID)))
	}
	sink := &tableSink{
		tableID:   tableID,
		manager:   m,
		buffer:    make([]*model.RowChangedEvent, 0, 128),
		events:    make([]*model.PolymorphicEvent, 0, 128),
		emittedTs: checkpointTs,
	}
	m.tableSinks[tableID] = sink
	return sink
}

// func (m *Manager) CreateKVSink(span regionspan.ComparableSpan, checkpointTs model.Ts) Sink {
// 	log.Warn("(rawkv) Manager::CreateKVSink")
// 	hash := span.Hash()
// 	m.tableSinksMu.Lock()
// 	defer m.tableSinksMu.Unlock()
// 	if _, exist := m.kvSinks[hash]; exist {
// 		// hash conflict is acceptable
// 		log.Warn("the kv sink already exists", zap.String("span", span.String()), zap.Uint64("hash", hash))
// 	}
// 	sink := &tableSink{
// 		span:      span,
// 		manager:   m,
// 		buffer:    make([]*model.RawKVEntry, 0, 128),
// 		emittedTs: checkpointTs,
// 	}
// 	m.kvSinks[hash] = sink
// 	return sink
// }

// Close closes the Sink manager and backend Sink, this method can be reentrantly called
func (m *Manager) Close(ctx context.Context) error {
	return m.backendSink.Close(ctx)
}

func (m *Manager) getMinEmittedTs() model.Ts {
	m.tableSinksMu.Lock()
	defer m.tableSinksMu.Unlock()
	if len(m.tableSinks) == 0 {
		return m.getCheckpointTs()
	}
	minTs := model.Ts(math.MaxUint64)
	for _, tableSink := range m.tableSinks {
		emittedTs := tableSink.getEmittedTs()
		if minTs > emittedTs {
			minTs = emittedTs
		}
	}
	// TODO(rawkv): eventSinks
	// if len(m.kvSinks) == 0 {
	// 	return m.getCheckpointTs()
	// }
	// for _, kvSink := range m.kvSinks {
	// 	emittedTs := kvSink.getEmittedTs()
	// 	if minTs > emittedTs {
	// 		minTs = emittedTs
	// 	}
	// }
	return minTs
}

func (m *Manager) flushBackendSink(ctx context.Context) (model.Ts, error) {
	m.flushMu.Lock()
	defer m.flushMu.Unlock()
	minEmittedTs := m.getMinEmittedTs()
	checkpointTs, err := m.backendSink.FlushRowChangedEvents(ctx, minEmittedTs)
	if err != nil {
		return m.getCheckpointTs(), errors.Trace(err)
	}
	atomic.StoreUint64(&m.checkpointTs, checkpointTs)
	return checkpointTs, nil
}

// func (m *Manager) flushBackendKVSink(ctx context.Context) (model.Ts, error) {
// 	m.flushMu.Lock()
// 	defer m.flushMu.Unlock()
// 	minEmittedTs := m.getMinEmittedTs()
// 	checkpointTs, err := m.backendSink.FlushRawKVEvents(ctx, minEmittedTs)
// 	if err != nil {
// 		return m.getCheckpointTs(), errors.Trace(err)
// 	}
// 	atomic.StoreUint64(&m.checkpointTs, checkpointTs)
// 	return checkpointTs, nil
// }

func (m *Manager) destroyTableSink(ctx context.Context, tableID model.TableID) error {
	m.tableSinksMu.Lock()
	delete(m.tableSinks, tableID)
	m.tableSinksMu.Unlock()
	callback := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.drawbackChan <- drawbackMsg{tableID: tableID, span: nil, callback: callback}:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-callback:
	}
	return m.backendSink.Barrier(ctx)
}

// func (m *Manager) destroyKVSink(ctx context.Context, span regionspan.ComparableSpan) error {
// 	hash := span.Hash()
// 	m.tableSinksMu.Lock()
// 	delete(m.kvSinks, hash)
// 	m.tableSinksMu.Unlock()
// 	callback := make(chan struct{})
// 	select {
// 	case <-ctx.Done():
// 		return ctx.Err()
// 	case m.drawbackChan <- drawbackMsg{tableID: -1, callback: callback, span: &span}:
// 	}
// 	select {
// 	case <-ctx.Done():
// 		return ctx.Err()
// 	case <-callback:
// 	}
// 	return m.backendSink.Barrier(ctx)
// }

func (m *Manager) getCheckpointTs() uint64 {
	return atomic.LoadUint64(&m.checkpointTs)
}

type tableSink struct {
	tableID model.TableID
	manager *Manager
	buffer  []*model.RowChangedEvent
	// emittedTs means all of events which of commitTs less than or equal to emittedTs is sent to backendSink
	emittedTs model.Ts

	span   regionspan.ComparableSpan
	events []*model.PolymorphicEvent
}

func (t *tableSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	// do nothing
	return nil
}

func (t *tableSink) EmitRowChangedEvents(ctx context.Context, events []*model.PolymorphicEvent, rows ...*model.RowChangedEvent) error {
	log.Warn("(rawkv)tableSink::EmitRowChangedEvents", zap.Any("events", events), zap.Any("rows", rows))
	t.buffer = append(t.buffer, rows...)
	t.events = append(t.events, events...)
	return nil
}

func (t *tableSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	// the table sink doesn't receive the DDL event
	return nil
}

func (t *tableSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	log.Warn("(rawkv)tableSink::FlushRowChangedEvents", zap.Uint64("resolvedTs", resolvedTs))
	i := sort.Search(len(t.events), func(i int) bool {
		return t.events[i].CRTs > resolvedTs
	})
	if i == 0 {
		atomic.StoreUint64(&t.emittedTs, resolvedTs)
		return t.manager.flushBackendSink(ctx)
	}
	resolvedRows := t.buffer[:i]
	t.buffer = append(make([]*model.RowChangedEvent, 0, len(t.buffer[i:])), t.buffer[i:]...)

	resolvedEvents := t.events[:i]
	t.events = append(make([]*model.PolymorphicEvent, 0, len(t.events[i:])), t.events[i:]...)

	log.Warn("(rawkv)tableSink::FlushRowChangedEvents", zap.Uint64("resolvedTs", resolvedTs), zap.Any("resolvedRows", resolvedRows), zap.Any("resolvedEvents", resolvedEvents))

	err := t.manager.backendSink.EmitRowChangedEvents(ctx, resolvedEvents, resolvedRows...)
	if err != nil {
		return t.manager.getCheckpointTs(), errors.Trace(err)
	}
	atomic.StoreUint64(&t.emittedTs, resolvedTs)
	return t.manager.flushBackendSink(ctx)
}

func (t *tableSink) getEmittedTs() uint64 {
	return atomic.LoadUint64(&t.emittedTs)
}

func (t *tableSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	// the table sink doesn't receive the checkpoint event
	return nil
}

// Note once the Close is called, no more events can be written to this table sink
func (t *tableSink) Close(ctx context.Context) error {
	return t.manager.destroyTableSink(ctx, t.tableID)
}

// Barrier is not used in table sink
func (t *tableSink) Barrier(ctx context.Context) error {
	return nil
}

type drawbackMsg struct {
	tableID  model.TableID
	span     *regionspan.ComparableSpan
	callback chan struct{}
}

type bufferSink struct {
	Sink
	checkpointTs uint64
	buffer       map[model.TableID][]*model.RowChangedEvent
	eventBuffer  map[model.TableID][]*model.PolymorphicEvent
	bufferMu     sync.Mutex
	flushTsChan  chan uint64
	drawbackChan chan drawbackMsg
}

func newBufferSink(
	ctx context.Context,
	backendSink Sink,
	errCh chan error,
	checkpointTs model.Ts,
	drawbackChan chan drawbackMsg,
) Sink {
	sink := &bufferSink{
		Sink: backendSink,
		// buffer shares the same flow control with table sink
		buffer:       make(map[model.TableID][]*model.RowChangedEvent),
		eventBuffer:  make(map[model.TableID][]*model.PolymorphicEvent),
		checkpointTs: checkpointTs,
		flushTsChan:  make(chan uint64, 128),
		drawbackChan: drawbackChan,
	}
	go sink.run(ctx, errCh)
	return sink
}

func (b *bufferSink) run(ctx context.Context, errCh chan error) {
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	advertiseAddr := util.CaptureAddrFromCtx(ctx)
	metricFlushDuration := flushRowChangedDuration.WithLabelValues(advertiseAddr, changefeedID, "Flush")
	metricEmitRowDuration := flushRowChangedDuration.WithLabelValues(advertiseAddr, changefeedID, "EmitRow")
	metricBufferSize := bufferChanSizeGauge.WithLabelValues(advertiseAddr, changefeedID)
	for {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			if err != nil && errors.Cause(err) != context.Canceled {
				errCh <- err
			}
			return
		case drawback := <-b.drawbackChan:
			b.bufferMu.Lock()
			if drawback.tableID >= 0 {
				delete(b.buffer, drawback.tableID)
				delete(b.eventBuffer, drawback.tableID)
			}
			// if drawback.span != nil {
			// 	delete(b.kvBuffer, drawback.span.Hash())
			// }
			b.bufferMu.Unlock()
			close(drawback.callback)
		case resolvedTs := <-b.flushTsChan:
			b.bufferMu.Lock()
			// find all rows before resolvedTs and emit to backend sink
			for tableID, rows := range b.buffer {
				i := sort.Search(len(rows), func(i int) bool {
					return rows[i].CommitTs > resolvedTs
				})

				start := time.Now()
				events := b.eventBuffer[tableID][:i]
				err := b.Sink.EmitRowChangedEvents(ctx, events, rows[:i]...)
				if err != nil {
					b.bufferMu.Unlock()
					if errors.Cause(err) != context.Canceled {
						errCh <- err
					}
					return
				}
				dur := time.Since(start)
				metricEmitRowDuration.Observe(dur.Seconds())

				// put remaining rows back to buffer
				// append to a new, fixed slice to avoid lazy GC
				b.buffer[tableID] = append(make([]*model.RowChangedEvent, 0, len(rows[i:])), rows[i:]...)
				b.eventBuffer[tableID] = append(make([]*model.PolymorphicEvent, 0, len(events[i:])), events[i:]...)
			}
			b.bufferMu.Unlock()

			start := time.Now()
			checkpointTs, err := b.Sink.FlushRowChangedEvents(ctx, resolvedTs)
			if err != nil {
				if errors.Cause(err) != context.Canceled {
					errCh <- err
				}
				return
			}
			atomic.StoreUint64(&b.checkpointTs, checkpointTs)

			dur := time.Since(start)
			metricFlushDuration.Observe(dur.Seconds())
			if dur > 3*time.Second {
				log.Warn("flush row changed events too slow",
					zap.Duration("duration", dur), util.ZapFieldChangefeed(ctx))
			}
		case <-time.After(defaultMetricInterval):
			metricBufferSize.Set(float64(len(b.buffer) + len(b.eventBuffer))) // TODO(rawkv): double length here.
		}
	}
}

func (b *bufferSink) EmitRowChangedEvents(ctx context.Context, events []*model.PolymorphicEvent, rows ...*model.RowChangedEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		if len(rows) == 0 {
			return nil
		}
		tableID := rows[0].Table.TableID
		b.bufferMu.Lock()
		b.buffer[tableID] = append(b.buffer[tableID], rows...)
		b.eventBuffer[tableID] = append(b.eventBuffer[tableID], events...)
		b.bufferMu.Unlock()
	}
	return nil
}

func (b *bufferSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	select {
	case <-ctx.Done():
		return atomic.LoadUint64(&b.checkpointTs), ctx.Err()
	case b.flushTsChan <- resolvedTs:
	}
	return atomic.LoadUint64(&b.checkpointTs), nil
}
