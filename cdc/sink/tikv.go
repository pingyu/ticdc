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
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/notify"
	"github.com/twmb/murmur3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/tidb/store/tikv"
	tikvconfig "github.com/pingcap/tidb/store/tikv/config"
)

const (
	defaultConcurrency       uint32 = 4
	defaultTikvByteSizeLimit int64  = 4 * 1024 * 1024 // 4MB
)

type rawkvClient interface {
	BatchPut(keys, values [][]byte) error
	BatchDelete(keys [][]byte) error
	Close() error
}

var _ rawkvClient = &tikv.RawKVClient{}

type fnCreateClient func([]string, tikvconfig.Security) (rawkvClient, error)

func createRawKVClient(pdAddr []string, security tikvconfig.Security) (rawkvClient, error) {
	return tikv.NewRawKVClient(pdAddr, security)
}

type tikvSink struct {
	workerNum   uint32
	workerInput []chan struct {
		event      *model.PolymorphicEvent
		resolvedTs uint64
	}
	workerResolvedTs []uint64
	checkpointTs     uint64
	resolvedNotifier *notify.Notifier
	resolvedReceiver *notify.Receiver

	fnCreateClient fnCreateClient
	config         *tikvconfig.Config
	pdAddr         []string
	opts           map[string]string

	statistics *Statistics
}

func createTiKVSink(
	ctx context.Context,
	fnCreateClient fnCreateClient,
	config *tikvconfig.Config,
	pdAddr []string,
	opts map[string]string,
	errCh chan error,
) (*tikvSink, error) {
	workerNum := defaultConcurrency
	if s, ok := opts["concurrency"]; ok {
		c, _ := strconv.Atoi(s)
		workerNum = uint32(c)
	}
	workerInput := make([]chan struct {
		event      *model.PolymorphicEvent
		resolvedTs uint64
	}, workerNum)
	for i := 0; i < int(workerNum); i++ {
		workerInput[i] = make(chan struct {
			event      *model.PolymorphicEvent
			resolvedTs uint64
		}, 12800)
	}

	notifier := new(notify.Notifier)
	resolvedReceiver, err := notifier.NewReceiver(50 * time.Millisecond)
	if err != nil {
		return nil, err
	}
	k := &tikvSink{
		workerNum:        workerNum,
		workerInput:      workerInput,
		workerResolvedTs: make([]uint64, workerNum),
		resolvedNotifier: notifier,
		resolvedReceiver: resolvedReceiver,

		fnCreateClient: fnCreateClient,
		config:         config,
		pdAddr:         pdAddr,
		opts:           opts,

		statistics: NewStatistics(ctx, "TiKVSink", opts),
	}

	go func() {
		if err := k.run(ctx); err != nil && errors.Cause(err) != context.Canceled {
			select {
			case <-ctx.Done():
				return
			case errCh <- err:
			default:
				log.Error("error channel is full", zap.Error(err))
			}
		}
	}()
	return k, nil
}

func (k *tikvSink) dispatch(ev *model.PolymorphicEvent) uint32 {
	hasher := murmur3.New32()
	hasher.Write(ev.RawKV.Key)
	return uint32(hasher.Sum32()) % k.workerNum
}

func (k *tikvSink) EmitRowChangedEvents(ctx context.Context, events []*model.PolymorphicEvent, rows ...*model.RowChangedEvent) error {
	log.Debug("(rawkv)tikvSink::EmitRowChangedEvents", zap.Any("events", events))
	rowsCount := 0
	for _, ev := range events {
		workerIdx := k.dispatch(ev)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case k.workerInput[workerIdx] <- struct {
			event      *model.PolymorphicEvent
			resolvedTs uint64
		}{event: ev}:
		}
		rowsCount++
	}
	k.statistics.AddRowsCount(rowsCount)
	return nil
}

func (k *tikvSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	log.Debug("(rawkv)tikvSink::FlushRowChangedEvents", zap.Uint64("resolvedTs", resolvedTs), zap.Uint64("checkpointTs", k.checkpointTs))
	if resolvedTs <= k.checkpointTs {
		return k.checkpointTs, nil
	}

	for i := 0; i < int(k.workerNum); i++ {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case k.workerInput[i] <- struct {
			event      *model.PolymorphicEvent
			resolvedTs uint64
		}{resolvedTs: resolvedTs}:
		}
	}

	// waiting for all row events are sent to TiKV
flushLoop:
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-k.resolvedReceiver.C:
			for i := 0; i < int(k.workerNum); i++ {
				if resolvedTs > atomic.LoadUint64(&k.workerResolvedTs[i]) {
					continue flushLoop
				}
			}
			break flushLoop
		}
	}
	k.checkpointTs = resolvedTs
	k.statistics.PrintStatus(ctx)
	return k.checkpointTs, nil
}

func (k *tikvSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	return nil
}

func (k *tikvSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	return nil
}

// Initialize registers Avro schemas for all tables
func (k *tikvSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	// No longer need it for now
	return nil
}

func (k *tikvSink) Close(ctx context.Context) error {
	return nil
}

func (k *tikvSink) Barrier(cxt context.Context) error {
	// Barrier does nothing because FlushRowChangedEvents in mq sink has flushed
	// all buffered events forcedlly.
	return nil
}

func (k *tikvSink) run(ctx context.Context) error {
	defer k.resolvedReceiver.Stop()
	wg, ctx := errgroup.WithContext(ctx)
	for i := uint32(0); i < k.workerNum; i++ {
		workerIdx := i
		wg.Go(func() error {
			return k.runWorker(ctx, workerIdx)
		})
	}
	return wg.Wait()
}

type innerBatch struct {
	OpType model.OpType
	Keys   [][]byte
	Values [][]byte
}

type tikvBatcher struct {
	Batches  []innerBatch
	count    int
	byteSize int64
}

func (b *tikvBatcher) Count() int {
	return b.count
}

func (b *tikvBatcher) ByteSize() int64 {
	return b.byteSize
}

func (b *tikvBatcher) Append(ev *model.PolymorphicEvent) {
	// log.Debug("(rawkv)tikvBatch::Append", zap.Any("event", ev))
	if len(b.Batches) == 0 || b.Batches[len(b.Batches)-1].OpType != ev.RawKV.OpType {
		batch := innerBatch{
			OpType: ev.RawKV.OpType,
			Keys:   [][]byte{ev.RawKV.Key},
			Values: [][]byte{ev.RawKV.Value},
		}
		b.Batches = append(b.Batches, batch)
	} else {
		b.Batches[len(b.Batches)-1].Keys = append(b.Batches[len(b.Batches)-1].Keys, ev.RawKV.Key)
		b.Batches[len(b.Batches)-1].Values = append(b.Batches[len(b.Batches)-1].Values, ev.RawKV.Value)
	}
	b.count += 1
	b.byteSize += int64(len(ev.RawKV.Key) + len(ev.RawKV.Value))
}

func (b *tikvBatcher) Reset() {
	b.Batches = b.Batches[:0]
	b.count = 0
	b.byteSize = 0
}

func (k *tikvSink) runWorker(ctx context.Context, workerIdx uint32) error {
	log.Info("(rawkv)tikvSink worker start", zap.Uint32("workerIdx", workerIdx))
	input := k.workerInput[workerIdx]

	cli, err := k.fnCreateClient(k.pdAddr, tikvconfig.Security{})
	if err != nil {
		return err
	}
	defer cli.Close()

	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	batcher := tikvBatcher{}

	flushToTiKV := func() error {
		return k.statistics.RecordBatchExecution(func() (int, error) {
			log.Debug("(rawkv)tikvSink::flushToTiKV", zap.Any("batches", batcher.Batches))
			thisBatchSize := batcher.Count()
			if thisBatchSize == 0 {
				return 0, nil
			}

			for _, batch := range batcher.Batches {
				var err error
				if batch.OpType == model.OpTypePut {
					err = cli.BatchPut(batch.Keys, batch.Values)
				} else if batch.OpType == model.OpTypeDelete {
					err = cli.BatchDelete(batch.Keys)
				}
				if err != nil {
					return 0, err
				}
				log.Debug("(rawkv)TiKVSink flushed", zap.Int("thisBatchSize", thisBatchSize), zap.Any("batch", batch))
			}
			batcher.Reset()
			return thisBatchSize, nil
		})
	}
	for {
		var e struct {
			event      *model.PolymorphicEvent
			resolvedTs uint64
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
			if err := flushToTiKV(); err != nil {
				return errors.Trace(err)
			}
			continue
		case e = <-input:
		}
		if e.event == nil {
			if e.resolvedTs != 0 {
				log.Debug("(rawkv)tikvSink::runWorker push workerResolvedTs", zap.Uint32("workerIdx", workerIdx), zap.Uint64("event.resolvedTs", e.resolvedTs))
				if err := flushToTiKV(); err != nil {
					return errors.Trace(err)
				}

				atomic.StoreUint64(&k.workerResolvedTs[workerIdx], e.resolvedTs)
				k.resolvedNotifier.Notify()
			}
			continue
		}
		log.Debug("(rawkv)tikvSink::runWorker append event", zap.Uint32("workerIdx", workerIdx), zap.Any("event", e.event))
		batcher.Append(e.event)

		if batcher.ByteSize() >= defaultTikvByteSizeLimit {
			if err := flushToTiKV(); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func parseTiKVUri(sinkURI *url.URL, opts map[string]string) (*tikvconfig.Config, []string, error) {
	config := tikvconfig.DefaultConfig()

	pdAddr := strings.Split(sinkURI.Host, ",")
	if len(pdAddr) > 0 {
		for i, d := range pdAddr {
			pdAddr[i] = "http://" + d
		}
	} else {
		pdAddr = append(pdAddr, "http://127.0.0.1:2379")
	}

	s := sinkURI.Query().Get("concurrency")
	if s != "" {
		_, err := strconv.Atoi(s)
		if err != nil {
			return nil, nil, cerror.WrapError(cerror.ErrTiKVInvalidConfig, err)
		}
		opts["concurrency"] = s
	}

	// s = sinkURI.Query().Get("max-batch-size")
	// if s != "" {
	// 	opts["max-batch-size"] = s
	// }
	return &config, pdAddr, nil
}

func newTiKVSink(ctx context.Context, sinkURI *url.URL, filter *filter.Filter, replicaConfig *config.ReplicaConfig, opts map[string]string, errCh chan error) (*tikvSink, error) {
	config, pdAddr, err := parseTiKVUri(sinkURI, opts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sink, err := createTiKVSink(ctx, createRawKVClient, config, pdAddr, opts, errCh)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return sink, nil
}
