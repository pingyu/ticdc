// Copyright 2020 PingCAP, Inc.
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
	"bytes"
	"context"
	"fmt"
	"net/url"

	"github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	tikvconfig "github.com/pingcap/tidb/store/tikv/config"
	"go.uber.org/zap"
)

type tikvSinkSuite struct{}

var _ = check.Suite(&tikvSinkSuite{})

type mockRawKVClient struct {
	b *bytes.Buffer
}

func (c *mockRawKVClient) String() string {
	return c.b.String()
}

func (c *mockRawKVClient) Reset() {
	c.b.Reset()
}

func (c *mockRawKVClient) BatchPut(keys, values [][]byte) error {
	for i, k := range keys {
		fmt.Fprintf(c.b, "1,%s,%s|", string(k), string(values[i]))
	}
	fmt.Fprintf(c.b, "\n")
	return nil
}

func (c *mockRawKVClient) BatchDelete(keys [][]byte) error {
	for _, k := range keys {
		fmt.Fprintf(c.b, "2,%s,|", string(k))
	}
	fmt.Fprintf(c.b, "\n")
	return nil
}

func (c *mockRawKVClient) Close() error {
	return nil
}

var _ rawkvClient = &mockRawKVClient{}

func (s tikvSinkSuite) TestTiKVSinkConfig(c *check.C) {
	uri := "tikv://127.0.0.1:1001,127.0.0.2:1002/?concurrency=10"
	sinkURI, err := url.Parse(uri)
	c.Assert(err, check.IsNil)

	opts := make(map[string]string)
	_, pdAddr, err := parseTiKVUri(sinkURI, opts)
	c.Assert(err, check.IsNil)
	c.Assert(len(pdAddr), check.Equals, 2)
	c.Assert(pdAddr[0], check.Equals, "http://127.0.0.1:1001")
	c.Assert(pdAddr[1], check.Equals, "http://127.0.0.2:1002")

	c.Assert(opts["concurrency"], check.Equals, "10")
}

func (s tikvSinkSuite) TestTiKVSinkBatcher(c *check.C) {
	batcher := tikvBatcher{}
	keys := []string{
		"a", "b", "c", "a",
	}
	values := []string{
		"1", "2", "3", "",
	}
	opTypes := []model.OpType{
		model.OpTypePut, model.OpTypePut, model.OpTypePut, model.OpTypeDelete,
	}
	for i := range keys {
		ev := &model.PolymorphicEvent{
			RawKV: &model.RawKVEntry{
				OpType: opTypes[i],
				Key:    []byte(keys[i]),
				Value:  []byte(values[i]),
			},
			CRTs: uint64(i),
		}
		batcher.Append(ev)
	}
	c.Assert(len(batcher.Batches), check.Equals, 2)
	c.Assert(batcher.Count(), check.Equals, 4)
	c.Assert(batcher.ByteSize(), check.Equals, int64(7))

	buf := &bytes.Buffer{}

	fmt.Fprintf(buf, "%+v", batcher.Batches[0])
	c.Assert(buf.String(), check.Equals, "{OpType:1 Keys:[[97] [98] [99]] Values:[[49] [50] [51]]}")
	buf.Reset()

	fmt.Fprintf(buf, "%+v", batcher.Batches[1])
	c.Assert(buf.String(), check.Equals, "{OpType:2 Keys:[[97]] Values:[[]]}")
	buf.Reset()

	batcher.Reset()
	c.Assert(len(batcher.Batches), check.Equals, 0)
	c.Assert(batcher.Count(), check.Equals, 0)
	c.Assert(batcher.ByteSize(), check.Equals, int64(0))
}

func (s tikvSinkSuite) TestTiKVSink(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())

	uri := "tikv://127.0.0.1:1001,127.0.0.2:1002/?concurrency=10"
	sinkURI, err := url.Parse(uri)
	c.Assert(err, check.IsNil)

	opts := make(map[string]string)
	config, pdAddr, err := parseTiKVUri(sinkURI, opts)
	c.Assert(err, check.IsNil)

	errCh := make(chan error)

	mockCli := &mockRawKVClient{
		b: &bytes.Buffer{},
	}

	fnCreate := func(pdAddr []string, security tikvconfig.Security) (rawkvClient, error) {
		return mockCli, nil
	}

	sink, err := createTiKVSink(ctx, fnCreate, config, pdAddr, opts, errCh)
	c.Assert(err, check.IsNil)

	keys := []string{
		"a", "b", "c", "a",
	}
	values := []string{
		"1", "2", "3", "",
	}
	opTypes := []model.OpType{
		model.OpTypePut, model.OpTypePut, model.OpTypePut, model.OpTypeDelete,
	}
	for i := range keys {
		ev := &model.PolymorphicEvent{
			RawKV: &model.RawKVEntry{
				OpType: opTypes[i],
				Key:    []byte(keys[i]),
				Value:  []byte(values[i]),
			},
			CRTs: uint64(i),
		}
		err = sink.EmitRowChangedEvents(ctx, []*model.PolymorphicEvent{ev})
		c.Assert(err, check.IsNil)
	}
	checkpointTs, err := sink.FlushRowChangedEvents(ctx, uint64(120))
	c.Assert(err, check.IsNil)
	c.Assert(checkpointTs, check.Equals, uint64(120))

	output := mockCli.String()
	log.Info("Output", zap.String("mockCli.String()", output))
	// c.Assert(output, check.Equals, "BatchPUT\n(a,1)(b,2)(c,3)\n")
	mockCli.Reset()

	// flush older resolved ts
	checkpointTs, err = sink.FlushRowChangedEvents(ctx, uint64(110))
	c.Assert(err, check.IsNil)
	c.Assert(checkpointTs, check.Equals, uint64(120))

	err = sink.Close(ctx)
	c.Assert(err, check.IsNil)

	cancel()
}
