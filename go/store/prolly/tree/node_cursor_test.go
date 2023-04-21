// Copyright 2021 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"unsafe"

	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNodeCursor(t *testing.T) {
	t.Run("new cursor at item", func(t *testing.T) {
		testNewCursorAtItem(t, 10)
		testNewCursorAtItem(t, 100)
		testNewCursorAtItem(t, 1000)
		testNewCursorAtItem(t, 10_000)
	})

	t.Run("get ordinal at item", func(t *testing.T) {
		counts := []int{10, 100, 1000, 10_000}
		for _, c := range counts {
			t.Run(fmt.Sprintf("%d", c), func(t *testing.T) {
				testGetOrdinalOfCursor(t, c)
			})
		}
	})

	t.Run("retreat past beginning", func(t *testing.T) {
		ctx := context.Background()
		root, _, ns := randomTree(t, 10_000)
		assert.NotNil(t, root)
		before, err := newCursorAtStart(ctx, ns, root)
		assert.NoError(t, err)
		err = before.retreat(ctx)
		assert.NoError(t, err)
		assert.False(t, before.Valid())

		start, err := newCursorAtStart(ctx, ns, root)
		assert.NoError(t, err)
		assert.True(t, start.compare(before) > 0, "start is after before")
		assert.True(t, before.compare(start) < 0, "before is before start")

		// Backwards iteration...
		end, err := newCursorAtEnd(ctx, ns, root)
		assert.NoError(t, err)
		i := 0
		for end.compare(before) > 0 {
			i++
			err = end.retreat(ctx)
			assert.NoError(t, err)
		}
		assert.Equal(t, 10_000/2, i)
	})
}

func testNewCursorAtItem(t *testing.T, count int) {
	root, items, ns := randomTree(t, count)
	assert.NotNil(t, root)

	ctx := context.Background()
	for i := range items {
		key, value := items[i][0], items[i][1]
		cur, err := NewCursorAtKey(ctx, ns, root, val.Tuple(key), keyDesc)
		require.NoError(t, err)
		assert.Equal(t, key, cur.CurrentKey())
		assert.Equal(t, value, cur.currentValue())
	}

	validateTreeItems(t, ns, root, items)
}

func testGetOrdinalOfCursor(t *testing.T, count int) {
	tuples, desc := AscendingUintTuples(count)

	ctx := context.Background()
	ns := NewTestNodeStore()
	serializer := message.NewProllyMapSerializer(desc, ns.Pool())
	chkr, err := newEmptyChunker(ctx, ns, serializer)
	require.NoError(t, err)

	for _, item := range tuples {
		err = chkr.AddPair(ctx, Item(item[0]), Item(item[1]))
		assert.NoError(t, err)
	}
	nd, err := chkr.Done(ctx)
	assert.NoError(t, err)

	for i := 0; i < len(tuples); i++ {
		curr, err := NewCursorAtKey(ctx, ns, nd, tuples[i][0], desc)
		require.NoError(t, err)

		ord, err := getOrdinalOfCursor(curr)
		require.NoError(t, err)

		assert.Equal(t, uint64(i), ord)
	}

	b := val.NewTupleBuilder(desc)
	b.PutUint32(0, uint32(len(tuples)))
	aboveItem := b.Build(sharedPool)

	curr, err := NewCursorAtKey(ctx, ns, nd, aboveItem, desc)
	require.NoError(t, err)

	ord, err := getOrdinalOfCursor(curr)
	require.NoError(t, err)

	require.Equal(t, uint64(len(tuples)), ord)

	// A cursor past the end should return an ordinal count equal to number of
	// nodes.
	curr, err = newCursorPastEnd(ctx, ns, nd)
	require.NoError(t, err)

	ord, err = getOrdinalOfCursor(curr)
	require.NoError(t, err)

	require.Equal(t, uint64(len(tuples)), ord)
}

func randomTree(t *testing.T, count int) (Node, [][2]Item, NodeStore) {
	ctx := context.Background()
	ns := NewTestNodeStore()
	serializer := message.NewProllyMapSerializer(valDesc, ns.Pool())
	chkr, err := newEmptyChunker(ctx, ns, serializer)
	require.NoError(t, err)

	items := randomTupleItemPairs(count/2, ns)
	for _, item := range items {
		err = chkr.AddPair(ctx, Item(item[0]), Item(item[1]))
		assert.NoError(t, err)
	}
	nd, err := chkr.Done(ctx)
	assert.NoError(t, err)
	return nd, items, ns
}

var keyDesc = val.NewTupleDescriptor(
	val.Type{Enc: val.Int64Enc, Nullable: false},
)
var valDesc = val.NewTupleDescriptor(
	val.Type{Enc: val.Int64Enc, Nullable: true},
	val.Type{Enc: val.Int64Enc, Nullable: true},
	val.Type{Enc: val.Int64Enc, Nullable: true},
	val.Type{Enc: val.Int64Enc, Nullable: true},
)

func randomTupleItemPairs(count int, ns NodeStore) (items [][2]Item) {
	tups := RandomTuplePairs(count, keyDesc, valDesc, ns)
	items = make([][2]Item, count)
	if len(tups) != len(items) {
		panic("mismatch")
	}

	for i := range items {
		items[i][0] = Item(tups[i][0])
		items[i][1] = Item(tups[i][1])
	}
	return
}

func BenchmarkSortSearch(b *testing.B) {
	const count = uint16(512)
	const mod = count - 1
	keys := randomKeys(count)
	c4 := makeChunk(keys[:128])
	c3 := makeChunk(keys[128:256])
	c2 := makeChunk(keys[256:384])
	c1 := makeChunk(keys[384:])
	b.Run("linear search", func(b *testing.B) {
		var x uint16
		for i := 0; i < b.N; i++ {
			k := keys[uint16(i)&mod]
			x = linearSearch(k, c4)
			x = linearSearch(k, c3)
			x = linearSearch(k, c2)
			x = linearSearch(k, c1)
		}
		assert.True(b, x < count)
		b.ReportAllocs()
	})
	b.Run("sort search", func(b *testing.B) {
		var x uint16
		for i := 0; i < b.N; i++ {
			k := keys[uint16(i)&mod]
			x = sortSearch(k, c4)
			x = sortSearch(k, c3)
			x = sortSearch(k, c2)
			x = sortSearch(k, c1)
		}
		assert.True(b, x < count)
		b.ReportAllocs()
	})
	b.Run("branchless search", func(b *testing.B) {
		var x uint16
		for i := 0; i < b.N; i++ {
			k := keys[uint16(i)&mod]
			x = branchlessSearch(k, c4)
			x = branchlessSearch(k, c3)
			x = branchlessSearch(k, c2)
			x = branchlessSearch(k, c1)
		}
		assert.True(b, x < count)
		b.ReportAllocs()
	})
}

func TestLinearSearch(t *testing.T) {
	const n uint16 = 256
	present := randomKeys(n)
	absent := randomKeys(n)
	ch := makeChunk(present)
	for _, k := range present {
		exp := sort.Search(int(n), func(i int) bool {
			return bytes.Compare(k, ch.get(uint16(i))) < 0
		})
		act := linearSearch(k, ch)
		assert.Equal(t, exp, int(act))
	}
	for _, k := range absent {
		exp := sort.Search(int(n), func(i int) bool {
			return bytes.Compare(k, ch.get(uint16(i))) < 0
		})
		act := linearSearch(k, ch)
		assert.Equal(t, exp, int(act))
	}
}

func linearSearch(k []byte, c chunk) (i uint16) {
	for i = 0; i < c.count(); i++ {
		if bytes.Compare(c.get(i), k) >= 0 {
			break
		}
	}
	return
}

func sortSearch(k []byte, c chunk) uint16 {
	i, j := uint16(0), c.count()
	for i < j {
		h := (i + j) >> 1
		cmp := bytes.Compare(k, c.get(h))
		if cmp > 0 {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}

func branchlessSearch(k []byte, c chunk) uint16 {
	lo, hi := uint16(0), c.count()
	for hi > 1 {
		mid := hi >> 1
		cmp := bytes.Compare(c.get((lo+mid)-1), k)
		lo += cmov(cmp, mid)
		hi -= mid
	}
	return lo
}

func cmov(cmp int, mid uint16) uint16 {
	return uint16(cmp>>1) & mid
}

func TestCmov(t *testing.T) {
	assert.Equal(t, uint16(0), cmov(1, uint16(7)))
	assert.Equal(t, uint16(0), cmov(0, uint16(7)))
	assert.Equal(t, uint16(7), cmov(-1, uint16(7)))
}

func randomKeys(count uint16) (keys [][]byte) {
	const maxKeySz = 32
	keys = make([][]byte, count)
	bb := make([]byte, count*maxKeySz)
	rand.Read(bb)
	for i := range keys {
		sz := rand.Intn(24) + 8
		keys[i] = bb[:sz]
		bb = bb[sz:]
	}
	return
}

func TestMakeChunk(t *testing.T) {
	data := make([]byte, 64)
	keys := make([][]byte, 64)
	for i := range keys {
		data[i] = byte(i)
		keys[i] = data[i : i+1]
	}
	c := makeChunk(keys)
	for i := range keys {
		assert.Equal(t, keys[i], c.get(uint16(i)))
	}
}

func BenchmarkSplice(b *testing.B) {
	const (
		keys   = uint16(128)
		writes = uint16(1024)
		kmod   = keys - 1
		wmod   = writes - 1
	)

	ch := makeChunk(randomKeys(keys))
	wr := randomKeys(writes)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		j := uint16(i) & kmod
		ch = ch.splice(j, wr[uint16(i)&wmod])
	}
	b.ReportAllocs()
}

type chunk struct {
	buf []byte
}

func (c chunk) count() uint16 {
	return readUint16(c.buf)
}

func (c chunk) get(i uint16) (v []byte) {
	o := uint16Sz + uint16Sz*i
	start := unsafeUint16(c.buf, o)
	stop := unsafeUint16(c.buf, o+uint16Sz)
	v = c.buf[start:stop]
	return
}

func (c chunk) splice(i uint16, v []byte) (up chunk) {
	o := uint16Sz + uint16Sz*i
	start := readUint16(c.buf[o:])
	stop := readUint16(c.buf[o+uint16Sz:])

	diff := uint16(len(v)) - (stop - start)
	up.buf = make([]byte, len(c.buf)+int(diff))

	copy(up.buf, c.buf[:start])
	copy(up.buf[int(start):], v)
	copy(up.buf[int(start)+len(v):], c.buf[stop:])

	// update offsets
	for j := i + 1; j < c.count(); j++ {
		bb := c.buf[uint16Sz+uint16Sz*j:]
		off := readUint16(bb)
		writeUint16(bb, off+diff)
	}
	return
}

const (
	uint16Sz = 2
)

func makeChunk(keys [][]byte) (c chunk) {
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})
	sz := uint16Sz + uint16Sz*(len(keys)+1)
	off := uint16(sz) // key array offset
	for _, k := range keys {
		sz += len(k)
	}
	c.buf = make([]byte, sz)
	bb := c.buf

	// key count
	writeUint16(bb, uint16(len(keys)))
	bb = bb[uint16Sz:]
	// key offsets
	for _, k := range keys {
		writeUint16(bb, off)
		bb = bb[uint16Sz:]
		off += uint16(len(k))
	}
	writeUint16(bb, off)
	bb = bb[uint16Sz:]
	for _, k := range keys {
		n := copy(bb, k)
		bb = bb[n:]
	}
	assertTrue(len(bb) == 0, "fuck")
	return
}

func unsafeUint16(buf []byte, off uint16) uint16 {
	return *(*uint16)(unsafe.Pointer(&buf[off]))
}

func readUint16(buf []byte) uint16 {
	return binary.LittleEndian.Uint16(buf)
}

func writeUint16(buf []byte, v uint16) {
	binary.LittleEndian.PutUint16(buf, v)
}
