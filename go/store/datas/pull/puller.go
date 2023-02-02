// Copyright 2019 Dolthub, Inc.
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

package pull

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/nbs"
)

// ErrDBUpToDate is the error code returned from NewPuller in the event that there is no work to do.
var ErrDBUpToDate = errors.New("the database does not need to be pulled as it's already up to date")

// ErrIncompatibleSourceChunkStore is the error code returned from NewPuller in
// the event that the source ChunkStore does not implement `NBSCompressedChunkStore`.
var ErrIncompatibleSourceChunkStore = errors.New("the chunk store of the source database does not implement NBSCompressedChunkStore.")

const (
	maxChunkWorkers       = 2
	outstandingTableFiles = 2
)

// FilledWriters store CmpChunkTableWriter that have been filled and are ready to be flushed.  In the future will likely
// add the md5 of the data to this structure to be used to verify table upload calls.
type FilledWriters struct {
	wr *nbs.CmpChunkTableWriter
}

// CmpChnkAndRefs holds a CompressedChunk and all of it's references
type CmpChnkAndRefs struct {
	cmpChnk nbs.CompressedChunk
	refs    map[hash.Hash]bool
}

type WalkAddrs func(chunks.Chunk, func(hash.Hash, bool) error) error

// Puller is used to sync data between to Databases
type Puller struct {
	waf WalkAddrs

	srcChunkStore nbs.NBSCompressedChunkStore
	sinkDBCS      chunks.ChunkStore
	hashes        hash.HashSet
	downloaded    hash.HashSet

	wr            *nbs.CmpChunkTableWriter
	tablefileSema *semaphore.Weighted
	tempDir       string
	chunksPerTF   int

	pushLog *log.Logger

	statsCh chan Stats
	stats   *stats
}

// NewPuller creates a new Puller instance to do the syncing.  If a nil puller is returned without error that means
// that there is nothing to pull and the sinkDB is already up to date.
func NewPuller(
	ctx context.Context,
	tempDir string,
	chunksPerTF int,
	srcCS, sinkCS chunks.ChunkStore,
	walkAddrs WalkAddrs,
	hashes []hash.Hash,
	statsCh chan Stats,
) (*Puller, error) {
	// Sanity Check
	hs := hash.NewHashSet(hashes...)
	missing, err := srcCS.HasMany(ctx, hs)
	if err != nil {
		return nil, err
	}
	if missing.Size() != 0 {
		return nil, errors.New("not found")
	}

	hs = hash.NewHashSet(hashes...)
	missing, err = sinkCS.HasMany(ctx, hs)
	if err != nil {
		return nil, err
	}
	if missing.Size() == 0 {
		return nil, ErrDBUpToDate
	}

	if srcCS.Version() != sinkCS.Version() {
		return nil, fmt.Errorf("cannot pull from src to sink; src version is %v and sink version is %v", srcCS.Version(), sinkCS.Version())
	}

	srcChunkStore, ok := srcCS.(nbs.NBSCompressedChunkStore)
	if !ok {
		return nil, ErrIncompatibleSourceChunkStore
	}

	wr, err := nbs.NewCmpChunkTableWriter(tempDir)

	if err != nil {
		return nil, err
	}

	var pushLogger *log.Logger
	if dbg, ok := os.LookupEnv("PUSH_LOG"); ok && strings.ToLower(dbg) == "true" {
		logFilePath := filepath.Join(tempDir, "push.log")
		f, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)

		if err == nil {
			pushLogger = log.New(f, "", log.Lmicroseconds)
		}
	}

	p := &Puller{
		waf:           walkAddrs,
		srcChunkStore: srcChunkStore,
		sinkDBCS:      sinkCS,
		hashes:        hash.NewHashSet(hashes...),
		downloaded:    hash.HashSet{},
		tablefileSema: semaphore.NewWeighted(outstandingTableFiles),
		tempDir:       tempDir,
		wr:            wr,
		chunksPerTF:   chunksPerTF,
		pushLog:       pushLogger,
		statsCh:       statsCh,
		stats:         &stats{},
	}

	if lcs, ok := sinkCS.(chunks.LoggingChunkStore); ok {
		lcs.SetLogger(p)
	}

	return p, nil
}

func (p *Puller) Logf(fmt string, args ...interface{}) {
	if p.pushLog != nil {
		p.pushLog.Printf(fmt, args...)
	}
}

type readable interface {
	Reader() (io.ReadCloser, error)
	Remove() error
}

type tempTblFile struct {
	id          string
	read        readable
	numChunks   int
	chunksLen   uint64
	contentLen  uint64
	contentHash []byte
}

type countingReader struct {
	io.ReadCloser
	cnt *uint64
}

func (c countingReader) Read(p []byte) (int, error) {
	n, err := c.ReadCloser.Read(p)
	atomic.AddUint64(c.cnt, uint64(n))
	return n, err
}

func emitStats(s *stats, ch chan Stats) (cancel func()) {
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	cancel = func() {
		close(done)
		wg.Wait()
	}

	go func() {
		defer wg.Done()
		sampleduration := 100 * time.Millisecond
		samplesinsec := uint64((1 * time.Second) / sampleduration)
		weight := 0.1
		ticker := time.NewTicker(sampleduration)
		defer ticker.Stop()
		var lastSendBytes, lastFetchedBytes uint64
		for {
			select {
			case <-ticker.C:
				newSendBytes := atomic.LoadUint64(&s.finishedSendBytes)
				newFetchedBytes := atomic.LoadUint64(&s.fetchedSourceBytes)
				sendBytesDiff := newSendBytes - lastSendBytes
				fetchedBytesDiff := newFetchedBytes - lastFetchedBytes

				newSendBPS := float64(sendBytesDiff * samplesinsec)
				newFetchedBPS := float64(fetchedBytesDiff * samplesinsec)

				curSendBPS := math.Float64frombits(atomic.LoadUint64(&s.sendBytesPerSec))
				curFetchedBPS := math.Float64frombits(atomic.LoadUint64(&s.fetchedSourceBytesPerSec))

				smoothedSendBPS := newSendBPS
				if curSendBPS != 0 {
					smoothedSendBPS = curSendBPS + weight*(newSendBPS-curSendBPS)
				}

				smoothedFetchBPS := newFetchedBPS
				if curFetchedBPS != 0 {
					smoothedFetchBPS = curFetchedBPS + weight*(newFetchedBPS-curFetchedBPS)
				}

				if smoothedSendBPS < 1 {
					smoothedSendBPS = 0
				}
				if smoothedFetchBPS < 1 {
					smoothedFetchBPS = 0
				}

				atomic.StoreUint64(&s.sendBytesPerSec, math.Float64bits(smoothedSendBPS))
				atomic.StoreUint64(&s.fetchedSourceBytesPerSec, math.Float64bits(smoothedFetchBPS))

				lastSendBytes = newSendBytes
				lastFetchedBytes = newFetchedBytes
			case <-done:
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		updateduration := 1 * time.Second
		ticker := time.NewTicker(updateduration)
		for {
			select {
			case <-ticker.C:
				ch <- s.read()
			case <-done:
				ch <- s.read()
				return
			}
		}
	}()

	return cancel
}

type stats struct {
	finishedSendBytes uint64
	bufferedSendBytes uint64
	sendBytesPerSec   uint64

	totalSourceChunks        uint64
	fetchedSourceChunks      uint64
	fetchedSourceBytes       uint64
	fetchedSourceBytesPerSec uint64

	sendBytesPerSecF          float64
	fetchedSourceBytesPerSecF float64
}

type Stats struct {
	FinishedSendBytes uint64
	BufferedSendBytes uint64
	SendBytesPerSec   float64

	TotalSourceChunks        uint64
	FetchedSourceChunks      uint64
	FetchedSourceBytes       uint64
	FetchedSourceBytesPerSec float64
}

func (s *stats) read() Stats {
	var ret Stats
	ret.FinishedSendBytes = atomic.LoadUint64(&s.finishedSendBytes)
	ret.BufferedSendBytes = atomic.LoadUint64(&s.bufferedSendBytes)
	ret.SendBytesPerSec = math.Float64frombits(atomic.LoadUint64(&s.sendBytesPerSec))
	ret.TotalSourceChunks = atomic.LoadUint64(&s.totalSourceChunks)
	ret.FetchedSourceChunks = atomic.LoadUint64(&s.fetchedSourceChunks)
	ret.FetchedSourceBytes = atomic.LoadUint64(&s.fetchedSourceBytes)
	ret.FetchedSourceBytesPerSec = math.Float64frombits(atomic.LoadUint64(&s.fetchedSourceBytesPerSec))
	return ret
}

// The puller is structured as a number of concurrent goroutines communicating
// over channels.  They all run within the same errgroup and they all listen
// for ctx.Done on every channel send and receive.
//
// goUploadTempTableFile is a goroutine which reads off the <-chan tmpTblFile
// and uploads the read table file. We run multiple copies of it to get
// upload parallelism on pushes.
//
// goFinalizeTableFiles is a goroutine which reads off the <-chan FilledWriters
// channel.  It finalizes a table file and adds the tmpTblFile to the upload
// channel. It writes to shared state, fileIdToNumChunks, which will be used by
// Pull to call destDB.AddTableFilesToManifest if everything completes
// successfully.
//
// goCmpChunkWriter reads off a <-chan nbs.CompressedChunk carrying chunks to
// write to destDB. It writes the incoming compressed chunk to a compressed
// chunk writer. When the compressed chunk writer is large enough, it sends the
// table file as a FilledWriter down the filled writers channel and starts a
// new table file.
//
// goNovelHashesFilter reads off a <-chan hash.HashSet which is sending
// potentially novel chunk hashes we may want to fetch from srcDB, coming from
// goBatchHashes. It filters the incoming addresses by a set of addresses it has
// already downloaded. It calls HasMany on the destDB, collecting only novel
// addresses which are not already present in the destDB and not already
// downloaded. It adds those novel addresses to the downloaded set and then
// forwards on the set of hashes which we actually want to fetch.
//
// goGetManyChunks reads off a <-chan hash.HashSet. It calls
// srcDB.GetManyCompressed(..., hs) for each HashSet it receives. It forwards
// each compressed chunk it receives to goCmpChunkWriter and to
// goChunkAddrsProcessor. We run multiple copies of goGetManyChunks to
// implement io parallelism and attempt to paper over stragglers with regards
// to the GetManyCompressed interface.
//
// goChunkAddrsProcessor walks the chunk addresses present in each incoming
// chunk using p.waf. It forwards the found addresses to goNovelHashesFilter.
// If p.waf is CPU bound (chunk decoding or snappy decompression), we can run
// multiple copies of goChunkAddrsProcessor.
//
// goBatchHashes is middleware which connects goChunkAddrsProcessor to
// goNovelHashesFilter. It reads off the channel carrying addreses from
// goChunkAddrsProcessor and builds up a batch of |BatchSize| to send to
// |goNovelHashesFilter|. In order to avoid deadlocking, it must buffer arbitrary
// amounts of data, but it always attempts to forward along the most recent
// complete batch it has built. This hueristic allows for reasonable fanout
// while typically focusing on making progress at lower levels of the tree when
// we have lots of addresses to walk.
//
// There is a loop between the channels:
//
//        +---------------------------------------------------------------------------+
//        |                                                                           |
//        V                                                                           |
// goNovelHashesFilter -> goGetManyChunks -> goChunkAddrsProcessor -> goBatchHashes --+
//
// which makes finalization a bit awkward. Intuitively, the pipeline should
// shutdown as soon as everything that |goNovelHashesFilter| ever output has been
// processed and there it nothing more inflight. In order to accomplish this,
// we use a WaitGroupCh abstraction. It looks a lot like a sync.WaitGroup, but
// instead of |Wait()| has a |Done() <-chan struct{}| which is closed when the
// count reaches 0. The maintained count is the count of batches existing
// between |goBatchHashes| and |goNovelHashesFilter| plus the count of chunk
// addresses in flight between |goNovelHashesFilter| and |goBatchHashes|. When
// the count reaches 0 there are no in flight chunk addresses and no more
// batches being built or sent. To main the count, |goNovelHashesFilter|
// increases the inflightWg by |len(hs)| for every |HashSet| it sends to its
// output.  |goBatchHashes| gets an incoming |HashSet| for every visited chunk
// and it decreases the count by 1 for every |HashSet| it processes.
// |goBatchHashes| also increases the count by 1 for every batch it creates. To
// balance that, |goNovelHashesFilter| decreases the count by 1 for every batch
// it receives from |goBatchHashes|.

func (p *Puller) goUploadTempTableFile(ctx context.Context, files <-chan tempTblFile) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case f, ok := <-files:
			if !ok {
				return nil
			}
			err := p.uploadTempTableFile(ctx, f)
			if err != nil {
				return err
			}
		}
	}
}

func (p *Puller) goFinalizeTableFiles(ctx context.Context, fileIdToNumChunks map[string]int, files chan<- tempTblFile, fw <-chan FilledWriters) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case f, ok := <-fw:
			if !ok {
				return nil
			}
			chunksLen := f.wr.ContentLength()
			id, err := f.wr.Finish()
			if err != nil {
				return err
			}
			ttf := tempTblFile{
				id:          id,
				read:        f.wr,
				numChunks:   f.wr.ChunkCount(),
				chunksLen:   chunksLen,
				contentLen:  f.wr.ContentLength(),
				contentHash: f.wr.GetMD5(),
			}
			fileIdToNumChunks[id] = ttf.numChunks
			select {
			case <-ctx.Done():
				return nil
			case files <- ttf:
			}
		}
	}
}

func (p *Puller) goCmpChunkWriter(ctx context.Context, chnks <-chan nbs.CompressedChunk, fw chan<- FilledWriters) error {
	var wr *nbs.CmpChunkTableWriter
	for {
		select {
		case <-ctx.Done():
			return nil
		case chnk, ok := <-chnks:
			if !ok {
				if wr != nil {
					select {
					case <-ctx.Done():
						return nil
					case fw <- FilledWriters{wr}:
					}
				}
				return nil
			}

			if wr == nil {
				var err error
				wr, err = nbs.NewCmpChunkTableWriter(p.tempDir)
				if err != nil {
					return err
				}
			}

			err := wr.AddCmpChunk(chnk)
			if err != nil {
				return err
			}

			atomic.AddUint64(&p.stats.bufferedSendBytes, uint64(len(chnk.FullCompressedChunk)))

			if wr.ChunkCount() >= p.chunksPerTF {
				select {
				case fw <- FilledWriters{wr}:
				case <-ctx.Done():
					return nil
				}
				wr = nil
			}
		}
	}
}

func (p *Puller) goNovelHashesFilter(ctx context.Context, newAddrsCh <-chan hash.HashSet, toPullCh chan<- hash.HashSet, inflight *WaitGroupCh) error {
	downloaded := make(hash.HashSet)
	for {
		select {
		case <- ctx.Done():
			return nil
		case newAddrs, ok := <- newAddrsCh:
			if !ok {
				panic("bug in puller finalization; newAddrs was closed before outstanding closed Ch.")
			}
			newAddrs = limitToNewChunks(newAddrs, downloaded)
			var err error
			newAddrs, err = p.sinkDBCS.HasMany(ctx, newAddrs)
			if err != nil {
				return err
			}
			if len(newAddrs) != 0 {
				inflight.Add(int32(len(newAddrs)))
				downloaded.InsertAll(newAddrs)
				select {
				case <-ctx.Done():
					return nil
				case toPullCh <- newAddrs:
				}
			}
			inflight.Add(-1)
		case <-inflight.Done():
			return nil
		}
	}
}

type WaitGroupCh struct {
	cnt  int32
	done chan struct{}
}

func (w *WaitGroupCh) Add(i int32) {
	c := atomic.AddInt32(&w.cnt, i)
	if c == 0 {
		close(w.done)
	}
}

func (w *WaitGroupCh) Done() <-chan struct{} {
	return w.done
}

func newWaitGroupCh(i int32) *WaitGroupCh {
	return &WaitGroupCh{int32(i), make(chan struct{})}
}

func (p *Puller) goGetManyChunks(ctx context.Context, addrs <-chan hash.HashSet, toWriter chan<- nbs.CompressedChunk, toWalker chan<- nbs.CompressedChunk) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case addrSet, ok := <-addrs:
			if !ok {
				return nil
			}
			seen := int32(0)
			err := p.srcChunkStore.GetManyCompressed(ctx, addrSet, func(ctx context.Context, c nbs.CompressedChunk) {
				atomic.AddUint64(&p.stats.fetchedSourceBytes, uint64(len(c.FullCompressedChunk)))
				atomic.AddUint64(&p.stats.fetchedSourceChunks, uint64(1))
				atomic.AddInt32(&seen, 1)
				select {
				case toWriter <- c:
				case <-ctx.Done():
				}
				select {
				case toWalker <- c:
				case <-ctx.Done():
				}
			})
			if err != nil {
				return err
			}
			if int(seen) != len(addrSet) {
				return errors.New("failed to get all chunks.")
			}
		}
	}
}

const BatchSize = 64 * 1024

func (p *Puller) goBatchHashes(ctx context.Context, in <-chan hash.HashSet, out chan<- hash.HashSet, inflight *WaitGroupCh) error {
	batches := make([]hash.HashSet, 0)
	var cur hash.HashSet
	for {
		toSend := cur
		if len(batches) > 0 {
			toSend = batches[len(batches)-1]
		}
		if toSend != nil {
			select {
			case <-ctx.Done():
				return nil
			case out <- toSend:
				if len(batches) > 0 {
					batches = batches[:len(batches)-1]
				} else {
					cur = nil
				}
			case addrs, ok := <- in:
				if !ok {
					panic("error in Puller finalization; goBatchHashes saw a closed addrs channel while it still had batches to send.")
				}
				if len(addrs) > 0 {
					if cur == nil {
						// We made a new batch; increase inflight count.
						inflight.Add(1)
						cur = addrs
					} else {
						cur.InsertAll(addrs)
						if len(cur) >= BatchSize {
							batches = append(batches, cur)
							cur = nil
						}
					}
				}
				// A chunk got fully processed; decrease inflight count.
				inflight.Add(-1)
			}
		} else {
			select {
			case <-ctx.Done():
				return nil
			case addrs, ok := <- in:
				if !ok {
					return nil
				}
				if len(addrs) > 0 {
					// We made a new batch; increase inflight count.
					inflight.Add(1)
					cur = addrs
				}
				// A chunk got fully processed; decrease inflight count.
				inflight.Add(-1)
			}
		}
	}
}

func (p *Puller) goChunkAddrsProcessor(ctx context.Context, chnks <-chan nbs.CompressedChunk, addrs chan<- hash.HashSet) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case cmpChnk, ok := <-chnks:
			if !ok {
				return nil
			}
			chnk, err := cmpChnk.ToChunk()
			if err != nil {
				return err
			}
			refs := make(hash.HashSet)
			err = p.waf(chnk, func(h hash.Hash, _ bool) error {
				refs.Insert(h)
				return nil
			})
			if err != nil {
				return err
			}
			// TODO: Can send nil and avoid allocating a bunch of empty refs...
			select {
			case <-ctx.Done():
				return nil
			case addrs <- refs:
			}
		}
	}
}

func (p *Puller) uploadTempTableFile(ctx context.Context, tmpTblFile tempTblFile) error {
	fileSize := tmpTblFile.contentLen
	defer func() {
		_ = tmpTblFile.read.Remove()
	}()

	// By tracking the number of bytes uploaded here,
	// we can add bytes on to our bufferedSendBytes when
	// we have to retry a table file write.
	var localUploaded uint64
	return p.sinkDBCS.(nbs.TableFileStore).WriteTableFile(ctx, tmpTblFile.id, tmpTblFile.numChunks, tmpTblFile.contentHash, func() (io.ReadCloser, uint64, error) {
		rc, err := tmpTblFile.read.Reader()
		if err != nil {
			return nil, 0, err
		}

		if localUploaded == 0 {
			// So far, we've added all the bytes for the compressed chunk data.
			// We add the remaining bytes here --- bytes for the index and the
			// table file footer.
			atomic.AddUint64(&p.stats.bufferedSendBytes, uint64(fileSize)-tmpTblFile.chunksLen)
		} else {
			// A retry. We treat it as if what was already uploaded was rebuffered.
			atomic.AddUint64(&p.stats.bufferedSendBytes, uint64(localUploaded))
			localUploaded = 0
		}
		fWithStats := countingReader{countingReader{rc, &localUploaded}, &p.stats.finishedSendBytes}

		return fWithStats, uint64(fileSize), nil
	})
}

// Pull executes the sync operation
func (p *Puller) Pull(ctx context.Context) error {
	if p.statsCh != nil {
		c := emitStats(p.stats, p.statsCh)
		defer c()
	}

	hashes := make(hash.HashSet)
	hashes.InsertAll(p.hashes)
	fileIdToNumChunks := make(map[string]int)

	eg, ectx := errgroup.WithContext(ctx)

	const numAddrWalkers = 2
	const numGetMany = 3

	// Our uploaders
	toUploadCh := make(chan tempTblFile)
	eg.Go(func() error {
		eg, ectx := errgroup.WithContext(ectx)
		for i := 0; i < 3; i++ {
			eg.Go(func() error {
				return p.goUploadTempTableFile(ectx, toUploadCh)
			})
		}
		return eg.Wait()
	})

	// Our finalizer
	writersCh := make(chan FilledWriters)
	eg.Go(func() error {
		defer close(toUploadCh)
		return p.goFinalizeTableFiles(ectx, fileIdToNumChunks, toUploadCh, writersCh)
	})

	// Our writer
	toWriteCh := make(chan nbs.CompressedChunk)
	eg.Go(func() error {
		defer close(writersCh)
		return p.goCmpChunkWriter(ectx, toWriteCh, writersCh)
	})

	walkedAddrsCh := make(chan hash.HashSet)
	toWalkCh := make(chan nbs.CompressedChunk)

	// Our addr walkers
	eg.Go(func() error {
		defer close(walkedAddrsCh)
		eg, ectx := errgroup.WithContext(ectx)
		for i := 0; i < numAddrWalkers; i++ {
			eg.Go(func() error {
				return p.goChunkAddrsProcessor(ectx, toWalkCh, walkedAddrsCh)
			})
		}
		return eg.Wait()
	})

	getManyCh := make(chan hash.HashSet)
	eg.Go(func() error {
		defer close(toWriteCh)
		defer close(toWalkCh)
		eg, ectx := errgroup.WithContext(ectx)
		for i := 0; i < numGetMany; i++ {
			eg.Go(func() error {
				return p.goGetManyChunks(ectx, getManyCh, toWriteCh, toWalkCh)
			})
		}
		return eg.Wait()
	})

	inflight := newWaitGroupCh(1)
	batchesCh := make(chan hash.HashSet, 1)
	batchesCh <- hashes

	eg.Go(func() error {
		defer close(getManyCh)
		return p.goNovelHashesFilter(ectx, batchesCh, getManyCh, inflight)
	})

	eg.Go(func() error {
		defer close(batchesCh)
		return p.goBatchHashes(ectx, walkedAddrsCh, batchesCh, inflight)
	})

	err := eg.Wait()
	if err != nil {
		return err
	}

	return p.sinkDBCS.(nbs.TableFileStore).AddTableFilesToManifest(ctx, fileIdToNumChunks)
}

func limitToNewChunks(absent hash.HashSet, downloaded hash.HashSet) hash.HashSet {
	smaller := absent
	longer := downloaded
	if len(absent) > len(downloaded) {
		smaller = downloaded
		longer = absent
	}

	for k := range smaller {
		if longer.Has(k) {
			absent.Remove(k)
		}
	}

	return absent
}
