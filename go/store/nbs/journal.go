// Copyright 2022 Dolthub, Inc.
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

package nbs

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
)

var chunkJournalFeatureFlag = false

func init() {
	if os.Getenv("DOLT_ENABLE_CHUNK_JOURNAL") != "" {
		chunkJournalFeatureFlag = true
	}
}

func UseJournalStore(path string) bool {
	if chunkJournalFeatureFlag {
		return true
	}
	ok, err := journalFileExists(filepath.Join(path, chunkJournalAddr))
	if err != nil {
		panic(err)
	}
	return ok
}

const (
	chunkJournalName = chunkJournalAddr // todo
	tmpJournalPrefix = "nbs_journal_"
)

type journalConjoiner struct {
	child conjoinStrategy
}

func (c journalConjoiner) conjoinRequired(ts tableSet) bool {
	return c.child.conjoinRequired(ts)
}

func (c journalConjoiner) chooseConjoinees(upstream []tableSpec) (conjoinees, keepers []tableSpec, err error) {
	var stash tableSpec // don't conjoin journal
	pruned := make([]tableSpec, 0, len(upstream))
	for _, ts := range upstream {
		if isJournalAddr(ts.name) {
			stash = ts
		} else {
			pruned = append(pruned, ts)
		}
	}
	conjoinees, keepers, err = c.child.chooseConjoinees(pruned)
	if err != nil {
		return nil, nil, err
	}
	keepers = append(keepers, stash)
	return
}

// chunkJournal is a persistence abstraction for a NomsBlockStore.
// It implemented both manifest and tablePersister, durably writing
// both memTable persists and manifest updates to a single file.
type chunkJournal struct {
	wr   *journalWriter
	src  journalChunkSource
	path string

	contents  manifestContents
	backing   manifest
	persister *fsTablePersister
}

var _ tablePersister = &chunkJournal{}
var _ tableFilePersister = &chunkJournal{}
var _ manifest = &chunkJournal{}
var _ manifestGCGenUpdater = &chunkJournal{}
var _ io.Closer = &chunkJournal{}

func newChunkJournal(ctx context.Context, nbfVers, dir string, m manifest, p *fsTablePersister) (*chunkJournal, error) {
	path, err := filepath.Abs(filepath.Join(dir, chunkJournalName))
	if err != nil {
		return nil, err
	}

	j := &chunkJournal{path: path, backing: m, persister: p}
	j.contents.nbfVers = nbfVers

	ok, err := journalFileExists(path)
	if err != nil {
		return nil, err
	} else if ok {
		// only open a journalWriter if the journal file exists,
		// otherwise we wait to open in case we're cloning
		if err = j.openJournal(ctx); err != nil {
			return nil, err
		}
	}
	return j, nil
}

func (j *chunkJournal) openJournal(ctx context.Context) (err error) {
	var ok bool
	ok, err = journalFileExists(j.path)
	if err != nil {
		return err
	}

	if !ok { // create new journal file
		j.wr, err = createJournalWriter(ctx, j.path)
		if err != nil {
			return err
		}

		_, j.src, err = j.wr.ProcessJournal(ctx)
		if err != nil {
			return err
		}

		var contents manifestContents
		ok, contents, err = j.backing.ParseIfExists(ctx, &Stats{}, nil)
		if err != nil {
			return err
		}
		if ok {
			// write the current root hash to the journal file
			if err = j.wr.WriteRootHash(contents.root); err != nil {
				return
			}
			j.contents = contents
		}
		return
	}

	j.wr, ok, err = openJournalWriter(ctx, j.path)
	if err != nil {
		return err
	} else if !ok {
		return errors.New("missing chunk journal " + j.path)
	}

	// parse existing journal file
	var root hash.Hash
	root, j.src, err = j.wr.ProcessJournal(ctx)
	if err != nil {
		return err
	}

	var contents manifestContents
	ok, contents, err = j.backing.ParseIfExists(ctx, &Stats{}, nil)
	if err != nil {
		return err
	}

	if ok {
		// the journal file is the source of truth for the root hash, true-up persisted manifest
		contents.root = root
		contents, err = j.backing.Update(ctx, contents.lock, contents, &Stats{}, nil)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("manifest not found when opening chunk journal")
	}
	j.contents = contents
	return
}

// Persist implements tablePersister.
func (j *chunkJournal) Persist(ctx context.Context, mt *memTable, haver chunkReader, stats *Stats) (chunkSource, error) {
	if err := j.maybeInit(ctx); err != nil {
		return nil, err
	}

	if haver != nil {
		sort.Sort(hasRecordByPrefix(mt.order)) // hasMany() requires addresses to be sorted.
		if _, err := haver.hasMany(mt.order); err != nil {
			return nil, err
		}
		sort.Sort(hasRecordByOrder(mt.order)) // restore "insertion" order for write
	}

	for _, record := range mt.order {
		if record.has {
			continue
		}
		c := chunks.NewChunkWithHash(hash.Hash(*record.a), mt.chunks[*record.a])
		cc := ChunkToCompressedChunk(c)
		lookup, err := j.wr.WriteChunk(cc)
		if err != nil {
			return nil, err
		}
		j.src.lookups.put(*record.a, lookup)
		j.src.uncompressedSz += uint64(c.Size())
	}
	return j.src, nil
}

// ConjoinAll implements tablePersister.
func (j *chunkJournal) ConjoinAll(ctx context.Context, sources chunkSources, stats *Stats) (chunkSource, error) {
	return j.persister.ConjoinAll(ctx, sources, stats)
}

// Open implements tablePersister.
func (j *chunkJournal) Open(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (chunkSource, error) {
	if name == journalAddr {
		if err := j.maybeInit(ctx); err != nil {
			return nil, err
		}
		return j.src, nil
	}
	return j.persister.Open(ctx, name, chunkCount, stats)
}

// Exists implements tablePersister.
func (j *chunkJournal) Exists(ctx context.Context, name addr, chunkCount uint32, stats *Stats) (bool, error) {
	return j.persister.Exists(ctx, name, chunkCount, stats)
}

// PruneTableFiles implements tablePersister.
func (j *chunkJournal) PruneTableFiles(ctx context.Context, contents manifestContents, mtime time.Time) error {
	return j.persister.PruneTableFiles(ctx, contents, mtime)
}

func (j *chunkJournal) Path() string {
	return filepath.Dir(j.path)
}

func (j *chunkJournal) CopyTableFile(ctx context.Context, r io.ReadCloser, fileId string, chunkCount uint32) error {
	return j.persister.CopyTableFile(ctx, r, fileId, chunkCount)
}

// Name implements manifest.
func (j *chunkJournal) Name() string {
	return j.path
}

// Update implements manifest.
func (j *chunkJournal) Update(ctx context.Context, lastLock addr, next manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	if j.wr == nil {
		// pass the update to |j.backing| if the journals is not initialized
		return j.backing.Update(ctx, lastLock, next, stats, writeHook)
	}

	if j.contents.gcGen != next.gcGen {
		return manifestContents{}, errors.New("use UpdateGCGen to update GC generation")
	} else if j.contents.lock != lastLock {
		return j.contents, nil // |next| is stale
	}

	if writeHook != nil {
		if err := writeHook(); err != nil {
			return manifestContents{}, err
		}
	}

	// if |next| has a different table file set, flush to |j.backing|
	if !equalSpecs(j.contents.specs, next.specs) {
		_, mc, err := j.backing.ParseIfExists(ctx, stats, nil)
		if err != nil {
			return manifestContents{}, err
		}
		lastLock = mc.lock

		mc, err = j.backing.Update(ctx, lastLock, next, stats, nil)
		if err != nil {
			return manifestContents{}, err
		} else if mc.lock != next.lock {
			return manifestContents{}, errOptimisticLockFailedTables
		}
	}

	if err := j.wr.WriteRootHash(next.root); err != nil {
		return manifestContents{}, err
	}
	j.contents = next

	return j.contents, nil
}

// UpdateGCGen implements manifestGCGenUpdater.
func (j *chunkJournal) UpdateGCGen(ctx context.Context, lastLock addr, next manifestContents, stats *Stats, writeHook func() error) (manifestContents, error) {
	updater, ok := j.backing.(manifestGCGenUpdater)
	if !ok {
		err := fmt.Errorf("backing manifest (%s) does not support garbage collection", j.backing.Name())
		return manifestContents{}, err
	}

	latest, err := updater.UpdateGCGen(ctx, lastLock, next, stats, writeHook)
	if err != nil {
		return manifestContents{}, err
	} else if latest.root == next.root {
		j.contents = next // success
	}
	return latest, nil
}

// ParseIfExists implements manifest.
func (j *chunkJournal) ParseIfExists(ctx context.Context, stats *Stats, readHook func() error) (ok bool, mc manifestContents, err error) {
	if j.wr == nil {
		// parse contents from |j.backing| if the journal is not initialized
		return j.backing.ParseIfExists(ctx, stats, readHook)
	}
	if readHook != nil {
		if err = readHook(); err != nil {
			return false, manifestContents{}, err
		}
	}
	ok, mc = true, j.contents
	return
}

func (j *chunkJournal) maybeInit(ctx context.Context) (err error) {
	if j.wr == nil {
		err = j.openJournal(ctx)
	}
	return
}

// Close implements io.Closer
func (j *chunkJournal) Close() (err error) {
	ctx := context.Background()
	_, last, cerr := j.backing.ParseIfExists(ctx, &Stats{}, nil)
	if cerr != nil {
		err = cerr
	} else if !emptyAddr(j.contents.lock) {
		// best effort update to |backing|, this will
		// fail if the database has been deleted.
		// if we spuriously fail, we'll update |backing|
		// next time we open this chunkJournal
		_, _ = j.backing.Update(ctx, last.lock, j.contents, &Stats{}, nil)
	}
	if j.wr != nil {
		err = j.wr.Close()
	}
	return
}

func (j *chunkJournal) rotateJournalFile(ctx context.Context) (chunkSource, error) {
	var (
		spec    tableSpec
		wr      *journalWriter
		tmpPath string
	)

	eg, ectx := errgroup.WithContext(ctx)
	eg.Go(func() (err error) {
		dir := filepath.Dir(j.path)
		spec, err = convertJournalToTable(ectx, dir)
		return
	})
	eg.Go(func() (err error) {
		tmpPath = tmpJournalPath()
		wr, err = createJournalWriter(ectx, tmpPath)
		if err != nil {
			return err
		}
		// write current root hash to new journal
		if err = wr.WriteRootHash(j.contents.root); err != nil {
			return err
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	contents, err := addTableToManifest(ctx, j.backing, j.contents, spec)
	if err != nil {
		return nil, err
	}
	j.contents = contents

	// clobber the existing journal
	if err = os.Rename(tmpPath, j.path); err != nil {
		return nil, err
	}

	j.src = journalChunkSource{
		journal: wr,
		address: journalAddr,
		lookups: newLookupMap(),
	}
	j.wr = wr

	// return a chunkSource from the table file
	return j.persister.Open(ctx, spec.name, spec.chunkCount, new(Stats))
}

func convertJournalToTable(ctx context.Context, nbsPath string) (spec tableSpec, err error) {
	if nbsPath, err = filepath.Abs(nbsPath); err != nil {
		return tableSpec{}, err
	}

	j, err := os.Open(filepath.Join(nbsPath, chunkJournalName))
	if err != nil {
		return tableSpec{}, err
	}

	tmp, err := tempfiles.MovableTempFileProvider.NewFile(nbsPath, tempTablePrefix)
	if err != nil {
		return tableSpec{}, err
	}
	defer func() {
		if cerr := tmp.Close(); cerr != nil {
			err = cerr
		}
	}()

	wr := bufio.NewWriterSize(tmp, 65536)
	spec, err = writeJournalToTable(ctx, j, wr)
	if err != nil {
		return tableSpec{}, err
	}

	if err = wr.Flush(); err != nil {
		return tableSpec{}, err
	} else if err = tmp.Sync(); err != nil {
		return tableSpec{}, err
	}

	tp := filepath.Join(nbsPath, spec.name.String())
	if err = os.Rename(tmp.Name(), tp); err != nil {
		return tableSpec{}, nil
	}
	return
}

func addTableToManifest(ctx context.Context, m manifest, mc manifestContents, spec tableSpec) (manifestContents, error) {
	update := mc
	update.specs = append([]tableSpec{spec}, update.specs...)
	update.lock = generateLockHash(update.root, update.specs, update.appendix)
	return m.Update(ctx, mc.lock, update, new(Stats), nil)
}

func tmpJournalPath() string {
	d := tempfiles.MovableTempFileProvider.GetTempDir()
	f := tmpJournalPrefix + uuid.New().String()
	return filepath.Join(d, f)
}
