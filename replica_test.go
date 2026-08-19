package litestream_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/harmonicinc-video/litestream"
	"github.com/harmonicinc-video/litestream/file"
	"github.com/harmonicinc-video/litestream/mock"
	"github.com/pierrec/lz4/v4"
)

func TestReplica_Name(t *testing.T) {
	t.Run("WithName", func(t *testing.T) {
		if got, want := litestream.NewReplica(nil, "NAME").Name(), "NAME"; got != want {
			t.Fatalf("Name()=%v, want %v", got, want)
		}
	})
	t.Run("WithoutName", func(t *testing.T) {
		r := litestream.NewReplica(nil, "")
		r.Client = &mock.ReplicaClient{}
		if got, want := r.Name(), "mock"; got != want {
			t.Fatalf("Name()=%v, want %v", got, want)
		}
	})
}

func TestReplica_Sync(t *testing.T) {
	db, sqldb := MustOpenDBs(t)
	defer MustCloseDBs(t, db, sqldb)

	// Execute a query to force a write to the WAL.
	if _, err := sqldb.Exec(`CREATE TABLE foo (bar TEXT);`); err != nil {
		t.Fatal(err)
	}

	// Issue initial database sync to setup generation.
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Fetch current database position.
	dpos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}

	c := file.NewReplicaClient(t.TempDir())
	r := litestream.NewReplica(db, "")
	c.Replica, r.Client = r, c

	if err := r.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Verify client generation matches database.
	generations, err := c.Generations(context.Background())
	if err != nil {
		t.Fatal(err)
	} else if got, want := len(generations), 1; got != want {
		t.Fatalf("len(generations)=%v, want %v", got, want)
	} else if got, want := generations[0], dpos.Generation; got != want {
		t.Fatalf("generations[0]=%v, want %v", got, want)
	}

	// Verify WAL matches replica WAL.
	if b0, err := os.ReadFile(db.Path() + "-wal"); err != nil {
		t.Fatal(err)
	} else if r, err := c.WALSegmentReader(context.Background(), litestream.Pos{Generation: generations[0], Index: 0, Offset: 0}); err != nil {
		t.Fatal(err)
	} else if b1, err := io.ReadAll(lz4.NewReader(r)); err != nil {
		t.Fatal(err)
	} else if err := r.Close(); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(b0, b1) {
		t.Fatalf("wal mismatch: len(%d), len(%d)", len(b0), len(b1))
	}
}

func TestReplica_Snapshot(t *testing.T) {
	db, sqldb := MustOpenDBs(t)
	defer MustCloseDBs(t, db, sqldb)

	c := file.NewReplicaClient(t.TempDir())
	r := litestream.NewReplica(db, "")
	r.Client = c

	// Execute a query to force a write to the WAL.
	if _, err := sqldb.Exec(`CREATE TABLE foo (bar TEXT);`); err != nil {
		t.Fatal(err)
	} else if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	} else if err := r.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Fetch current database position & snapshot.
	pos0, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	} else if info, err := r.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	} else if got, want := info.Pos(), pos0.Truncate(); got != want {
		t.Fatalf("pos=%s, want %s", got, want)
	}

	// Sync database and then replica.
	if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	} else if err := r.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Execute a query to force a write to the WAL & truncate to start new index.
	if _, err := sqldb.Exec(`INSERT INTO foo (bar) VALUES ('baz');`); err != nil {
		t.Fatal(err)
	} else if err := db.Checkpoint(context.Background(), litestream.CheckpointModeTruncate); err != nil {
		t.Fatal(err)
	}

	// Fetch current database position & snapshot.
	pos1, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	} else if info, err := r.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	} else if got, want := info.Pos(), pos1.Truncate(); got != want {
		t.Fatalf("pos=%v, want %v", got, want)
	}

	// Verify two snapshots exist.
	if infos, err := r.Snapshots(context.Background()); err != nil {
		t.Fatal(err)
	} else if got, want := len(infos), 2; got != want {
		t.Fatalf("len=%v, want %v", got, want)
	} else if got, want := infos[0].Pos(), pos0.Truncate(); got != want {
		t.Fatalf("info[0]=%s, want %s", got, want)
	} else if got, want := infos[1].Pos(), pos1.Truncate(); got != want {
		t.Fatalf("info[1]=%s, want %s", got, want)
	}
}

// mustWriteStubSnapshot creates a stub snapshot file for the given generation
// and index under the file client's directory, then sets its mtime to the
// given time. No DB or Replica involvement — purely filesystem setup.
func mustWriteStubSnapshot(tb testing.TB, c *file.ReplicaClient, generation string, index int, mtime time.Time) {
	tb.Helper()
	snapshotPath, err := c.SnapshotPath(generation, index)
	if err != nil {
		tb.Fatalf("snapshot path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(snapshotPath), 0o755); err != nil {
		tb.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(snapshotPath, []byte("stub"), 0o644); err != nil {
		tb.Fatalf("write stub snapshot: %v", err)
	}
	if err := os.Chtimes(snapshotPath, mtime, mtime); err != nil {
		tb.Fatalf("chtimes: %v", err)
	}
}

// TestReplica_Retainer_PrunesExpiredKeepsRecent verifies that retainer()
// enforces retention at startup before any ticker fires.
//
// Three generations have all-expired snapshots; one has a recent snapshot
// inside the retention window. RetentionCheckInterval is set to 24h so the
// ticker never fires during the test — any pruning must come from the startup
// call.
func TestReplica_Retainer_PrunesExpiredKeepsRecent(t *testing.T) {
	const retention = 1 * time.Minute

	now := time.Now()
	expired := now.Add(-2 * retention) // clearly outside retention
	recent := now.Add(-retention / 2)  // clearly inside retention

	c := file.NewReplicaClient(t.TempDir())

	// Three expired generations — must be deleted by startup enforcement.
	expiredGens := []string{"aaaa000000000001", "aaaa000000000002", "aaaa000000000003"}
	for _, gen := range expiredGens {
		mustWriteStubSnapshot(t, c, gen, 0, expired)
	}

	// One recent generation — must survive.
	recentGen := "bbbb000000000001"
	mustWriteStubSnapshot(t, c, recentGen, 0, recent)

	r := litestream.NewReplica(nil, "")
	r.Client = c
	r.Retention = retention
	r.RetentionCheckInterval = 24 * time.Hour // ticker never fires during test

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.ReplicaRetainer(ctx)
	}()

	// Poll until the startup enforcement has pruned all expired generations,
	// rather than relying on a fixed sleep that can race under load.
	deadline := time.Now().Add(2 * time.Second)
	for {
		remaining, err := c.Generations(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !slices.ContainsFunc(remaining, func(g string) bool { return slices.Contains(expiredGens, g) }) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for retainer startup enforcement to prune expired generations")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	// Exactly the recent generation must survive.
	remaining, err := c.Generations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(remaining), 1; got != want {
		t.Fatalf("len(generations)=%d, want %d; generations=%v", got, want, remaining)
	}
	if got, want := remaining[0], recentGen; got != want {
		t.Fatalf("surviving generation=%s, want %s", got, want)
	}
}

// TestReplica_EnforceRetention_SkipsCurrentGeneration ensures the active
// generation is never pruned while older expired generations are cleaned up.
// This protects the live replica state from being deleted during retention.
func TestReplica_EnforceRetention_SkipsCurrentGeneration(t *testing.T) {
	const retention = 1 * time.Minute

	db, sqldb := MustOpenDBs(t)
	defer MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.Exec(`CREATE TABLE foo (bar TEXT);`); err != nil {
		t.Fatal(err)
	} else if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	pos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	activeGen := pos.Generation

	now := time.Now()
	expired := now.Add(-2 * retention)
	recent := now.Add(-retention / 2)

	c := file.NewReplicaClient(t.TempDir())
	mustWriteStubSnapshot(t, c, "aaaa000000000001", 0, expired)
	mustWriteStubSnapshot(t, c, "bbbb000000000001", 0, recent)

	activeDir, err := c.GenerationDir(activeGen)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(activeDir, 0o755); err != nil {
		t.Fatal(err)
	}

	r := litestream.NewReplica(db, "")
	r.Client = c
	r.Retention = retention

	if err := r.EnforceRetention(context.Background()); err != nil {
		t.Fatal(err)
	}

	generations, err := c.Generations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(generations, "aaaa000000000001") {
		t.Fatalf("expired generation was not deleted: %v", generations)
	}
	if !slices.Contains(generations, "bbbb000000000001") {
		t.Fatalf("recent generation was deleted: %v", generations)
	}
	if !slices.Contains(generations, activeGen) {
		t.Fatalf("current generation was deleted: %v", generations)
	}
	if got, want := len(generations), 2; got != want {
		t.Fatalf("len(generations)=%d, want %d; generations=%v", got, want, generations)
	}
}

// delayedReplicaClient adds a delay to Generations() and triggers a hook
// after the first call so the test can create a new generation mid-retention.
type delayedReplicaClient struct {
	litestream.ReplicaClient
	generationsDelay time.Duration

	mu                    sync.Mutex
	generationsCalled     bool
	afterFirstGenerations func()
}

func (c *delayedReplicaClient) Generations(ctx context.Context) ([]string, error) {
	time.Sleep(c.generationsDelay)
	result, err := c.ReplicaClient.Generations(ctx)

	c.mu.Lock()
	first := !c.generationsCalled
	c.generationsCalled = true
	hook := c.afterFirstGenerations
	c.mu.Unlock()

	if first && hook != nil {
		hook()
	}
	return result, err
}

// TestReplica_EnforceRetention_NoTOCTOU ensures a generation created after the
// initial listing is not deleted because retention only works from that list.
func TestReplica_EnforceRetention_NoTOCTOU(t *testing.T) {
	const retention = 2 * time.Hour

	now := time.Now()
	recent := now.Add(-retention / 2)

	baseClient := file.NewReplicaClient(t.TempDir())

	// Seed two pre-existing in-retention generations.
	mustWriteStubSnapshot(t, baseClient, "aaaa000000000001", 0, recent)
	mustWriteStubSnapshot(t, baseClient, "aaaa000000000002", 0, recent)

	lateGen := "cccc000000000001"

	dc := &delayedReplicaClient{
		ReplicaClient:    baseClient,
		generationsDelay: 50 * time.Millisecond,
		afterFirstGenerations: func() {
			// Simulate monitor creating a new generation while retention is running.
			mustWriteStubSnapshot(t, baseClient, lateGen, 0, now)
		},
	}

	r := litestream.NewReplica(nil, "")
	r.Client = dc
	r.Retention = retention

	if err := r.EnforceRetention(context.Background()); err != nil {
		t.Fatal(err)
	}

	// The late generation must still exist because it was not in the
	// captured generation set.
	generations, err := baseClient.Generations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(generations, lateGen) {
		t.Fatalf("late generation %s was deleted; remaining=%v", lateGen, generations)
	}
	if !slices.Contains(generations, "aaaa000000000001") {
		t.Fatalf("in-retention generation aaaa000000000001 was deleted; remaining=%v", generations)
	}
	if !slices.Contains(generations, "aaaa000000000002") {
		t.Fatalf("in-retention generation aaaa000000000002 was deleted; remaining=%v", generations)
	}
}

// TestReplica_EnforceRetention_StartupRace covers the startup race where a
// monitor creates a generation while retention is running; the active one must
// survive.
func TestReplica_EnforceRetention_StartupRace(t *testing.T) {
	const retention = 2 * time.Hour

	db, sqldb := MustOpenDBs(t)
	defer MustCloseDBs(t, db, sqldb)

	if _, err := sqldb.Exec(`CREATE TABLE foo (bar TEXT);`); err != nil {
		t.Fatal(err)
	} else if err := db.Sync(context.Background()); err != nil {
		t.Fatal(err)
	}

	pos, err := db.Pos()
	if err != nil {
		t.Fatal(err)
	}
	activeGen := pos.Generation

	now := time.Now()
	recent := now.Add(-retention / 2)
	expired := now.Add(-2 * retention)

	c := file.NewReplicaClient(t.TempDir())

	mustWriteStubSnapshot(t, c, "aaaa000000000001", 0, expired)
	mustWriteStubSnapshot(t, c, "bbbb000000000001", 0, recent)

	activeDir, err := c.GenerationDir(activeGen)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(activeDir, 0o755); err != nil {
		t.Fatal(err)
	}

	r := litestream.NewReplica(db, "")
	r.Client = c
	r.Retention = retention

	// Run EnforceRetention multiple times to confirm idempotency.
	for i := 0; i < 5; i++ {
		if err := r.EnforceRetention(context.Background()); err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
	}

	generations, err := c.Generations(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if slices.Contains(generations, "aaaa000000000001") {
		t.Fatalf("expired generation was not deleted: %v", generations)
	}
	if !slices.Contains(generations, "bbbb000000000001") {
		t.Fatalf("in-retention generation was deleted: %v", generations)
	}
	if !slices.Contains(generations, activeGen) {
		t.Fatalf("active generation %s was deleted: %v", activeGen, generations)
	}
}

// toctouReplicaClient creates a new generation only after the first
// Generations() call returns, matching the TOCTOU race in the bug.
type toctouReplicaClient struct {
	litestream.ReplicaClient

	mu                sync.Mutex
	generationsCalled bool
	afterGenerations  func()
}

func (c *toctouReplicaClient) Generations(ctx context.Context) ([]string, error) {
	result, err := c.ReplicaClient.Generations(ctx)

	c.mu.Lock()
	first := !c.generationsCalled
	c.generationsCalled = true
	hook := c.afterGenerations
	c.mu.Unlock()

	if first && hook != nil {
		hook()
	}
	return result, err
}

// TestReplica_EnforceRetention_TOCTOU_Reproduction covers the original race:
// a new generation appears after the retention list is captured, and must not
// be deleted.
func TestReplica_EnforceRetention_TOCTOU_Reproduction(t *testing.T) {
	const retention = 2 * time.Hour

	now := time.Now()
	expired := now.Add(-2 * retention)
	recent := now.Add(-retention / 2)

	baseClient := file.NewReplicaClient(t.TempDir())

	// One expired generation that should be cleaned up.
	mustWriteStubSnapshot(t, baseClient, "aaaa000000000001", 0, expired)
	// One in-retention generation so EnforceRetention doesn't need to create
	// a new snapshot (which would require a real *DB).
	mustWriteStubSnapshot(t, baseClient, "bbbb000000000001", 0, recent)

	// The "late" generation is created only after EnforceRetention's single
	// Generations() call returns — it doesn't exist yet when retention
	// captures its snapshot set.
	lateGen := "dddd000000000001"

	tc := &toctouReplicaClient{
		ReplicaClient: baseClient,
		afterGenerations: func() {
			mustWriteStubSnapshot(t, baseClient, lateGen, 0, now)
		},
	}

	r := litestream.NewReplica(nil, "")
	r.Client = tc
	r.Retention = retention

	if err := r.EnforceRetention(context.Background()); err != nil {
		t.Fatal(err)
	}

	// The expired generation should be gone.
	generations, err := baseClient.Generations(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(generations, "aaaa000000000001") {
		t.Fatalf("expired generation was not deleted: %v", generations)
	}
	if !slices.Contains(generations, "bbbb000000000001") {
		t.Fatalf("in-retention generation was deleted: %v", generations)
	}

	// The late generation must survive — the fix ensures Generations() is
	// called only once, so the late generation is never in the delete scope.
	if !slices.Contains(generations, lateGen) {
		t.Fatalf("late generation %s was improperly deleted (TOCTOU race!): remaining=%v", lateGen, generations)
	}
}
