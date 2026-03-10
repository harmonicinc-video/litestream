package litestream_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"slices"
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
