package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/oz/view-counter/internal/testdb"
)

var ctx = context.Background()

func newStore(t *testing.T) (*Store, *testdb.DB) {
	t.Helper()
	db := testdb.New(t)
	s, err := New(ctx, db.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s, db
}

func fp(s string) []byte { return []byte("fp-" + s) }

func mustHit(t *testing.T, s *Store, key, viewer string) bool {
	t.Helper()
	counted, err := s.Hit(ctx, key, fp(viewer))
	if err != nil {
		t.Fatal(err)
	}
	return counted
}

func mustTotal(t *testing.T, s *Store, key string) int64 {
	t.Helper()
	n, err := s.Total(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestHitCountsOncePerViewerAndKey(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)

	if !mustHit(t, s, "vid1", "alice") {
		t.Fatal("first hit should count")
	}
	if mustHit(t, s, "vid1", "alice") {
		t.Fatal("repeat hit should not count")
	}
	if got := mustTotal(t, s, "vid1"); got != 1 {
		t.Fatalf("total = %d, want 1", got)
	}
}

func TestDifferentViewersAndKeysCountSeparately(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)

	for _, h := range [][2]string{{"vid1", "alice"}, {"vid1", "bob"}, {"vid2", "alice"}} {
		if !mustHit(t, s, h[0], h[1]) {
			t.Fatalf("hit %v should count", h)
		}
	}
	if got := mustTotal(t, s, "vid1"); got != 2 {
		t.Fatalf("vid1 total = %d, want 2", got)
	}
	if got := mustTotal(t, s, "vid2"); got != 1 {
		t.Fatalf("vid2 total = %d, want 1", got)
	}
}

func TestSevenDayWindow(t *testing.T) {
	t.Parallel()
	s, db := newStore(t)
	mustHit(t, s, "vid1", "alice")

	backdate := func(age string) {
		t.Helper()
		_, err := db.Pool.Exec(ctx, `UPDATE seen SET seen_at = now() - $1::interval`, age)
		if err != nil {
			t.Fatal(err)
		}
	}

	backdate("6 days")
	if mustHit(t, s, "vid1", "alice") {
		t.Fatal("hit inside the window should not count")
	}

	// The duplicate must not have refreshed seen_at: still about 6 days old.
	var age time.Duration
	err := db.Pool.QueryRow(ctx, `SELECT now() - seen_at FROM seen`).Scan(&age)
	if err != nil {
		t.Fatal(err)
	}
	if age < 5*24*time.Hour {
		t.Fatalf("duplicate extended the window: seen_at age = %v", age)
	}

	backdate("8 days")
	if !mustHit(t, s, "vid1", "alice") {
		t.Fatal("hit after the window should count again")
	}
	if got := mustTotal(t, s, "vid1"); got != 2 {
		t.Fatalf("total = %d, want 2", got)
	}

	// Counting again restarts the window.
	if mustHit(t, s, "vid1", "alice") {
		t.Fatal("hit right after a counted hit should not count")
	}
}

func TestHitIsAtomic(t *testing.T) {
	t.Parallel()
	s, db := newStore(t)

	_, err := db.Pool.Exec(ctx, `
		CREATE FUNCTION fail_hit() RETURNS trigger AS $$
		BEGIN RAISE EXCEPTION 'boom'; END $$ LANGUAGE plpgsql;
		CREATE TRIGGER fail_hit BEFORE INSERT OR UPDATE ON page_views
			FOR EACH ROW EXECUTE FUNCTION fail_hit();`)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.Hit(ctx, "vid1", fp("alice")); err == nil {
		t.Fatal("expected the hit to fail")
	}
	var n int
	db.Pool.QueryRow(ctx, `SELECT count(*) FROM seen`).Scan(&n)
	if n != 0 {
		t.Fatalf("seen has %d rows after a failed hit, want 0", n)
	}

	// Once the failure is gone, the caller's retry counts.
	if _, err := db.Pool.Exec(ctx, `DROP TRIGGER fail_hit ON page_views`); err != nil {
		t.Fatal(err)
	}
	if !mustHit(t, s, "vid1", "alice") {
		t.Fatal("retry should count")
	}
}

func TestConcurrentUniqueHitsAreAllCounted(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)

	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Hit(ctx, "vid1", fp(fmt.Sprint(i))); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if got := mustTotal(t, s, "vid1"); got != n {
		t.Fatalf("total = %d, want %d", got, n)
	}
}

func TestConcurrentDuplicateHitsCountOnce(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)

	const n = 50
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		counted int
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := s.Hit(ctx, "vid1", fp("alice"))
			if err != nil {
				t.Error(err)
			}
			if ok {
				mu.Lock()
				counted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if counted != 1 {
		t.Fatalf("%d concurrent duplicates counted, want exactly 1", counted)
	}
	if got := mustTotal(t, s, "vid1"); got != 1 {
		t.Fatalf("total = %d, want 1", got)
	}
}

func TestTotalOfUnknownKeyIsZero(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	if got := mustTotal(t, s, "nope"); got != 0 {
		t.Fatalf("total = %d, want 0", got)
	}
}

func TestTotalSumsAcrossDays(t *testing.T) {
	t.Parallel()
	s, db := newStore(t)
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO page_views (key, day, views) VALUES
			('vid1', current_date, 3),
			('vid1', current_date - 1, 4),
			('vid2', current_date, 100);
		INSERT INTO page_totals (key, views) VALUES ('vid1', 7), ('vid2', 100)`)
	if err != nil {
		t.Fatal(err)
	}
	if got := mustTotal(t, s, "vid1"); got != 7 {
		t.Fatalf("total = %d, want 7", got)
	}
}

// assertTotalsMatchDays fails if page_totals disagrees with the sum of
// page_views for any key, in either direction.
func assertTotalsMatchDays(t *testing.T, db *testdb.DB) {
	t.Helper()
	var bad int
	err := db.Pool.QueryRow(ctx, `
		SELECT count(*) FROM (
			SELECT key, SUM(views) AS v FROM page_views GROUP BY key
		) d FULL JOIN page_totals t USING (key)
		WHERE d.v IS DISTINCT FROM t.views`).Scan(&bad)
	if err != nil {
		t.Fatal(err)
	}
	if bad != 0 {
		t.Fatalf("%d keys where page_totals differs from the sum of page_views", bad)
	}
}

func TestTotalsStayInStepWithDailyViews(t *testing.T) {
	t.Parallel()
	s, db := newStore(t)

	// Single hits, including a duplicate that must not count.
	for _, v := range []string{"a", "b", "a"} {
		mustHit(t, s, "vid1", v)
	}
	mustHit(t, s, "vid2", "a")

	// A batch spanning two days and two keys, with a repeat of a viewer
	// already counted by Hit.
	now := time.Now().UTC()
	yesterday := now.AddDate(0, 0, -1)
	err := s.HitBatch(ctx, []Hit{
		{"vid1", [16]byte{1}, now}, {"vid1", [16]byte{2}, now},
		{"vid1", [16]byte{3}, yesterday}, {"vid3", [16]byte{1}, yesterday},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.HitBatch(ctx, []Hit{{"vid1", [16]byte{1}, now}}); err != nil {
		t.Fatal(err)
	}

	assertTotalsMatchDays(t, db)
	if got := mustTotal(t, s, "vid1"); got != 5 {
		t.Fatalf("vid1 total = %d, want 5", got)
	}
	if got := mustTotal(t, s, "vid3"); got != 1 {
		t.Fatalf("vid3 total = %d, want 1", got)
	}
}

func TestDailyFillsMissingDaysWithZero(t *testing.T) {
	t.Parallel()
	s, db := newStore(t)
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO page_views (key, day, views) VALUES
			('vid1', '2026-01-02', 5),
			('vid1', '2026-01-04', 7),
			('vid2', '2026-01-03', 99)`)
	if err != nil {
		t.Fatal(err)
	}

	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	got, err := s.Daily(ctx, "vid1", from, to)
	if err != nil {
		t.Fatal(err)
	}
	want := []DayViews{
		{"2026-01-01", 0}, {"2026-01-02", 5}, {"2026-01-03", 0},
		{"2026-01-04", 7}, {"2026-01-05", 0},
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("daily = %v, want %v", got, want)
	}
}

func TestDailySingleDay(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	d := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	got, err := s.Daily(ctx, "vid1", d, d)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Views != 0 {
		t.Fatalf("daily = %v, want one zero day", got)
	}
}

func TestTopOrdersByViewsThenKeyAndHonoursLimit(t *testing.T) {
	t.Parallel()
	s, db := newStore(t)
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO page_views (key, day, views) VALUES
			('b', current_date, 5), ('b', current_date - 1, 5),
			('a', current_date, 10),
			('c', current_date, 3),
			('d', current_date, 50);
		INSERT INTO page_totals (key, views) VALUES ('a', 10), ('b', 10), ('c', 3), ('d', 50)`)
	if err != nil {
		t.Fatal(err)
	}

	got, err := s.Top(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	want := []KeyViews{{"d", 50}, {"a", 10}, {"b", 10}}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("top = %v, want %v", got, want)
	}
}

func TestTopOnEmptyTableIsEmptyNotNil(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	got, err := s.Top(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("top = %#v, want empty non-nil slice", got)
	}
}

func TestPruneSeenDeletesOnlyExpiredRows(t *testing.T) {
	t.Parallel()
	s, db := newStore(t)
	mustHit(t, s, "vid1", "old")
	mustHit(t, s, "vid1", "recent")
	if _, err := db.Pool.Exec(ctx,
		`UPDATE seen SET seen_at = now() - interval '8 days' WHERE fp_hash = $1`, fp("old")); err != nil {
		t.Fatal(err)
	}

	if err := s.PruneSeen(ctx); err != nil {
		t.Fatal(err)
	}

	var n int
	db.Pool.QueryRow(ctx, `SELECT count(*) FROM seen`).Scan(&n)
	if n != 1 {
		t.Fatalf("seen has %d rows, want 1", n)
	}
	if mustHit(t, s, "vid1", "recent") {
		t.Fatal("recent viewer should still be deduped after prune")
	}
	if got := mustTotal(t, s, "vid1"); got != 2 {
		t.Fatalf("prune must not change counts: total = %d, want 2", got)
	}
}

func fpArr(s string) [16]byte {
	var a [16]byte
	copy(a[:], s)
	return a
}

func TestHitBatchDedupesWithinAndAcrossBatches(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	now := time.Now()

	batch := []Hit{
		{"vid1", fpArr("alice"), now},
		{"vid1", fpArr("alice"), now}, // repeat inside the batch
		{"vid1", fpArr("bob"), now},
		{"vid2", fpArr("alice"), now}, // same viewer, other key
	}
	if err := s.HitBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if got := mustTotal(t, s, "vid1"); got != 2 {
		t.Fatalf("vid1 = %d, want 2", got)
	}
	if got := mustTotal(t, s, "vid2"); got != 1 {
		t.Fatalf("vid2 = %d, want 1", got)
	}

	// Replaying the same batch (a redelivery) must count nothing.
	if err := s.HitBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	if got := mustTotal(t, s, "vid1"); got != 2 {
		t.Fatalf("vid1 after replay = %d, want 2", got)
	}
}

func TestHitBatchEmptyIsNoop(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	if err := s.HitBatch(ctx, nil); err != nil {
		t.Fatal(err)
	}
}

func TestHitBatchCountsOnTheDayOfTheHit(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	d1 := time.Date(2026, 3, 1, 23, 59, 0, 0, time.UTC)
	d2 := d1.Add(2 * time.Minute) // 2026-03-02

	err := s.HitBatch(ctx, []Hit{
		{"vid1", fpArr("a"), d1},
		{"vid1", fpArr("b"), d2},
		{"vid1", fpArr("c"), d2},
	})
	if err != nil {
		t.Fatal(err)
	}
	days, err := s.Daily(ctx, "vid1", d1, d2)
	if err != nil {
		t.Fatal(err)
	}
	want := []DayViews{{"2026-03-01", 1}, {"2026-03-02", 2}}
	if fmt.Sprint(days) != fmt.Sprint(want) {
		t.Fatalf("days = %v, want %v", days, want)
	}
}

func TestHitBatchSevenDayWindow(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	t0 := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	for _, c := range []struct {
		at   time.Time
		want int64
	}{
		{t0, 1},
		{t0.Add(6 * 24 * time.Hour), 1}, // inside the window: not counted
		{t0.Add(8 * 24 * time.Hour), 2}, // outside: counted again
	} {
		if err := s.HitBatch(ctx, []Hit{{"vid1", fpArr("alice"), c.at}}); err != nil {
			t.Fatal(err)
		}
		if got := mustTotal(t, s, "vid1"); got != c.want {
			t.Fatalf("at %v total = %d, want %d", c.at, got, c.want)
		}
	}
}

func TestConcurrentBatchesCountEachViewerOnce(t *testing.T) {
	t.Parallel()
	s, _ := newStore(t)
	now := time.Now()

	// Overlapping batches from several writers, all on one hot key.
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var b []Hit
			for i := 0; i < 50; i++ {
				b = append(b, Hit{"hot", fpArr(fmt.Sprint("v", i)), now})
			}
			if err := s.HitBatch(ctx, b); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if got := mustTotal(t, s, "hot"); got != 50 {
		t.Fatalf("total = %d, want 50", got)
	}
}
