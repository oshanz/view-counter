package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/oz/view-counter/internal/store"
	"github.com/oz/view-counter/internal/testdb"
)

var secret = []byte("test-secret")

type env struct {
	h  http.Handler
	db *testdb.DB
	st *store.Store
}

func newEnv(t *testing.T) *env {
	t.Helper()
	db := testdb.New(t)
	st, err := store.New(context.Background(), db.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	return &env{h: New(st, secret), db: db, st: st}
}

func (e *env) do(method, target, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	e.h.ServeHTTP(rec, req)
	return rec
}

func (e *env) hit(key, fingerprint string) *httptest.ResponseRecorder {
	b, _ := json.Marshal(map[string]string{"key": key, "fingerprint": fingerprint})
	return e.do("POST", "/v1/hit", string(b))
}

func wantStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("status = %d, want %d (body %q)", rec.Code, want, rec.Body.String())
	}
}

func decode(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q, want application/json", ct)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
}

func total(t *testing.T, e *env, key string) int64 {
	t.Helper()
	rec := e.do("GET", "/v1/views?key="+key, "")
	wantStatus(t, rec, 200)
	var out struct{ Views int64 }
	decode(t, rec, &out)
	return out.Views
}

func TestHitReturns204AndCounts(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	wantStatus(t, e.hit("vid1", "alice"), 204)
	if got := total(t, e, "vid1"); got != 1 {
		t.Fatalf("total = %d, want 1", got)
	}
}

func TestDuplicateHitReturns204ButDoesNotCount(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	wantStatus(t, e.hit("vid1", "alice"), 204)
	wantStatus(t, e.hit("vid1", "alice"), 204)
	wantStatus(t, e.hit("vid1", "bob"), 204)
	if got := total(t, e, "vid1"); got != 2 {
		t.Fatalf("total = %d, want 2", got)
	}
}

func TestHitValidation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	cases := map[string]string{
		"malformed json":   `{"key":`,
		"empty body":       ``,
		"missing key":      `{"fingerprint":"x"}`,
		"missing fp":       `{"key":"vid1"}`,
		"empty fp":         `{"key":"vid1","fingerprint":""}`,
		"key with space":   `{"key":"a b","fingerprint":"x"}`,
		"key with slash":   `{"key":"a/b","fingerprint":"x"}`,
		"key with sql":     `{"key":"a';--","fingerprint":"x"}`,
		"key too long":     `{"key":"` + strings.Repeat("a", 65) + `","fingerprint":"x"}`,
		"fingerprint long": `{"key":"vid1","fingerprint":"` + strings.Repeat("a", 257) + `"}`,
		"body over 4KB":    `{"key":"vid1","fingerprint":"x","pad":"` + strings.Repeat("a", 5000) + `"}`,
		"wrong types":      `{"key":1,"fingerprint":2}`,
		"json array":       `[]`,
	}
	for name, body := range cases {
		rec := e.do("POST", "/v1/hit", body)
		if rec.Code != 400 {
			t.Errorf("%s: status = %d, want 400", name, rec.Code)
		}
	}
	var n int
	e.db.Pool.QueryRow(context.Background(), `SELECT count(*) FROM seen`).Scan(&n)
	if n != 0 {
		t.Fatalf("rejected hits left %d seen rows", n)
	}
}

func TestHitAcceptsBoundaryValues(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	wantStatus(t, e.hit(strings.Repeat("a", 64), strings.Repeat("f", 256)), 204)
	wantStatus(t, e.hit("A-z_0-9", "x"), 204)
	wantStatus(t, e.hit("dQw4w9WgXcQ", "x"), 204)
}

func TestWrongMethodsAndUnknownRoutes(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	wantStatus(t, e.do("GET", "/v1/hit", ""), 405)
	wantStatus(t, e.do("POST", "/v1/views?key=vid1", ""), 405)
	wantStatus(t, e.do("POST", "/v1/top", ""), 405)
	wantStatus(t, e.do("GET", "/v1/nope", ""), 404)
}

func TestFingerprintIsHashedBeforeStorage(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	const raw = "alice-device-1234"
	wantStatus(t, e.hit("vid1", raw), 204)

	var fp []byte
	e.db.Pool.QueryRow(context.Background(), `SELECT fp_hash FROM seen`).Scan(&fp)
	if len(fp) != 16 {
		t.Fatalf("fp_hash length = %d, want 16", len(fp))
	}
	if strings.Contains(string(fp), raw) {
		t.Fatal("raw fingerprint stored")
	}

	var n int
	e.db.Pool.QueryRow(context.Background(),
		`SELECT count(*) FROM seen WHERE fp_hash = $1`, []byte(raw)).Scan(&n)
	if n != 0 {
		t.Fatal("raw fingerprint found in seen")
	}
}

func TestDedupeDependsOnSecret(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	wantStatus(t, e.hit("vid1", "alice"), 204)

	// Rotating the secret resets dedupe.
	rotated := New(e.st, []byte("another-secret"))
	rec := httptest.NewRecorder()
	rotated.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/hit",
		strings.NewReader(`{"key":"vid1","fingerprint":"alice"}`)))
	wantStatus(t, rec, 204)
	if got := total(t, e, "vid1"); got != 2 {
		t.Fatalf("total = %d, want 2 after secret rotation", got)
	}
}

func TestViewsTotal(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.hit("vid1", "a")
	e.hit("vid1", "b")

	rec := e.do("GET", "/v1/views?key=vid1", "")
	wantStatus(t, rec, 200)
	var out struct {
		Key   string
		Views int64
	}
	decode(t, rec, &out)
	if out.Key != "vid1" || out.Views != 2 {
		t.Fatalf("got %+v, want vid1 with 2 views", out)
	}
	if got := total(t, e, "unknown"); got != 0 {
		t.Fatalf("unknown key total = %d, want 0", got)
	}
}

func TestViewsValidation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	bad := []string{
		"/v1/views",
		"/v1/views?key=",
		"/v1/views?key=a%20b",
		"/v1/views?key=vid1&from=2026-01-01",
		"/v1/views?key=vid1&to=2026-01-01",
		"/v1/views?key=vid1&from=nope&to=2026-01-01",
		"/v1/views?key=vid1&from=2026-01-01&to=01-02-2026",
		"/v1/views?key=vid1&from=2026-01-05&to=2026-01-01",
		"/v1/views?key=vid1&from=2025-01-01&to=2026-12-31",
	}
	for _, u := range bad {
		if rec := e.do("GET", u, ""); rec.Code != 400 {
			t.Errorf("%s: status = %d, want 400", u, rec.Code)
		}
	}
}

func TestViewsSeries(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	_, err := e.db.Pool.Exec(context.Background(), `
		INSERT INTO page_views (key, day, views) VALUES
			('vid1', '2026-01-02', 5), ('vid1', '2026-01-04', 7)`)
	if err != nil {
		t.Fatal(err)
	}

	rec := e.do("GET", "/v1/views?key=vid1&from=2026-01-01&to=2026-01-05", "")
	wantStatus(t, rec, 200)
	var out struct {
		Key, From, To string
		Days          []store.DayViews
	}
	decode(t, rec, &out)
	if out.Key != "vid1" || out.From != "2026-01-01" || out.To != "2026-01-05" {
		t.Fatalf("unexpected envelope: %+v", out)
	}
	wantViews := []int64{0, 5, 0, 7, 0}
	if len(out.Days) != len(wantViews) {
		t.Fatalf("got %d days, want %d", len(out.Days), len(wantViews))
	}
	for i, d := range out.Days {
		if d.Views != wantViews[i] {
			t.Errorf("day %s = %d, want %d", d.Day, d.Views, wantViews[i])
		}
	}
}

func TestViewsSeriesRangeLimits(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// 366 days apart is the maximum allowed; one more is rejected.
	ok := from.AddDate(0, 0, 366).Format(time.DateOnly)
	tooLong := from.AddDate(0, 0, 367).Format(time.DateOnly)
	wantStatus(t, e.do("GET", "/v1/views?key=vid1&from=2026-01-01&to="+ok, ""), 200)
	wantStatus(t, e.do("GET", "/v1/views?key=vid1&from=2026-01-01&to="+tooLong, ""), 400)

	// from == to is a valid single-day range.
	rec := e.do("GET", "/v1/views?key=vid1&from=2026-01-01&to=2026-01-01", "")
	wantStatus(t, rec, 200)
	var out struct{ Days []store.DayViews }
	decode(t, rec, &out)
	if len(out.Days) != 1 {
		t.Fatalf("got %d days, want 1", len(out.Days))
	}
}

func TestTop(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for i, key := range []string{"low", "mid", "high"} {
		for v := 0; v <= i*2; v++ {
			e.hit(key, "viewer"+string(rune('a'+v)))
		}
	}

	rec := e.do("GET", "/v1/top", "")
	wantStatus(t, rec, 200)
	var out struct{ Top []store.KeyViews }
	decode(t, rec, &out)
	want := []store.KeyViews{{Key: "high", Views: 5}, {Key: "mid", Views: 3}, {Key: "low", Views: 1}}
	if len(out.Top) != 3 {
		t.Fatalf("top = %v, want %v", out.Top, want)
	}
	for i := range want {
		if out.Top[i] != want[i] {
			t.Fatalf("top = %v, want %v", out.Top, want)
		}
	}

	rec = e.do("GET", "/v1/top?n=1", "")
	wantStatus(t, rec, 200)
	decode(t, rec, &out)
	if len(out.Top) != 1 || out.Top[0].Key != "high" {
		t.Fatalf("top n=1 = %v", out.Top)
	}
}

func TestTopEmptyIsEmptyArray(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	rec := e.do("GET", "/v1/top", "")
	wantStatus(t, rec, 200)
	if got := strings.TrimSpace(rec.Body.String()); got != `{"top":[]}` {
		t.Fatalf("body = %s, want {\"top\":[]}", got)
	}
}

func TestTopLimitValidation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	for _, n := range []string{"0", "-1", "101", "abc", "1.5"} {
		if rec := e.do("GET", "/v1/top?n="+n, ""); rec.Code != 400 {
			t.Errorf("n=%s: status = %d, want 400", n, rec.Code)
		}
	}
	for _, n := range []string{"1", "100"} {
		wantStatus(t, e.do("GET", "/v1/top?n="+n, ""), 200)
	}
}

func TestDatabaseDownReturns503(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.st.Close()

	wantStatus(t, e.hit("vid1", "alice"), 503)
	wantStatus(t, e.do("GET", "/v1/views?key=vid1", ""), 503)
	wantStatus(t, e.do("GET", "/v1/views?key=vid1&from=2026-01-01&to=2026-01-02", ""), 503)
	wantStatus(t, e.do("GET", "/v1/top", ""), 503)
}

func TestErrorResponsesAreJSON(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	rec := e.do("POST", "/v1/hit", `{`)
	wantStatus(t, rec, 400)
	var out map[string]string
	decode(t, rec, &out)
	if out["error"] == "" {
		t.Fatalf("missing error message: %s", rec.Body.String())
	}
}
