package gosqs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// *Record must keep satisfying the legacy interface.
var _ Message = (*Record)(nil)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// startBatch runs a consumer until cancel. Logger defaults to quiet; a test
// that wants the log passes its own.
func startBatch(t *testing.T, f *fakeSQS, h BatchHandler, o Options) (cancel func(), done <-chan struct{}) {
	t.Helper()
	if o.Logger == nil {
		o.Logger = quiet
	}
	if o.WaitTime == 0 {
		o.WaitTime = time.Second
	}
	c, err := New(f, "https://sqs.local/q", h, o)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancelCtx := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { _ = c.Run(ctx); close(finished) }()
	return cancelCtx, finished
}

func stopBatch(t *testing.T, cancel func(), done <-chan struct{}) {
	t.Helper()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// runBatch starts a consumer, waits until cond holds (or 3s), then shuts it down.
func runBatch(t *testing.T, f *fakeSQS, h BatchHandler, o Options, cond func() bool) {
	t.Helper()
	cancel, done := startBatch(t, f, h, o)
	deadline := time.Now().Add(3 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	stopBatch(t, cancel, done)
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.doubleDelete) > 0 || f.inflight < 0 {
		t.Errorf("receipts deleted twice: %v (inflight %d)", f.doubleDelete, f.inflight)
	}
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

func ids(recs []*Record) []string {
	out := make([]string, len(recs))
	for i, r := range recs {
		out[i] = r.ID
	}
	return out
}

func rh(ids ...string) []string {
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = "rh-" + id
	}
	sort.Strings(out)
	return out
}

func TestBatchConsumer_Acks(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name        string
		n           int
		group       func(int) string
		fifo        bool
		handler     BatchHandler
		wantDeleted []string
		wantVis     map[string]int32 // backoff visibility per receipt
	}{
		{
			name:        "all succeed",
			n:           3,
			handler:     func(context.Context, []*Record) Result { return Result{} },
			wantDeleted: rh("m0", "m1", "m2"),
		},
		{
			name: "partial failure keeps failed records",
			n:    3,
			handler: func(_ context.Context, r []*Record) Result {
				return Result{Failures: []Failure{{ID: "m1", Err: boom}}}
			},
			wantDeleted: rh("m0", "m2"),
		},
		{
			name: "retry after sets visibility",
			n:    2,
			handler: func(context.Context, []*Record) Result {
				return Result{Failures: []Failure{{ID: "m0", Err: boom, RetryAfter: 45 * time.Second}}}
			},
			wantDeleted: rh("m1"),
			wantVis:     map[string]int32{"rh-m0": 45},
		},
		{
			name:        "panic deletes nothing",
			n:           2,
			handler:     func(context.Context, []*Record) Result { panic("bad") },
			wantDeleted: nil,
		},
		{
			name: "unknown id fails whole batch",
			n:    2,
			handler: func(context.Context, []*Record) Result {
				return Result{Failures: []Failure{{ID: "nope", Err: boom}}}
			},
			wantDeleted: nil,
		},
		{
			name:  "fifo failure blocks later messages in group only",
			n:     4,
			group: func(i int) string { return []string{"a", "a", "a", "b"}[i] },
			fifo:  true,
			handler: func(context.Context, []*Record) Result { // reports only m1; m2 must still be held
				return Result{Failures: []Failure{{ID: "m1", Err: boom}}}
			},
			wantDeleted: rh("m0", "m3"),
		},
		{
			name:  "per-message adapter stops group at first failure",
			n:     4,
			group: func(i int) string { return []string{"a", "a", "b", "b"}[i] },
			fifo:  true,
			handler: PerMessage(func(_ context.Context, r *Record) error {
				if r.ID == "m2" {
					return boom
				}
				return nil
			}, 4),
			wantDeleted: rh("m0", "m1"),
		},
		{
			name: "decode failure fails only that record",
			n:    2,
			handler: func() BatchHandler {
				h := Decoded(func(_ context.Context, items []Item[struct{ N int }]) Result { return Result{} })
				return func(ctx context.Context, r []*Record) Result {
					r[0].Data = []byte("{not json")
					return h(ctx, r)
				}
			}(),
			wantDeleted: rh("m1"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(tt.n, tt.group)
			var mu sync.Mutex
			handled := 0
			h := func(ctx context.Context, r []*Record) Result {
				mu.Lock()
				handled += len(r)
				mu.Unlock()
				return tt.handler(ctx, r)
			}
			runBatch(t, f, h, Options{MaxBatch: 10, Linger: 20 * time.Millisecond, FIFO: aws.Bool(tt.fifo)}, func() bool {
				mu.Lock()
				defer mu.Unlock()
				return handled >= tt.n
			})
			deleted, calls, vis, _, _, _ := f.snapshot()
			sort.Strings(deleted)
			if !slices.Equal(deleted, tt.wantDeleted) {
				t.Errorf("deleted = %v, want %v", deleted, tt.wantDeleted)
			}
			for _, c := range calls {
				if c > 10 {
					t.Errorf("DeleteMessageBatch with %d entries", c)
				}
			}
			for k, want := range tt.wantVis {
				if vis[k] != want {
					t.Errorf("visibility[%s] = %d, want %d", k, vis[k], want)
				}
			}
		})
	}
}

func TestBatchConsumer_Batching(t *testing.T) {
	tests := []struct {
		name      string
		n         int
		maxBatch  int
		linger    time.Duration
		wantSizes []int
	}{
		{"accumulates across receives beyond 10", 25, 25, time.Second, []int{25}},
		{"linger flushes a partial batch", 3, 10, 20 * time.Millisecond, []int{3}},
		{"splits at max batch", 12, 5, 50 * time.Millisecond, []int{5, 5, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(tt.n, nil)
			var mu sync.Mutex
			var sizes []int
			h := func(_ context.Context, r []*Record) Result {
				mu.Lock()
				sizes = append(sizes, len(r))
				mu.Unlock()
				return Result{}
			}
			runBatch(t, f, h, Options{MaxBatch: tt.maxBatch, Linger: tt.linger, Workers: 1}, func() bool {
				d, _, _, _, _, _ := f.snapshot()
				return len(d) == tt.n
			})
			mu.Lock()
			defer mu.Unlock()
			if !slices.Equal(sizes, tt.wantSizes) {
				t.Errorf("batch sizes = %v, want %v", sizes, tt.wantSizes)
			}
		})
	}
}

func TestBatchConsumer_HeartbeatExtendsSlowBatch(t *testing.T) {
	f := newFake(2, nil)
	release := make(chan struct{})
	h := func(ctx context.Context, r []*Record) Result { <-release; return Result{} }
	go func() { time.Sleep(2500 * time.Millisecond); close(release) }()
	runBatch(t, f, h, Options{Visibility: 3 * time.Second, Heartbeat: time.Second, Linger: 10 * time.Millisecond}, func() bool {
		d, _, _, _, _, _ := f.snapshot()
		return len(d) == 2
	})
	_, _, vis, visCalls, _, _ := f.snapshot()
	if visCalls < 2 || vis["rh-m0"] != 3 {
		t.Errorf("heartbeat calls = %d, vis = %v; want >= 2 calls extending to 3s", visCalls, vis)
	}
}

func TestBatchConsumer_ReceiveErrorBacksOff(t *testing.T) {
	f := newFake(0, nil)
	f.recvErr = errors.New("throttled")
	runBatch(t, f, func(context.Context, []*Record) Result { return Result{} }, Options{}, func() bool { return false })
	if _, _, _, _, receives, _ := f.snapshot(); receives > 8 { // 3s of backoff starting at 100ms is ~5 calls
		t.Errorf("ReceiveMessage called %d times in 3s; want backoff", receives)
	}
}

func TestBatchConsumer_BackpressureCapsInFlight(t *testing.T) {
	f := newFake(40, nil)
	h := func(context.Context, []*Record) Result { time.Sleep(30 * time.Millisecond); return Result{} }
	runBatch(t, f, h, Options{MaxBatch: 5, Workers: 1, MaxInFlight: 7, Linger: 5 * time.Millisecond}, func() bool {
		d, _, _, _, _, _ := f.snapshot()
		return len(d) == 40
	})
	if _, _, _, _, _, peak := f.snapshot(); peak > 7 {
		t.Errorf("peak in-flight = %d, want <= 7", peak)
	}
}

func TestBatchConsumer_ShutdownDrainsAndAcks(t *testing.T) {
	f := newFake(3, nil)
	started := make(chan struct{})
	var once sync.Once
	h := func(ctx context.Context, r []*Record) Result {
		once.Do(func() { close(started) })
		time.Sleep(200 * time.Millisecond) // still running when ctx is cancelled
		if ctx.Err() != nil {
			return FailAll(r, ctx.Err())
		}
		return Result{}
	}
	runBatch(t, f, h, Options{Linger: 10 * time.Millisecond}, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})
	if d, _, _, _, _, _ := f.snapshot(); len(d) != 3 {
		t.Errorf("deleted %d after shutdown, want 3 (handler ctx must outlive Run ctx)", len(d))
	}
}

func TestBatchConsumer_DeleteRetriesOnlyRetryableEntries(t *testing.T) {
	f := newFake(3, nil)
	f.failDelete = map[string]bool{"rh-m0": false, "rh-m1": true} // m0 retryable, m1 sender fault
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	runBatch(t, f, func(context.Context, []*Record) Result { return Result{} }, Options{Linger: 10 * time.Millisecond, Logger: logger},
		func() bool {
			_, calls, _, _, _, _ := f.snapshot()
			return len(calls) >= 2
		})
	deleted, calls, _, _, _, _ := f.snapshot()
	sort.Strings(deleted)
	if want := rh("m0", "m2"); !slices.Equal(deleted, want) {
		t.Errorf("deleted = %v, want %v", deleted, want)
	}
	if want := []int{3, 1}; !slices.Equal(calls, want) {
		t.Errorf("DeleteMessageBatch entries per call = %v, want %v (retry only the non-sender-fault entry)", calls, want)
	}
	if got := logs.String(); !strings.Contains(got, "message_id=m1") || !strings.Contains(got, "receipt_handle=rh-m1") || !strings.Contains(got, "id=1") || !strings.Contains(got, "ReceiptHandleIsInvalid") || !strings.Contains(got, "bad receipt") {
		t.Errorf("sender-fault delete was not logged with message id, receipt, id, code, message: %s", got)
	}
}

func TestBatchConsumer_DrainTimeoutCancelsHandlers(t *testing.T) {
	f := newFake(2, nil)
	started := make(chan struct{})
	var once sync.Once
	var sawCancel bool
	h := func(ctx context.Context, r []*Record) Result {
		once.Do(func() { close(started) })
		select {
		case <-ctx.Done():
			sawCancel = true
			return FailAll(r, ctx.Err())
		case <-time.After(10 * time.Second):
			return Result{}
		}
	}
	begin := time.Now()
	runBatch(t, f, h, Options{Linger: 10 * time.Millisecond, DrainTimeout: 100 * time.Millisecond}, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})
	if took := time.Since(begin); took > 2*time.Second {
		t.Errorf("Run took %v after cancel; DrainTimeout should cancel handlers after 100ms", took)
	}
	if !sawCancel {
		t.Error("handler ctx was not cancelled after DrainTimeout")
	}
	if d, _, _, _, _, _ := f.snapshot(); len(d) != 0 {
		t.Errorf("deleted %v, want none (handler failed on cancel)", d)
	}
}

func TestBatchConsumer_MaxExtendStopsHeartbeat(t *testing.T) {
	tests := []struct {
		name      string
		maxExtend time.Duration
		minCalls  int
		maxCalls  int
	}{
		// MaxExtend 0 is rewritten to the default 15m, so a short handler is
		// still extended. Heartbeat is 1s and the handler runs 2.5s: two beats,
		// plus the visibility refresh before delete, with slack if a beat
		// overlaps the delete.
		{"default cap keeps extending a short handler", 0, 2, 5},
		// The first beat is at 1s, by which the record is already past a 500ms
		// cap, so the only call is the refresh before delete.
		{"capped before the first beat", 500 * time.Millisecond, 1, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(1, nil)
			h := func(context.Context, []*Record) Result { time.Sleep(2500 * time.Millisecond); return Result{} }
			cancel, done := startBatch(t, f, h, Options{Visibility: 4 * time.Second, Heartbeat: time.Second, MaxExtend: tt.maxExtend,
				Linger: 10 * time.Millisecond})
			if !waitUntil(t, 6*time.Second, func() bool {
				d, _, _, _, _, _ := f.snapshot()
				return len(d) == 1
			}) {
				stopBatch(t, cancel, done)
				t.Fatal("record was not deleted")
			}
			stopBatch(t, cancel, done)
			if _, _, _, visCalls, _, _ := f.snapshot(); visCalls < tt.minCalls || visCalls > tt.maxCalls {
				t.Errorf("heartbeat calls = %d, want %d..%d", visCalls, tt.minCalls, tt.maxCalls)
			}
		})
	}
}

func TestBatchConsumer_LegacyHandlerViaPerMessage(t *testing.T) {
	// A legacy gosqs.Handler runs unchanged through PerMessage; an error keeps the record.
	var legacy Handler = func(_ context.Context, m Message) error {
		if m.MessageId() == "m1" {
			return errors.New("boom")
		}
		return nil
	}
	f := newFake(3, nil)
	h := PerMessage(func(ctx context.Context, r *Record) error { return legacy(ctx, r) }, 2)
	runBatch(t, f, h, Options{Linger: 10 * time.Millisecond}, func() bool {
		d, _, _, _, _, _ := f.snapshot()
		return len(d) == 2
	})
	deleted, _, _, _, _, _ := f.snapshot()
	sort.Strings(deleted)
	if want := rh("m0", "m2"); !slices.Equal(deleted, want) {
		t.Errorf("deleted = %v, want %v", deleted, want)
	}
}

func TestOptions_Validation(t *testing.T) {
	tests := []struct {
		name       string
		url        string
		o          Options
		ok         bool
		visibility time.Duration
		heartbeat  time.Duration
		linger     time.Duration
	}{
		{"defaults", "u", Options{}, true, 30 * time.Second, 10 * time.Second, 200 * time.Millisecond},
		{"empty url", "", Options{}, false, 0, 0, 0},
		{"wait > 20s", "u", Options{WaitTime: 21 * time.Second}, false, 0, 0, 0},
		{"wait 20.9s truncates into range", "u", Options{WaitTime: 20*time.Second + 900*time.Millisecond}, true, 0, 0, 0},
		{"sub-second wait rounds up", "u", Options{WaitTime: 500 * time.Millisecond}, true, 0, 0, 0},
		{"heartbeat seconds collide after truncation", "u", Options{Visibility: 10*time.Second + 500*time.Millisecond, Heartbeat: 10 * time.Second}, false, 0, 0, 0},
		{"heartbeat one second under truncated visibility", "u", Options{Visibility: 10*time.Second + 500*time.Millisecond, Heartbeat: 9 * time.Second}, true, 10 * time.Second, 9 * time.Second, 0},
		{"sub-second gap is stored as whole seconds", "u", Options{Visibility: 10100 * time.Millisecond, Heartbeat: 9900 * time.Millisecond}, true, 10 * time.Second, 9 * time.Second, 0},
		{"heartbeat under 1s", "u", Options{Visibility: 30 * time.Second, Heartbeat: time.Millisecond}, false, 0, 0, 0},
		{"heartbeat truncates under 1s", "u", Options{Visibility: 30 * time.Second, Heartbeat: 999 * time.Millisecond}, false, 0, 0, 0},
		{"short visibility raises a default heartbeat to 1s", "u", Options{Visibility: 2 * time.Second}, true, 2 * time.Second, time.Second, 0},
		{"visibility over 12h", "u", Options{Visibility: 12*time.Hour + time.Second}, false, 0, 0, 0},
		{"visibility exactly 12h", "u", Options{Visibility: 12 * time.Hour}, true, 0, 0, 0},
		{"sub-second visibility", "u", Options{Visibility: 500 * time.Millisecond}, false, 0, 0, 0},
		{"linger >= visibility", "u", Options{Visibility: 2 * time.Second, Heartbeat: time.Second, Linger: 3 * time.Second}, false, 0, 0, 0},
		{"linger matches truncated visibility", "u", Options{Visibility: 10500 * time.Millisecond, Heartbeat: time.Second, Linger: 10 * time.Second}, false, 0, 0, 0},
		{"multi-second linger is stored truncated", "u", Options{Visibility: 30 * time.Second, Heartbeat: time.Second, Linger: 1500 * time.Millisecond}, true, 30 * time.Second, time.Second, time.Second},
		{"sub-second linger is kept", "u", Options{Linger: 200 * time.Millisecond}, true, 0, 0, 200 * time.Millisecond},
		{"fifo from url", "https://x/q.fifo", Options{}, true, 0, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o, err := tt.o.withDefaults(tt.url)
			if (err == nil) != tt.ok {
				t.Fatalf("err = %v, want ok=%v", err, tt.ok)
			}
			if !tt.ok {
				return
			}
			if *o.FIFO != (tt.url == "https://x/q.fifo") {
				t.Errorf("FIFO = %v", *o.FIFO)
			}
			if tt.o.WaitTime > 0 && tt.o.WaitTime < time.Second && o.WaitTime != time.Second {
				t.Errorf("WaitTime = %v, want 1s after round-up", o.WaitTime)
			}
			if tt.visibility != 0 && o.Visibility != tt.visibility {
				t.Errorf("Visibility = %v, want %v", o.Visibility, tt.visibility)
			}
			if tt.heartbeat != 0 && o.Heartbeat != tt.heartbeat {
				t.Errorf("Heartbeat = %v, want %v", o.Heartbeat, tt.heartbeat)
			}
			if tt.linger != 0 && o.Linger != tt.linger {
				t.Errorf("Linger = %v, want %v", o.Linger, tt.linger)
			}
		})
	}
}

func TestBatchConsumer_FIFOAcrossBatches(t *testing.T) {
	boom := errors.New("boom")
	const retry = 7 * time.Second
	opts := func(workers, maxBatch int) Options {
		return Options{
			MaxBatch: maxBatch, Workers: workers, Linger: 15 * time.Millisecond, FIFO: aws.Bool(true),
			Heartbeat: 20 * time.Second, Visibility: 30 * time.Second,
		}
	}
	// headFails fails the batch that contains m0 and succeeds any other batch.
	// A correct gate never hands the tail batch to the handler.
	headFails := func(got *[]string, mu *sync.Mutex) BatchHandler {
		return func(_ context.Context, recs []*Record) Result {
			mu.Lock()
			*got = append(*got, ids(recs)...)
			mu.Unlock()
			for _, r := range recs {
				if r.ID == "m0" {
					time.Sleep(150 * time.Millisecond) // let a second worker reach the tail
					return Result{Failures: []Failure{{ID: "m0", Err: boom, RetryAfter: retry}}}
				}
			}
			return Result{}
		}
	}
	assertTailHeld := func(t *testing.T, f *fakeSQS, got []string, wantHandled, wantDeleted []string) {
		t.Helper()
		if !slices.Equal(got, wantHandled) {
			t.Errorf("handler saw %v, want %v", got, wantHandled)
		}
		deleted, _, vis, _, _, _ := f.snapshot()
		sort.Strings(deleted)
		if !slices.Equal(deleted, wantDeleted) {
			t.Errorf("deleted = %v, want %v", deleted, wantDeleted)
		}
		for _, id := range []string{"m0", "m1", "m2", "m3"} {
			if slices.Contains(wantDeleted, "rh-"+id) {
				continue
			}
			if vis["rh-"+id] != int32(retry/time.Second) {
				t.Errorf("visibility[%s] = %d, want %d (RetryAfter inherited, not deleted)", id, vis["rh-"+id], int32(retry/time.Second))
			}
		}
	}

	t.Run("workers>1 head failure holds the tail", func(t *testing.T) {
		f := newFake(4, func(int) string { return "g" })
		var mu sync.Mutex
		var got []string
		runBatch(t, f, headFails(&got, &mu), opts(2, 2), func() bool {
			_, _, vis, _, _, _ := f.snapshot()
			return vis["rh-m3"] == int32(retry/time.Second)
		})
		mu.Lock()
		defer mu.Unlock()
		assertTailHeld(t, f, got, []string{"m0", "m1"}, nil)
	})

	t.Run("workers=1 head failure holds the tail", func(t *testing.T) {
		f := newFake(4, func(int) string { return "g" })
		var mu sync.Mutex
		var got []string
		runBatch(t, f, headFails(&got, &mu), opts(1, 2), func() bool {
			_, _, vis, _, _, _ := f.snapshot()
			return vis["rh-m3"] == int32(retry/time.Second)
		})
		mu.Lock()
		defer mu.Unlock()
		assertTailHeld(t, f, got, []string{"m0", "m1"}, nil)
	})

	t.Run("success preserves order across workers", func(t *testing.T) {
		f := newFake(4, func(int) string { return "g" })
		var mu sync.Mutex
		var got []string
		var orderOK = true
		h := func(_ context.Context, recs []*Record) Result {
			if recs[0].ID == "m2" {
				d, _, _, _, _, _ := f.snapshot()
				mu.Lock()
				if !slices.Contains(d, "rh-m0") || !slices.Contains(d, "rh-m1") {
					orderOK = false
				}
				mu.Unlock()
			}
			mu.Lock()
			got = append(got, ids(recs)...)
			mu.Unlock()
			if recs[0].ID == "m0" {
				time.Sleep(150 * time.Millisecond)
			}
			return Result{}
		}
		runBatch(t, f, h, opts(2, 2), func() bool {
			d, _, _, _, _, _ := f.snapshot()
			return len(d) == 4
		})
		mu.Lock()
		defer mu.Unlock()
		if !slices.Equal(got, []string{"m0", "m1", "m2", "m3"}) {
			t.Errorf("handler order = %v, want m0..m3", got)
		}
		if !orderOK {
			t.Error("tail batch started before the head batch was deleted")
		}
	})

	t.Run("other groups are not blocked", func(t *testing.T) {
		// MaxBatch 3 splits [a, b, b] and [c, b]. Failing b holds only b.
		f := newFake(5, func(i int) string { return []string{"a", "b", "b", "c", "b"}[i] })
		var mu sync.Mutex
		var got []string
		h := func(_ context.Context, recs []*Record) Result {
			mu.Lock()
			got = append(got, ids(recs)...)
			mu.Unlock()
			for _, r := range recs {
				if r.ID == "m1" {
					time.Sleep(100 * time.Millisecond)
					return Result{Failures: []Failure{{ID: "m1", Err: boom, RetryAfter: retry}}}
				}
			}
			return Result{}
		}
		runBatch(t, f, h, opts(2, 3), func() bool {
			d, _, vis, _, _, _ := f.snapshot()
			return len(d) >= 2 && vis["rh-m4"] == int32(retry/time.Second)
		})
		mu.Lock()
		defer mu.Unlock()
		sort.Strings(got)
		if want := []string{"m0", "m1", "m2", "m3"}; !slices.Equal(got, want) {
			t.Errorf("handler saw %v, want %v (m4 blocked, m3 not)", got, want)
		}
		deleted, _, vis, _, _, _ := f.snapshot()
		sort.Strings(deleted)
		if want := rh("m0", "m3"); !slices.Equal(deleted, want) {
			t.Errorf("deleted = %v, want %v", deleted, want)
		}
		for _, id := range []string{"m1", "m2", "m4"} {
			if vis["rh-"+id] != int32(retry/time.Second) {
				t.Errorf("visibility[%s] = %d, want %d", id, vis["rh-"+id], int32(retry/time.Second))
			}
		}
	})
}

func TestBatchConsumer_VisibilityEntryFailures(t *testing.T) {
	boom := errors.New("boom")
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	t.Run("heartbeat retries transient and logs sender fault", func(t *testing.T) {
		logs.Reset()
		f := newFake(2, nil)
		f.failVisibility = map[string]bool{"rh-m0": false, "rh-m1": true}
		release := make(chan struct{})
		entered := make(chan struct{})
		var once sync.Once
		h := func(context.Context, []*Record) Result {
			once.Do(func() { close(entered) })
			<-release
			return Result{}
		}
		cancel, done := startBatch(t, f, h, Options{
			Linger: 10 * time.Millisecond, Heartbeat: time.Second, Visibility: 5 * time.Second, Logger: logger,
		})
		defer stopBatch(t, cancel, done)
		defer close(release)
		if !waitUntil(t, 4*time.Second, func() bool { return f.visAttempt("rh-m0") >= 1 && f.visAttempt("rh-m1") >= 1 }) {
			t.Fatal("heartbeat did not run")
		}
		<-entered
		time.Sleep(200 * time.Millisecond) // well under the 1s period, so only the in-round retry lands
		if got := f.visAttempt("rh-m0"); got < 2 {
			t.Errorf("retryable visibility attempts = %d, want >= 2", got)
		}
		if got := f.visAttempt("rh-m1"); got != 1 {
			t.Errorf("sender-fault visibility attempts = %d, want 1 (no immediate retry)", got)
		}
		if _, _, vis, _, _, _ := f.snapshot(); vis["rh-m0"] != 5 {
			t.Errorf("visibility[rh-m0] = %d, want 5 after retry", vis["rh-m0"])
		}
		if got := logs.String(); !strings.Contains(got, "message_id=m1") || !strings.Contains(got, "receipt_handle=rh-m1") || !strings.Contains(got, "ReceiptHandleIsInvalid") || !strings.Contains(got, "bad receipt") {
			t.Errorf("sender-fault heartbeat entry not logged with message id and receipt: %s", got)
		}
	})

	t.Run("backoff retries a transient entry", func(t *testing.T) {
		f := newFake(1, nil)
		f.failVisibility = map[string]bool{"rh-m0": false}
		h := func(context.Context, []*Record) Result {
			return Result{Failures: []Failure{{ID: "m0", Err: boom, RetryAfter: 45 * time.Second}}}
		}
		runBatch(t, f, h, Options{Linger: 10 * time.Millisecond, Heartbeat: 20 * time.Second, Visibility: 30 * time.Second}, func() bool {
			return f.visAttempt("rh-m0") >= 2
		})
		if got := f.visAttempt("rh-m0"); got != 2 {
			t.Errorf("attempts = %d, want 2", got)
		}
		if _, _, vis, _, _, _ := f.snapshot(); vis["rh-m0"] != 45 {
			t.Errorf("visibility = %d, want 45", vis["rh-m0"])
		}
	})

	t.Run("backoff logs a sender fault and does not retry it", func(t *testing.T) {
		logs.Reset()
		f := newFake(1, nil)
		f.failVisibility = map[string]bool{"rh-m0": true}
		h := func(context.Context, []*Record) Result {
			return Result{Failures: []Failure{{ID: "m0", Err: boom, RetryAfter: 45 * time.Second}}}
		}
		runBatch(t, f, h, Options{Linger: 10 * time.Millisecond, Heartbeat: 20 * time.Second, Visibility: 30 * time.Second, Logger: logger}, func() bool {
			return f.visAttempt("rh-m0") >= 1
		})
		if got := f.visAttempt("rh-m0"); got != 1 {
			t.Errorf("attempts = %d, want 1", got)
		}
		if _, _, vis, _, _, _ := f.snapshot(); vis["rh-m0"] != 0 {
			t.Errorf("visibility = %d, want unset", vis["rh-m0"])
		}
		if got := logs.String(); !strings.Contains(got, "message_id=m0") || !strings.Contains(got, "receipt_handle=rh-m0") || !strings.Contains(got, "id=0") || !strings.Contains(got, "ReceiptHandleIsInvalid") || !strings.Contains(got, "bad receipt") {
			t.Errorf("sender-fault backoff not logged with message id, receipt, id, code, message: %s", got)
		}
	})
}

func TestBatchConsumer_RetryAfterRoundsUp(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name    string
		retry   time.Duration
		wantVis int32 // 0: visibility is not changed
	}{
		{"sub-second rounds up to 1s", 500 * time.Millisecond, 1},
		{"zero does not send 0", 0, 0},
		{"over a second truncates", 1500 * time.Millisecond, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(1, nil)
			handled := make(chan struct{})
			h := func(context.Context, []*Record) Result {
				close(handled)
				return Result{Failures: []Failure{{ID: "m0", Err: boom, RetryAfter: tt.retry}}}
			}
			cancel, done := startBatch(t, f, h, Options{Linger: 10 * time.Millisecond, Heartbeat: 20 * time.Second, Visibility: 30 * time.Second})
			defer stopBatch(t, cancel, done)
			select {
			case <-handled:
			case <-time.After(2 * time.Second):
				t.Fatal("handler was not called")
			}
			if tt.wantVis > 0 {
				if !waitUntil(t, 2*time.Second, func() bool {
					_, _, vis, _, _, _ := f.snapshot()
					return vis["rh-m0"] == tt.wantVis
				}) {
					_, _, vis, _, _, _ := f.snapshot()
					t.Fatalf("visibility = %d, want %d", vis["rh-m0"], tt.wantVis)
				}
			} else {
				// Backoff would run in the handler goroutine right after it
				// returns. Give that goroutine time, then assert it sent nothing.
				time.Sleep(200 * time.Millisecond)
			}
			if got := f.visAttempt("rh-m0"); (tt.wantVis == 0 && got != 0) || (tt.wantVis > 0 && got < 1) {
				t.Errorf("visibility attempts = %d, wantVis %d", got, tt.wantVis)
			}
			if _, _, vis, _, _, _ := f.snapshot(); vis["rh-m0"] != tt.wantVis {
				t.Errorf("visibility = %d, want %d", vis["rh-m0"], tt.wantVis)
			}
		})
	}
}

func TestBatchConsumer_BackoffBeforeDeleteAndHeartbeatKeepsAck(t *testing.T) {
	boom := errors.New("boom")
	f := newFake(2, nil)
	release := make(chan struct{})
	var once sync.Once
	releaseOnce := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseOnce)
	entered := make(chan struct{})
	var enterOnce sync.Once
	f.onDelete = func(context.Context) {
		enterOnce.Do(func() { close(entered) })
		<-release
	}
	h := func(context.Context, []*Record) Result {
		return Result{Failures: []Failure{{ID: "m0", Err: boom, RetryAfter: 45 * time.Second}}}
	}
	cancel, done := startBatch(t, f, h, Options{
		Linger: 10 * time.Millisecond, Heartbeat: time.Second, Visibility: 8 * time.Second, AckTimeout: 30 * time.Second,
	})
	defer stopBatch(t, cancel, done)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("delete did not start")
	}
	if _, _, vis, _, _, _ := f.snapshot(); vis["rh-m0"] != 45 {
		t.Fatalf("backoff not applied before delete, vis=%v", vis)
	}
	_, _, _, before, _, _ := f.snapshot()
	if !waitUntil(t, 4*time.Second, func() bool {
		_, _, vis, calls, _, _ := f.snapshot()
		return calls > before && vis["rh-m0"] == 45 && vis["rh-m1"] == 8
	}) {
		_, _, vis, calls, _, _ := f.snapshot()
		t.Fatalf("ack was not heartbeated during delete without overwriting backoff: calls %d->%d vis=%v", before, calls, vis)
	}
	releaseOnce()
	if !waitUntil(t, 3*time.Second, func() bool {
		d, _, _, _, _, _ := f.snapshot()
		return len(d) == 1
	}) {
		t.Fatal("ack was not deleted")
	}
	deleted, _, vis, calls, _, _ := f.snapshot()
	sort.Strings(deleted)
	if want := rh("m1"); !slices.Equal(deleted, want) {
		t.Errorf("deleted = %v, want %v", deleted, want)
	}
	if vis["rh-m0"] != 45 {
		t.Errorf("backoff overwritten after delete: %v", vis)
	}
	time.Sleep(1500 * time.Millisecond)
	if _, _, _, after, _, _ := f.snapshot(); after != calls {
		t.Errorf("heartbeat continued after untrack: %d -> %d", calls, after)
	}
}

func TestBatchConsumer_ExtendsVisibilityBeforeDelete(t *testing.T) {
	// Heartbeat one second under Visibility leaves less than AckTimeout when a
	// handler returns just before the next tick. That combination stays legal:
	// the delete path extends the receipt first instead of rejecting it.
	o, err := (Options{Visibility: 30 * time.Second, Heartbeat: 29 * time.Second, AckTimeout: 10 * time.Second}).withDefaults("u")
	if err != nil {
		t.Fatalf("Visibility-Heartbeat under AckTimeout must be accepted: %v", err)
	}
	if o.Visibility != 30*time.Second || o.Heartbeat != 29*time.Second {
		t.Fatalf("stored vis=%s heartbeat=%s", o.Visibility, o.Heartbeat)
	}

	f := newFake(1, nil)
	started := make(chan struct{})
	var once sync.Once
	f.onDelete = func(ctx context.Context) {
		once.Do(func() { close(started) })
		<-ctx.Done()
	}
	cancel, done := startBatch(t, f, func(context.Context, []*Record) Result { return Result{} }, Options{
		Linger: 10 * time.Millisecond, Visibility: 30 * time.Second, Heartbeat: 29 * time.Second, AckTimeout: 300 * time.Millisecond,
	})
	defer stopBatch(t, cancel, done)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("delete did not start")
	}
	if _, _, vis, calls, _, _ := f.snapshot(); vis["rh-m0"] != 30 || calls < 1 {
		t.Fatalf("visibility before delete = %d (%d calls), want 30 from the pre-delete extend", vis["rh-m0"], calls)
	}
}

func TestBatchConsumer_DeleteHasOwnAckTimeout(t *testing.T) {
	boom := errors.New("boom")
	f := newFake(2, nil)
	f.blockVis = 1 // backoff waits out its own AckTimeout
	h := func(context.Context, []*Record) Result {
		return Result{Failures: []Failure{{ID: "m0", Err: boom, RetryAfter: 10 * time.Second}}}
	}
	runBatch(t, f, h, Options{
		Linger: 10 * time.Millisecond, Heartbeat: 20 * time.Second, Visibility: 30 * time.Second, AckTimeout: 200 * time.Millisecond,
	}, func() bool {
		d, _, _, _, _, _ := f.snapshot()
		return slices.Contains(d, "rh-m1")
	})
	deleted, _, _, _, _, _ := f.snapshot()
	sort.Strings(deleted)
	if want := rh("m1"); !slices.Equal(deleted, want) {
		t.Errorf("deleted = %v, want %v (delete must not share backoff's expired context)", deleted, want)
	}
}

func TestBatchConsumer_DeleteFailureHoldsFIFOTail(t *testing.T) {
	fifo := func(maxBatch, workers int, ack time.Duration) Options {
		return Options{
			MaxBatch: maxBatch, Workers: workers, Linger: 300 * time.Millisecond, FIFO: aws.Bool(true),
			Heartbeat: time.Second, Visibility: 30 * time.Second, AckTimeout: ack,
		}
	}
	// headTail runs m0 then m1 as separate batches. The handler succeeds for
	// both; only a delete failure on m0 may stop m1. The consumer stays up so
	// the caller can see whether the failed receipt is still heartbeated.
	headTail := func(t *testing.T, f *fakeSQS, o Options) (handled []string, cancel func(), done <-chan struct{}) {
		t.Helper()
		var mu sync.Mutex
		var got []string
		h := func(_ context.Context, recs []*Record) Result {
			mu.Lock()
			got = append(got, ids(recs)...)
			mu.Unlock()
			if len(recs) > 0 && recs[0].ID == "m0" {
				time.Sleep(150 * time.Millisecond) // let the other worker reach the gate
			}
			return Result{}
		}
		cancel, done = startBatch(t, f, h, o)
		if !waitUntil(t, 3*time.Second, func() bool {
			_, calls, _, _, _, _ := f.snapshot()
			return len(calls) >= 1
		}) {
			stopBatch(t, cancel, done)
			t.Fatal("head delete was not attempted")
		}
		time.Sleep(300 * time.Millisecond) // a released tail would be deleted within this
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), got...), cancel, done
	}
	stillBeating := func(t *testing.T, f *fakeSQS) {
		t.Helper()
		n := f.visAttempt("rh-m0")
		if n < 1 {
			t.Fatal("head visibility was not extended")
		}
		time.Sleep(1300 * time.Millisecond)
		if got := f.visAttempt("rh-m0"); got <= n {
			t.Errorf("stopped heartbeating a failed delete: %d -> %d", n, got)
		}
	}

	t.Run("transport error on the head", func(t *testing.T) {
		f := newFake(2, func(int) string { return "g" })
		f.failDeleteReceipt = "rh-m0"
		handled, cancel, done := headTail(t, f, fifo(1, 2, time.Second))
		defer stopBatch(t, cancel, done)
		if !slices.Equal(handled, []string{"m0"}) {
			t.Errorf("handler saw %v, want [m0]", handled)
		}
		deleted, calls, vis, _, _, _ := f.snapshot()
		if len(deleted) != 0 {
			t.Errorf("deleted = %v, want none", deleted)
		}
		if len(calls) < 2 {
			t.Errorf("delete calls = %v, want two failed attempts and no tail call", calls)
		}
		if vis["rh-m0"] != 30 {
			t.Errorf("head visibility = %d, want 30", vis["rh-m0"])
		}
		stillBeating(t, f)
	})

	t.Run("ack timeout on the head", func(t *testing.T) {
		f := newFake(2, func(int) string { return "g" })
		f.hangDelete = "rh-m0"
		handled, cancel, done := headTail(t, f, fifo(1, 2, 200*time.Millisecond))
		defer stopBatch(t, cancel, done)
		if !slices.Equal(handled, []string{"m0"}) {
			t.Errorf("handler saw %v, want [m0]", handled)
		}
		if deleted, _, _, _, _, _ := f.snapshot(); len(deleted) != 0 {
			t.Errorf("deleted = %v, want none", deleted)
		}
		stillBeating(t, f)
	})

	t.Run("max batch 11 does not delete past a failed chunk", func(t *testing.T) {
		// Eleven records of one group are one handler batch and two delete
		// calls. The first call fails as a whole; the 11th must not be sent.
		f := newFake(11, func(int) string { return "g" })
		f.failDeleteReceipt = "rh-m0"
		cancel, done := startBatch(t, f, func(context.Context, []*Record) Result { return Result{} }, fifo(11, 1, time.Second))
		defer stopBatch(t, cancel, done)
		if !waitUntil(t, 3*time.Second, func() bool {
			_, calls, _, _, _, _ := f.snapshot()
			return len(calls) >= 2
		}) {
			t.Fatal("first chunk was not retried")
		}
		time.Sleep(300 * time.Millisecond) // a following 1-entry call would have landed
		deleted, calls, _, _, _, _ := f.snapshot()
		if len(deleted) != 0 {
			t.Errorf("deleted = %v, want none", deleted)
		}
		for _, n := range calls {
			if n == 1 {
				t.Errorf("delete calls = %v, want no 1-entry tail call", calls)
			}
		}
	})

	t.Run("partial failure in the first chunk holds the next", func(t *testing.T) {
		// m0 fails as an entry twice; m1..m9 share that call and may be
		// deleted. m10 is a later call and must not be sent.
		f := newFake(11, func(int) string { return "g" })
		f.failDeleteAlways = map[string]bool{"rh-m0": true}
		cancel, done := startBatch(t, f, func(context.Context, []*Record) Result { return Result{} }, fifo(11, 1, time.Second))
		defer stopBatch(t, cancel, done)
		if !waitUntil(t, 3*time.Second, func() bool {
			d, _, _, _, _, _ := f.snapshot()
			return len(d) >= 9
		}) {
			deleted, calls, _, _, _, _ := f.snapshot()
			t.Fatalf("deleted = %v calls = %v, want m1..m9 removed", deleted, calls)
		}
		time.Sleep(300 * time.Millisecond)
		deleted, calls, _, _, _, _ := f.snapshot()
		sort.Strings(deleted)
		if slices.Contains(deleted, "rh-m0") || slices.Contains(deleted, "rh-m10") {
			t.Errorf("deleted = %v, want m1..m9 only", deleted)
		}
		if want := rh("m1", "m2", "m3", "m4", "m5", "m6", "m7", "m8", "m9"); !slices.Equal(deleted, want) {
			t.Errorf("deleted = %v, want %v", deleted, want)
		}
		if want := []int{10, 1}; !slices.Equal(calls, want) {
			t.Errorf("delete calls = %v, want %v", calls, want)
		}
	})

	t.Run("another group is still deleted", func(t *testing.T) {
		f := newFake(11, func(i int) string {
			if i == 10 {
				return "b"
			}
			return "a"
		})
		f.failDeleteAlways = map[string]bool{"rh-m0": true}
		runBatch(t, f, func(context.Context, []*Record) Result { return Result{} }, fifo(11, 1, time.Second), func() bool {
			d, _, _, _, _, _ := f.snapshot()
			return slices.Contains(d, "rh-m10")
		})
		deleted, _, _, _, _, _ := f.snapshot()
		if !slices.Contains(deleted, "rh-m10") {
			t.Errorf("deleted = %v, want group b's m10 deleted", deleted)
		}
		if slices.Contains(deleted, "rh-m0") {
			t.Errorf("deleted = %v, want group a's head kept", deleted)
		}
	})
}

func TestBatchConsumer_FIFOGateMapShrinks(t *testing.T) {
	const n = 20
	groups := make([]string, n)
	for i := range groups {
		groups[i] = "g" + string(rune('a'+i))
	}
	f := newFake(n, func(i int) string { return groups[i] })
	release := make(chan struct{})
	h := func(context.Context, []*Record) Result {
		<-release
		return Result{}
	}
	c, err := New(f, "https://sqs.local/q.fifo", h, Options{
		MaxBatch: 10, Workers: 4, Linger: 20 * time.Millisecond, WaitTime: time.Second, Logger: quiet,
		Heartbeat: 20 * time.Second, Visibility: 30 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() { _ = c.Run(ctx); close(finished) }()
	defer stopBatch(t, cancel, finished)

	gateCount := func() int {
		c.gatesMu.Lock()
		defer c.gatesMu.Unlock()
		return len(c.gates)
	}
	if !waitUntil(t, 3*time.Second, func() bool {
		f.mu.Lock()
		q := len(f.queue)
		f.mu.Unlock()
		return q == 0 && gateCount() == n
	}) {
		f.mu.Lock()
		q := len(f.queue)
		f.mu.Unlock()
		t.Fatalf("while in flight: queue=%d gates=%d, want 0 and %d", q, gateCount(), n)
	}
	close(release)
	if !waitUntil(t, 3*time.Second, func() bool {
		d, _, _, _, _, _ := f.snapshot()
		return len(d) == n && gateCount() == 0
	}) {
		d, _, _, _, _, _ := f.snapshot()
		t.Fatalf("after idle: deleted=%d gates=%d, want %d and 0", len(d), gateCount(), n)
	}

	// The same group id is processed again after its gate was dropped.
	f.push("again", groups[0])
	if !waitUntil(t, 3*time.Second, func() bool {
		d, _, _, _, _, _ := f.snapshot()
		c.gatesMu.Lock()
		gates := len(c.gates)
		c.gatesMu.Unlock()
		return slices.Contains(d, "rh-again") && gates == 0
	}) {
		t.Fatalf("reused group: deleted-again=%v gates=%d", func() bool {
			d, _, _, _, _, _ := f.snapshot()
			return slices.Contains(d, "rh-again")
		}(), gateCount())
	}
}
