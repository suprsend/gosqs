package gosqs

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// *Record must keep satisfying the legacy interface.
var _ Message = (*Record)(nil)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// runBatch starts a consumer, waits until cond holds (or 3s), then shuts it down.
func runBatch(t *testing.T, f *fakeSQS, h BatchHandler, o Options, cond func() bool) {
	t.Helper()
	o.Logger = quiet
	if o.WaitTime == 0 {
		o.WaitTime = time.Second
	}
	c, err := New(f, "https://sqs.local/q", h, o)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.doubleDelete) > 0 || f.inflight < 0 {
		t.Errorf("receipts deleted twice: %v (inflight %d)", f.doubleDelete, f.inflight)
	}
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
	runBatch(t, f, func(context.Context, []*Record) Result { return Result{} }, Options{Linger: 10 * time.Millisecond},
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
		// Heartbeat every 200ms for a 1.3s handler: ~6 rounds without a cap.
		{"uncapped keeps extending", 0, 5, 7},
		{"capped at 500ms after receipt", 500 * time.Millisecond, 1, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFake(1, nil)
			h := func(context.Context, []*Record) Result { time.Sleep(1300 * time.Millisecond); return Result{} }
			runBatch(t, f, h, Options{Visibility: time.Second, Heartbeat: 200 * time.Millisecond, MaxExtend: tt.maxExtend,
				Linger: 10 * time.Millisecond}, func() bool {
				d, _, _, _, _, _ := f.snapshot()
				return len(d) == 1
			})
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
		name string
		url  string
		o    Options
		ok   bool
	}{
		{"defaults", "u", Options{}, true},
		{"empty url", "", Options{}, false},
		{"wait > 20s", "u", Options{WaitTime: 21 * time.Second}, false},
		{"heartbeat >= visibility", "u", Options{Visibility: 10 * time.Second, Heartbeat: 10 * time.Second}, false},
		{"linger >= visibility", "u", Options{Visibility: 2 * time.Second, Heartbeat: time.Second, Linger: 3 * time.Second}, false},
		{"fifo from url", "https://x/q.fifo", Options{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			o, err := tt.o.withDefaults(tt.url)
			if (err == nil) != tt.ok {
				t.Fatalf("err = %v, want ok=%v", err, tt.ok)
			}
			if tt.ok && *o.FIFO != (tt.url == "https://x/q.fifo") {
				t.Errorf("FIFO = %v", *o.FIFO)
			}
		})
	}
}
