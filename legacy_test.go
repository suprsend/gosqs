package gosqs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// isSQSErr reports whether err is target, with or without Context attached.
func isSQSErr(err error, target *SQSError) bool {
	var e *SQSError
	return errors.As(err, &e) && e.Err == target.Err
}

// sqsServer is a minimal SQS JSON-protocol endpoint for the legacy consumer,
// which holds a concrete *sqs.Client. It records the calls it gets.
type sqsServer struct {
	mu             sync.Mutex
	calls          map[string]int // action -> count
	visibility     []int32        // VisibilityTimeout of each ChangeMessageVisibility
	failRecv       bool           // ReceiveMessage returns a 500
	failVis        int            // ChangeMessageVisibility returns a 500 this many times
	hangVis        bool           // ChangeMessageVisibility waits until the client cancels
	visHang        []time.Duration
	recv           []string // successive ReceiveMessage bodies; then "{}" or block
	blockEmptyRecv bool     // an empty receive waits on the request context
}

func newSQSServer(t *testing.T) (*sqsServer, *sqs.Client, string) {
	t.Helper()
	s := &sqsServer{calls: map[string]int{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		action := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "AmazonSQS.")
		body, _ := io.ReadAll(r.Body)
		var in map[string]any
		_ = json.Unmarshal(body, &in)
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		s.mu.Lock()
		s.calls[action]++
		switch action {
		case "ReceiveMessage":
			fail := s.failRecv
			var payload string
			if !fail && len(s.recv) > 0 {
				payload = s.recv[0]
				s.recv = s.recv[1:]
			}
			block := !fail && payload == "" && s.blockEmptyRecv
			s.mu.Unlock()
			if fail {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"__type":"com.amazonaws.sqs#InternalError","message":"boom"}`))
				return
			}
			if block {
				<-r.Context().Done()
				return
			}
			if payload == "" {
				payload = `{}`
			}
			_, _ = w.Write([]byte(payload))
			return
		case "ChangeMessageVisibility":
			v, _ := in["VisibilityTimeout"].(float64)
			s.visibility = append(s.visibility, int32(v))
			fail := false
			if s.failVis > 0 {
				s.failVis--
				fail = true
			}
			hang := s.hangVis
			s.mu.Unlock()
			if fail {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte(`{"__type":"com.amazonaws.sqs#InternalError","message":"boom"}`))
				return
			}
			if hang {
				start := time.Now()
				<-r.Context().Done()
				s.mu.Lock()
				s.visHang = append(s.visHang, time.Since(start))
				s.mu.Unlock()
				return
			}
			_, _ = w.Write([]byte(`{}`))
			return
		default:
			s.mu.Unlock()
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(srv.Close)
	client := sqs.New(sqs.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		Credentials:  aws.AnonymousCredentials{},
		Retryer:      aws.NopRetryer{},
	})
	return s, client, srv.URL + "/000000000000/q"
}

func (s *sqsServer) count(action string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[action]
}

func newLegacyConsumer(client *sqs.Client, url string, vt, extensionLimit int) *consumer {
	return &consumer{
		sqs: client, QueueURL: url, VisibilityTimeout: vt, extensionLimit: extensionLimit,
		workerPool: 1, maxNumberOfMessages: maxMessages, waitTimeSeconds: 1,
		messageHandlerName: "route", logger: discardLogger{},
	}
}

type discardLogger struct{}

func (discardLogger) Println(...interface{}) {}

func sqsMessage(id, receipt string) types.Message {
	return types.Message{MessageId: aws.String(id), ReceiptHandle: aws.String(receipt), Body: aws.String(`{}`)}
}

func testMessage(route string) *message {
	return newMessage(sqsMessage("m1", "rh-m1"), route)
}

func TestWithRecovery(t *testing.T) {
	boom := errors.New("boom")
	recoverFn := func() { _ = recover() }
	tests := []struct {
		name    string
		h       Handler
		wantErr error
	}{
		{"success", func(context.Context, Message) error { return nil }, nil},
		{"handler error passes through", func(context.Context, Message) error { return boom }, boom},
		{"panic becomes ErrPanic", func(context.Context, Message) error { panic("bad") }, ErrPanic},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := WithRecovery(recoverFn)(tt.h)(context.Background(), testMessage("route"))
			if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil && err != nil) {
				t.Errorf("err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestConsumerRun(t *testing.T) {
	boom := errors.New("boom")
	recoverFn := func() { _ = recover() }
	tests := []struct {
		name        string
		route       string // route on the message; the handler is registered under "route"
		h           Handler
		wantDeleted bool
		wantErr     func(error) bool
	}{
		{
			name: "success deletes", route: "route",
			h:           func(context.Context, Message) error { return nil },
			wantDeleted: true, wantErr: func(err error) bool { return err == nil },
		},
		{
			name: "handler error keeps message", route: "route",
			h:       func(context.Context, Message) error { return boom },
			wantErr: func(err error) bool { return errors.Is(err, boom) },
		},
		{
			name: "recovered panic keeps message", route: "route",
			h:       func(context.Context, Message) error { panic("bad") },
			wantErr: func(err error) bool { return errors.Is(err, ErrPanic) },
		},
		{
			name: "unknown route keeps message", route: "other",
			h:       func(context.Context, Message) error { return nil },
			wantErr: func(err error) bool { return isSQSErr(err, ErrNoRoute) },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, client, url := newSQSServer(t)
			c := newLegacyConsumer(client, url, 30, 2)
			c.RegisterHandler("route", tt.h, WithRecovery(recoverFn))
			if err := c.run(testMessage(tt.route)); !tt.wantErr(err) {
				t.Errorf("run() err = %v", err)
			}
			if got := srv.count("DeleteMessage") == 1; got != tt.wantDeleted {
				t.Errorf("deleted = %v, want %v", got, tt.wantDeleted)
			}
		})
	}
}

func TestConsumerRun_Heartbeat(t *testing.T) {
	tests := []struct {
		name           string
		vt             int
		extensionLimit int
		handlerTime    time.Duration
		minBeats       int
		maxBeats       int
	}{
		// VT=3s beats every 1s. 3.5s covers two beats with slack for a third or
		// fourth if the goroutine or the sleep runs long. run waits for the
		// heartbeat goroutine to exit before this assertion.
		{"beats every VT/3 while the handler runs", 3, 2, 3500 * time.Millisecond, 2, 4},
		// VT=1, L=1: slack clamps to 0, budget is (L+1)*VT = 2s, which allows
		// exactly 3 beats. The 4th, at ~1.33s, would push past 2s.
		{"stops at the hold budget", 1, 1, 2 * time.Second, 3, 3},
		{"ExtensionLimit 0 disables it", 1, 0, 1 * time.Second, 0, 0},
		{"fast handler never beats", 3, 2, 10 * time.Millisecond, 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv, client, url := newSQSServer(t)
			c := newLegacyConsumer(client, url, tt.vt, tt.extensionLimit)
			c.RegisterHandler("route", func(context.Context, Message) error { time.Sleep(tt.handlerTime); return nil })
			if err := c.run(testMessage("route")); err != nil {
				t.Fatalf("run() err = %v", err)
			}
			srv.mu.Lock()
			defer srv.mu.Unlock()
			if n := len(srv.visibility); n < tt.minBeats || n > tt.maxBeats {
				t.Errorf("ChangeMessageVisibility calls = %d, want %d..%d", n, tt.minBeats, tt.maxBeats)
			}
			for _, v := range srv.visibility {
				if v != int32(tt.vt) {
					t.Errorf("VisibilityTimeout = %d, want %d (absolute, not cumulative)", v, tt.vt)
				}
			}
		})
	}
}

func TestHoldBudget(t *testing.T) {
	vt := 30 * time.Second
	if got := holdBudget(vt, 2); got != 130*time.Second {
		t.Fatalf("budget = %v, want 130s", got)
	}
	beats := gridBeats(vt, 130*time.Second)
	if len(beats) != 10 || beats[0] != 10*time.Second || beats[len(beats)-1] != 100*time.Second {
		t.Fatalf("allowed beats = %v, want 10s..100s", beats)
	}
	// VT <= 10: the old sleeper's VT-10 delay was immediate, so the budget is (L+1)*VT.
	if got := holdBudget(10*time.Second, 2); got != 30*time.Second {
		t.Errorf("VT=10 budget = %v, want 30s", got)
	}
	if got := holdBudget(time.Second, 1); got != 2*time.Second {
		t.Errorf("VT=1 L=1 budget = %v, want 2s", got)
	}
	if n := len(gridBeats(time.Second, holdBudget(time.Second, 1))); n != 3 {
		t.Errorf("VT=1 L=1 allows %d beats, want 3", n)
	}
	// The shortest VT that beats once more than the old 3*L cap (L=1 => 3).
	vt = 15 * time.Second
	if n := len(gridBeats(vt, holdBudget(vt, 1))); n != 4 {
		t.Errorf("VT=15 L=1 allows %d beats, want 4", n)
	}
	// VT=14 L=1: the old sleeper held until 32s. A VT/3 grid last accepts a
	// beat at 14s, which lapses at 28s. The off-grid beat reaches the budget.
	vt = 14 * time.Second
	budget := holdBudget(vt, 1)
	if budget != 32*time.Second {
		t.Fatalf("VT=14 L=1 budget = %v, want 32s", budget)
	}
	grid := gridBeats(vt, budget)
	if len(grid) == 0 || grid[len(grid)-1]+vt >= budget-time.Second {
		t.Fatalf("grid beats %v already reach budget %v", grid, budget)
	}
	elapsed := time.Duration(0)
	next := vt / 3
	var taken []time.Duration
	for range 8 {
		wait, ok := nextHeartbeatDelay(elapsed, next, vt, budget, heartbeatSkew)
		if !ok {
			break
		}
		elapsed += wait
		taken = append(taken, elapsed)
		next += vt / 3
	}
	if len(taken) != len(grid)+1 {
		t.Fatalf("scheduled beats = %v, want the grid plus one beat at the budget", taken)
	}
	if end := taken[len(taken)-1] + vt; end < budget-heartbeatSkew || end > budget {
		t.Fatalf("invisibility ends at %v, want %v", end, budget)
	}
	if heartbeatCallTimeout(30*time.Second) != 5*time.Second {
		t.Errorf("timeout(30s) = %v, want 5s", heartbeatCallTimeout(30*time.Second))
	}
	if heartbeatCallTimeout(3*time.Second) != time.Second {
		t.Errorf("timeout(3s) = %v, want 1s", heartbeatCallTimeout(3*time.Second))
	}
	if heartbeatCallTimeout(time.Second) != time.Second/3 {
		t.Errorf("timeout(1s) = %v, want VT/3", heartbeatCallTimeout(time.Second))
	}
}

// gridBeats lists the VisibilityTimeout/3 instants that still fit in budget.
func gridBeats(vt, budget time.Duration) []time.Duration {
	var beats []time.Duration
	for beat := 1; beat < 30; beat++ {
		at := time.Duration(beat) * (vt / 3)
		if beatHoldsPastBudget(at, vt, budget) {
			break
		}
		beats = append(beats, at)
	}
	return beats
}

func TestConsumerRun_HeartbeatRespectsOldBudget(t *testing.T) {
	// VT=15 L=1 schedules beats at 5, 10, 15 and 20s. The old 3*L cap would
	// have stopped at 3. The 5th beat (25s) is past the 35s budget.
	t.Parallel()
	srv, client, url := newSQSServer(t)
	c := newLegacyConsumer(client, url, 15, 1)
	c.RegisterHandler("route", func(context.Context, Message) error {
		time.Sleep(24 * time.Second)
		return nil
	})
	if err := c.run(testMessage("route")); err != nil {
		t.Fatalf("run() err = %v", err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if n := len(srv.visibility); n < 4 || n > 4 {
		t.Errorf("ChangeMessageVisibility calls = %d, want 4 (past 3*ExtensionLimit, short of the budget)", n)
	}
	for _, v := range srv.visibility {
		if v != 15 {
			t.Errorf("VisibilityTimeout = %d, want 15", v)
		}
	}
}

func TestConsumerRun_ShortVisibilityReachesOldBudget(t *testing.T) {
	// VT=14 L=1. The old sleeper woke at 4s and set visibility to 28s, so the
	// receipt stayed invisible until t=32s. The VT/3 grid's last accepted beat
	// is at 14s and would lapse at 28s, leaving a 30s handler outside the hold.
	// The elapsed-time stop takes one more beat so the handler is still inside.
	t.Parallel()
	srv, client, url := newSQSServer(t)
	c := newLegacyConsumer(client, url, 14, 1)
	c.RegisterHandler("route", func(context.Context, Message) error {
		time.Sleep(22 * time.Second)
		return nil
	})
	if err := c.run(testMessage("route")); err != nil {
		t.Fatalf("run() err = %v", err)
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if n := len(srv.visibility); n != 4 {
		t.Errorf("ChangeMessageVisibility calls = %d, want 4 (the grid alone stops at 3)", n)
	}
	for _, v := range srv.visibility {
		if v != 14 {
			t.Errorf("VisibilityTimeout = %d, want 14", v)
		}
	}
}

func TestConsumerRun_PanicStopsHeartbeat(t *testing.T) {
	srv, client, url := newSQSServer(t)
	c := newLegacyConsumer(client, url, 1, 5) // would beat for seconds if done stayed open
	c.RegisterHandler("route", func(context.Context, Message) error { panic("bad") })
	func() {
		defer func() { _ = recover() }()
		_ = c.run(testMessage("route"))
	}()
	n := srv.count("ChangeMessageVisibility")
	time.Sleep(800 * time.Millisecond)
	if got := srv.count("ChangeMessageVisibility"); got != n {
		t.Errorf("heartbeat continued after panic: %d -> %d", n, got)
	}
}

func TestConsumerRun_HeartbeatContinuesAfterExtendError(t *testing.T) {
	srv, client, url := newSQSServer(t)
	srv.failVis = 1
	c := newLegacyConsumer(client, url, 3, 2)
	c.RegisterHandler("route", func(context.Context, Message) error {
		time.Sleep(3500 * time.Millisecond)
		return nil
	})
	if err := c.run(testMessage("route")); err != nil {
		t.Fatalf("run() err = %v", err)
	}
	if n := srv.count("ChangeMessageVisibility"); n < 2 {
		t.Errorf("calls = %d, want >= 2 after a 500", n)
	}
}

func TestConsumerHeartbeat_PerCallTimeout(t *testing.T) {
	srv, client, url := newSQSServer(t)
	srv.hangVis = true
	c := newLegacyConsumer(client, url, 3, 2) // call timeout is min(5s, VT/3) = 1s
	c.RegisterHandler("route", func(context.Context, Message) error {
		time.Sleep(1500 * time.Millisecond)
		return nil
	})
	finished := make(chan struct{})
	go func() {
		_ = c.run(testMessage("route"))
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(8 * time.Second):
		t.Fatal("run blocked in ChangeMessageVisibility; the call needs its own timeout")
	}
	srv.mu.Lock()
	defer srv.mu.Unlock()
	if len(srv.visHang) == 0 {
		t.Fatal("visibility call did not observe client cancel")
	}
	for _, d := range srv.visHang {
		if d < 200*time.Millisecond || d > 2*time.Second {
			t.Errorf("visibility call hung for %v, want about 1s (min(5s, VT/3))", d)
		}
	}
}

func receiveBody(id, receipt, route string) string {
	return `{"Messages":[{"MessageId":"` + id + `","ReceiptHandle":"` + receipt + `","Body":"{}","MessageAttributes":{"route":{"DataType":"String","StringValue":"` + route + `"}}}]}`
}

func TestConsume_RouteSelection(t *testing.T) {
	tests := []struct {
		name        string
		handlerName string
		attr        string
		want        string
	}{
		{"attribute when name is empty", "", "post_created", "post_created"},
		{"name wins over attribute", "fixed", "post_created", "fixed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv, client, url := newSQSServer(t)
			srv.recv = []string{receiveBody("m1", "rh-1", tt.attr)}
			srv.blockEmptyRecv = true
			c := newLegacyConsumer(client, url, 30, 0) // no heartbeat
			c.messageHandlerName = tt.handlerName
			got := make(chan string, 1)
			c.RegisterHandler(tt.want, func(_ context.Context, m Message) error {
				got <- m.Route()
				return nil
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() { c.Consume(ctx); close(done) }()
			select {
			case route := <-got:
				if route != tt.want {
					t.Errorf("route = %q, want %q", route, tt.want)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("handler was not called through Consume")
			}
			deadline := time.Now().Add(2 * time.Second)
			for srv.count("DeleteMessage") == 0 && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			cancel()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("Consume did not return after cancel")
			}
			if n := srv.count("DeleteMessage"); n != 1 {
				t.Errorf("DeleteMessage calls = %d, want 1", n)
			}
		})
	}
}

func TestConsume_ReceiveErrorSleepRespectsCtx(t *testing.T) {
	srv, client, url := newSQSServer(t)
	srv.failRecv = true
	c := newLegacyConsumer(client, url, 30, 2)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Consume(ctx); close(done) }()
	for srv.count("ReceiveMessage") == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Consume still sleeping 2s after cancel; the 10s retry sleep must respect ctx")
	}
	if n := srv.count("ReceiveMessage"); n != 1 {
		t.Errorf("ReceiveMessage calls = %d, want 1", n)
	}
}

func TestNewConsumer_WaitTimeSeconds(t *testing.T) {
	tests := []struct {
		name string
		in   int
		want int32
	}{
		{"unset defaults to 20s long poll", 0, 20},
		{"explicit value kept", 5, 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewConsumer(Config{Key: "k", Secret: "s", Region: "us-east-1", QueueURL: "https://sqs.local/q", WaitTimeSeconds: tt.in}, "q")
			if err != nil {
				t.Fatal(err)
			}
			if got := c.(*consumer).waitTimeSeconds; got != tt.want {
				t.Errorf("waitTimeSeconds = %d, want %d", got, tt.want)
			}
		})
	}
}
