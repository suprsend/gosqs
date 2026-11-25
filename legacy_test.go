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
	mu         sync.Mutex
	calls      map[string]int // action -> count
	visibility []int32        // VisibilityTimeout of each ChangeMessageVisibility
	failRecv   bool           // ReceiveMessage returns a 500
}

func newSQSServer(t *testing.T) (*sqsServer, *sqs.Client, string) {
	t.Helper()
	s := &sqsServer{calls: map[string]int{}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		action := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "AmazonSQS.")
		body, _ := io.ReadAll(r.Body)
		var in map[string]any
		_ = json.Unmarshal(body, &in)
		s.mu.Lock()
		s.calls[action]++
		fail := s.failRecv && action == "ReceiveMessage"
		switch action {
		case "ChangeMessageVisibility":
			v, _ := in["VisibilityTimeout"].(float64)
			s.visibility = append(s.visibility, int32(v))
		}
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/x-amz-json-1.0")
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"__type":"com.amazonaws.sqs#InternalError","message":"boom"}`))
			return
		}
		_, _ = w.Write([]byte(`{}`))
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
		// VT=3s beats every 1s, setting visibility back to 3s each time. The old
		// extender slept VT-10s, i.e. never fired in time at VT<=10.
		{"beats every VT/3 while the handler runs", 3, 2, 2500 * time.Millisecond, 2, 2},
		{"stops after 3*ExtensionLimit beats", 1, 1, 2 * time.Second, 3, 3},
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
