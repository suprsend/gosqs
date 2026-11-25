package gosqs

// Batch consumer: receives from one queue, hands batches of records to a
// BatchHandler and acks them with DeleteMessageBatch. It lives next to the
// per-message Consumer and does not change it.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// API is the subset of *sqs.Client the batch consumer uses. *sqs.Client satisfies it;
// tests pass a fake.
type API interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(ctx context.Context, in *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
	ChangeMessageVisibilityBatch(ctx context.Context, in *sqs.ChangeMessageVisibilityBatchInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error)
}

var _ API = (*sqs.Client)(nil)

// Record is one received message. *Record also satisfies Message, so
// per-message handlers keep working through PerMessage.
type Record struct {
	ID            string
	ReceiptHandle string
	GroupID       string // FIFO only, "" on standard queues
	Data          []byte
	Attributes    map[string]string // string-typed message attributes
	ReceiveCount  int
	SentAt        time.Time
	receivedAt    time.Time
}

func (r *Record) Body() []byte                 { return r.Data }
func (r *Record) MessageId() string            { return r.ID }
func (r *Record) Attribute(key string) string  { return r.Attributes[key] }
func (r *Record) Route() string                { return r.Attributes["route"] }
func (r *Record) Decode(out interface{}) error { return json.Unmarshal(r.Data, out) }
func (r *Record) DecodeModified(body, changes interface{}) error {
	return r.Decode(&struct{ Body, Changes interface{} }{body, changes})
}

// Failure marks one record as not processed. It is not deleted.
type Failure struct {
	ID         string
	Err        error
	RetryAfter time.Duration // 0: redeliver when the current visibility lapses
}

// Result reports per-record failures. The zero value means every record succeeded.
type Result struct{ Failures []Failure }

// FailAll fails every record in recs with err.
func FailAll(recs []*Record, err error) Result {
	f := make([]Failure, len(recs))
	for i, r := range recs {
		f[i] = Failure{ID: r.ID, Err: err}
	}
	return Result{Failures: f}
}

// BatchHandler processes a batch. Records of one FIFO group arrive in queue order.
type BatchHandler func(ctx context.Context, recs []*Record) Result

// Errors reported in Failure.Err by the batch consumer and its adapters.
var (
	ErrPanic        = errors.New("gosqs: handler panicked") // also returned by WithRecovery
	ErrUnknownID    = errors.New("gosqs: handler reported an id not in the batch")
	ErrGroupBlocked = errors.New("gosqs: earlier message in the FIFO group failed")
	ErrDecode       = errors.New("gosqs: decode failed")
)

// Options tune the batch consumer. Zero values take the defaults in withDefaults.
type Options struct {
	MaxBatch     int           // records per handler call; may exceed 10. Default 10.
	Linger       time.Duration // flush a partial batch after this. Default 200ms.
	Workers      int           // concurrent handler calls. Default 4.
	MaxInFlight  int           // received but not yet acked. Default Workers*MaxBatch + MaxBatch.
	Receivers    int           // concurrent ReceiveMessage loops. Default 1.
	WaitTime     time.Duration // long poll, 1s..20s. Default 20s.
	Visibility   time.Duration // set on receive and on each heartbeat. Default 30s.
	Heartbeat    time.Duration // extend in-flight receipts this often. Default Visibility/3.
	MaxExtend    time.Duration // stop extending a record this long after receipt. Default 15m.
	DrainTimeout time.Duration // after ctx is done, wait this long before cancelling handlers. Default 30s.
	AckTimeout   time.Duration // per delete/visibility call. Default 10s.
	FIFO         *bool         // default: queue URL ends in ".fifo"
	Logger       *slog.Logger
}

func (o Options) withDefaults(queueURL string) (Options, error) {
	def := func(v *int, d int) {
		if *v <= 0 {
			*v = d
		}
	}
	defD := func(v *time.Duration, d time.Duration) {
		if *v <= 0 {
			*v = d
		}
	}
	def(&o.MaxBatch, 10)
	def(&o.Workers, 4)
	def(&o.Receivers, 1)
	def(&o.MaxInFlight, o.Workers*o.MaxBatch+o.MaxBatch)
	defD(&o.Linger, 200*time.Millisecond)
	defD(&o.WaitTime, 20*time.Second)
	defD(&o.Visibility, 30*time.Second)
	defD(&o.Heartbeat, o.Visibility/3)
	defD(&o.MaxExtend, 15*time.Minute)
	defD(&o.DrainTimeout, 30*time.Second)
	defD(&o.AckTimeout, 10*time.Second)
	if o.FIFO == nil {
		o.FIFO = aws.Bool(strings.HasSuffix(queueURL, ".fifo"))
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	switch {
	case queueURL == "":
		return o, errors.New("gosqs: empty queue URL")
	case o.WaitTime > 20*time.Second:
		return o, fmt.Errorf("gosqs: WaitTime %v exceeds 20s", o.WaitTime)
	case o.Visibility < time.Second || o.Heartbeat >= o.Visibility:
		return o, fmt.Errorf("gosqs: need 1s <= Heartbeat(%v) < Visibility(%v)", o.Heartbeat, o.Visibility)
	case o.Linger >= o.Visibility:
		return o, fmt.Errorf("gosqs: Linger %v must be well under Visibility %v", o.Linger, o.Visibility)
	case o.MaxInFlight < o.MaxBatch:
		return o, fmt.Errorf("gosqs: MaxInFlight %d < MaxBatch %d would never fill a batch", o.MaxInFlight, o.MaxBatch)
	}
	return o, nil
}

// BatchConsumer polls one queue and dispatches batches to a BatchHandler.
type BatchConsumer struct {
	api API
	url string
	h   BatchHandler
	o   Options
	sem chan struct{} // MaxInFlight tokens; one per received, un-acked record

	mu       sync.Mutex
	inflight map[*Record]struct{}
	// hbMu is held for a whole heartbeat round and around a RetryAfter
	// backoff, so the two never interleave.
	hbMu sync.Mutex
}

// New returns a batch consumer for queueURL. api is usually an *sqs.Client.
// The queue is treated as FIFO when the URL ends in ".fifo", unless o.FIFO is set.
func New(api API, queueURL string, h BatchHandler, o Options) (*BatchConsumer, error) {
	if api == nil || h == nil {
		return nil, errors.New("gosqs: nil API or BatchHandler")
	}
	o, err := o.withDefaults(queueURL)
	if err != nil {
		return nil, err
	}
	return &BatchConsumer{api: api, url: queueURL, h: h, o: o,
		sem: make(chan struct{}, o.MaxInFlight), inflight: map[*Record]struct{}{}}, nil
}

// Run blocks until ctx is done and every received record is acked, failed, or
// the drain timeout cancels the handlers. Handlers get a context that outlives
// ctx by DrainTimeout, so a SIGTERM does not abort work half way.
func (c *BatchConsumer) Run(ctx context.Context) error {
	base := context.WithoutCancel(ctx)
	handlerCtx, cancelHandlers := context.WithCancel(base)
	defer cancelHandlers()
	hbCtx, stopHB := context.WithCancel(base)
	var hb sync.WaitGroup
	hb.Go(func() { c.heartbeat(hbCtx) })

	in := make(chan *Record, c.o.MaxBatch)
	batches := make(chan []*Record)

	var recv sync.WaitGroup
	for range c.o.Receivers {
		recv.Go(func() { c.receive(ctx, in) })
	}
	go func() { recv.Wait(); close(in) }()

	var workers sync.WaitGroup
	for range c.o.Workers {
		workers.Go(func() {
			for b := range batches {
				c.process(handlerCtx, b)
			}
		})
	}

	// The drain clock starts when ctx is done, not when the last batch is
	// handed off: with every worker busy, the hand-off itself waits.
	drained := make(chan struct{})
	go func() {
		select {
		case <-drained:
			return
		case <-ctx.Done():
		}
		t := time.NewTimer(c.o.DrainTimeout)
		defer t.Stop()
		select {
		case <-drained:
		case <-t.C:
			c.o.Logger.Warn("gosqs: drain timeout, cancelling handlers")
			cancelHandlers()
		}
	}()

	c.accumulate(in, batches) // returns once receivers stop and the last batch is handed off
	workers.Wait()
	close(drained)
	stopHB()
	hb.Wait()
	return nil
}

func (c *BatchConsumer) acquire(ctx context.Context, max int) (int, bool) {
	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		return 0, false
	}
	n := 1
	for n < max {
		select {
		case c.sem <- struct{}{}:
			n++
		default:
			return n, true
		}
	}
	return n, true
}

func (c *BatchConsumer) release(n int) {
	for range n {
		<-c.sem
	}
}

func (c *BatchConsumer) receive(ctx context.Context, out chan<- *Record) {
	delay := 100 * time.Millisecond
	for ctx.Err() == nil {
		n, ok := c.acquire(ctx, 10)
		if !ok {
			return
		}
		res, err := c.api.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:                    aws.String(c.url),
			MaxNumberOfMessages:         int32(n),
			WaitTimeSeconds:             int32(c.o.WaitTime / time.Second),
			VisibilityTimeout:           int32(c.o.Visibility / time.Second),
			MessageAttributeNames:       []string{"All"},
			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameAll},
		})
		if err != nil {
			c.release(n)
			if ctx.Err() != nil {
				return
			}
			c.o.Logger.Error("gosqs: receive failed", "err", err, "retry_in", delay)
			if !sleep(ctx, delay) {
				return
			}
			delay = min(delay*2, 10*time.Second)
			continue
		}
		delay = 100 * time.Millisecond
		c.release(n - len(res.Messages))
		now := time.Now()
		for _, m := range res.Messages {
			r := toRecord(m, now)
			c.track(r)
			out <- r // the accumulator drains `in` until it is closed
		}
	}
}

func (c *BatchConsumer) accumulate(in <-chan *Record, out chan<- []*Record) {
	defer close(out)
	var buf []*Record
	var timer *time.Timer
	var timerC <-chan time.Time
	flush := func() {
		if timer != nil {
			timer.Stop()
			timerC = nil
		}
		if len(buf) > 0 {
			out <- buf // blocks while all workers are busy: backpressure
			buf = nil
		}
	}
	for {
		select {
		case r, ok := <-in:
			if !ok {
				flush()
				return
			}
			buf = append(buf, r)
			if len(buf) == 1 {
				timer = time.NewTimer(c.o.Linger)
				timerC = timer.C
			}
			if len(buf) >= c.o.MaxBatch {
				flush()
			}
		case <-timerC:
			flush()
		}
	}
}

func (c *BatchConsumer) process(ctx context.Context, b []*Record) {
	start := time.Now()
	res := c.call(ctx, b)
	failed := c.failures(b, res)

	var ack []*Record
	var backoff []Failure
	for _, r := range b {
		f, bad := failed[r.ID]
		switch {
		case !bad:
			ack = append(ack, r)
		case f.RetryAfter > 0:
			backoff = append(backoff, f)
		}
	}
	// Stop heartbeating first, so a heartbeat cannot undo a backoff.
	c.untrack(b)
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.o.AckTimeout)
	defer cancel()
	c.deleteAll(actx, ack)
	if len(backoff) > 0 {
		// Under hbMu, so a heartbeat round that still holds these receipts
		// cannot land after the backoff and overwrite it.
		c.hbMu.Lock()
		c.backoffAll(actx, b, backoff)
		c.hbMu.Unlock()
	}
	c.release(len(b))
	c.o.Logger.Debug("gosqs: batch done", "size", len(b), "acked", len(ack),
		"failed", len(failed), "took", time.Since(start))
}

func (c *BatchConsumer) call(ctx context.Context, b []*Record) (res Result) {
	defer func() {
		if p := recover(); p != nil {
			c.o.Logger.Error("gosqs: handler panic", "panic", p, "stack", string(debug.Stack()))
			res = FailAll(b, fmt.Errorf("%w: %v", ErrPanic, p))
		}
	}()
	return c.h(ctx, b)
}

// failures indexes the handler's failures and applies two safety rules:
// an unknown ID fails the whole batch, and on FIFO queues a failure blocks the
// rest of its group, so a later message is never deleted before an earlier one.
func (c *BatchConsumer) failures(b []*Record, res Result) map[string]Failure {
	ids := make(map[string]bool, len(b))
	for _, r := range b {
		ids[r.ID] = true
	}
	failed := make(map[string]Failure, len(res.Failures))
	for _, f := range res.Failures {
		if !ids[f.ID] {
			c.o.Logger.Error("gosqs: unknown failure id, failing batch", "id", f.ID)
			return c.failures(b, FailAll(b, ErrUnknownID))
		}
		failed[f.ID] = f
	}
	if !*c.o.FIFO {
		return failed
	}
	blocked := map[string]Failure{}
	for _, r := range b {
		if f, ok := blocked[r.GroupID]; ok {
			if _, own := failed[r.ID]; !own {
				failed[r.ID] = Failure{ID: r.ID, Err: ErrGroupBlocked, RetryAfter: f.RetryAfter}
			}
			continue
		}
		if f, ok := failed[r.ID]; ok {
			blocked[r.GroupID] = f
		}
	}
	return failed
}

func (c *BatchConsumer) deleteAll(ctx context.Context, recs []*Record) {
	for chunk := range slices.Chunk(recs, 10) {
		entries := make([]types.DeleteMessageBatchRequestEntry, len(chunk))
		for i, r := range chunk {
			entries[i] = types.DeleteMessageBatchRequestEntry{Id: aws.String(strconv.Itoa(i)), ReceiptHandle: aws.String(r.ReceiptHandle)}
		}
		// One retry for the whole call and for retryable per-entry failures.
		for attempt := 0; attempt < 2 && len(entries) > 0; attempt++ {
			out, err := c.api.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: aws.String(c.url), Entries: entries})
			if err != nil {
				c.o.Logger.Error("gosqs: delete batch failed", "err", err, "n", len(entries))
				continue
			}
			entries = retryable(entries, out.Failed)
		}
		if len(entries) > 0 {
			c.o.Logger.Error("gosqs: records not deleted, will be redelivered", "n", len(entries))
		}
	}
}

func retryable(sent []types.DeleteMessageBatchRequestEntry, failed []types.BatchResultErrorEntry) []types.DeleteMessageBatchRequestEntry {
	var again []types.DeleteMessageBatchRequestEntry
	for _, f := range failed {
		if f.SenderFault { // e.g. ReceiptHandleIsInvalid: retrying cannot help
			continue
		}
		for _, e := range sent {
			if aws.ToString(e.Id) == aws.ToString(f.Id) {
				again = append(again, e)
			}
		}
	}
	return again
}

func (c *BatchConsumer) backoffAll(ctx context.Context, b []*Record, fs []Failure) {
	byID := make(map[string]*Record, len(b))
	for _, r := range b {
		byID[r.ID] = r
	}
	for chunk := range slices.Chunk(fs, 10) {
		entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, len(chunk))
		for i, f := range chunk {
			secs := min(int32(f.RetryAfter/time.Second), 12*60*60)
			entries[i] = types.ChangeMessageVisibilityBatchRequestEntry{
				Id: aws.String(strconv.Itoa(i)), ReceiptHandle: aws.String(byID[f.ID].ReceiptHandle), VisibilityTimeout: secs,
			}
		}
		if _, err := c.api.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: aws.String(c.url), Entries: entries}); err != nil {
			c.o.Logger.Warn("gosqs: backoff failed, falling back to visibility timeout", "err", err)
		}
	}
}

// heartbeat extends every in-flight receipt in batches of 10. One goroutine
// covers all records, from receipt (not dispatch) until ack.
func (c *BatchConsumer) heartbeat(ctx context.Context) {
	t := time.NewTicker(c.o.Heartbeat)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		c.extendInFlight(ctx)
	}
}

func (c *BatchConsumer) extendInFlight(ctx context.Context) {
	c.hbMu.Lock()
	defer c.hbMu.Unlock()
	now := time.Now()
	c.mu.Lock()
	var due []*Record
	for r := range c.inflight {
		if now.Sub(r.receivedAt) < c.o.MaxExtend {
			due = append(due, r)
		}
	}
	c.mu.Unlock()
	for chunk := range slices.Chunk(due, 10) {
		entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, len(chunk))
		for i, r := range chunk {
			entries[i] = types.ChangeMessageVisibilityBatchRequestEntry{
				Id: aws.String(strconv.Itoa(i)), ReceiptHandle: aws.String(r.ReceiptHandle),
				VisibilityTimeout: int32(c.o.Visibility / time.Second),
			}
		}
		cctx, cancel := context.WithTimeout(ctx, c.o.AckTimeout)
		_, err := c.api.ChangeMessageVisibilityBatch(cctx, &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: aws.String(c.url), Entries: entries})
		cancel()
		if err != nil && ctx.Err() == nil {
			c.o.Logger.Warn("gosqs: heartbeat failed", "err", err, "n", len(entries))
		}
	}
}

func (c *BatchConsumer) track(r *Record) { c.mu.Lock(); c.inflight[r] = struct{}{}; c.mu.Unlock() }
func (c *BatchConsumer) untrack(b []*Record) {
	c.mu.Lock()
	for _, r := range b {
		delete(c.inflight, r)
	}
	c.mu.Unlock()
}

func toRecord(m types.Message, now time.Time) *Record {
	r := &Record{
		ID: aws.ToString(m.MessageId), ReceiptHandle: aws.ToString(m.ReceiptHandle),
		Data: []byte(aws.ToString(m.Body)), Attributes: make(map[string]string, len(m.MessageAttributes)),
		GroupID: m.Attributes[string(types.MessageSystemAttributeNameMessageGroupId)], receivedAt: now,
	}
	for k, v := range m.MessageAttributes {
		if v.StringValue != nil {
			r.Attributes[k] = *v.StringValue
		}
	}
	r.ReceiveCount, _ = strconv.Atoi(m.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	if ms, err := strconv.ParseInt(m.Attributes[string(types.MessageSystemAttributeNameSentTimestamp)], 10, 64); err == nil {
		r.SentAt = time.UnixMilli(ms)
	}
	return r
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
