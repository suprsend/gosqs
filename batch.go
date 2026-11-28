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
	// groupSeq is the FIFO arrival order of this record within GroupID.
	// Assigned when the record is batched; zero for standard queues.
	groupSeq int
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
	ID  string
	Err error
	// RetryAfter delays redelivery. 0 leaves the current visibility to lapse.
	// SQS visibility is whole seconds and treats 0 as immediately visible, so a
	// positive duration below 1s is sent as 1s.
	RetryAfter time.Duration
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
	// errNotDeleted is recorded against a receipt DeleteMessageBatch did not
	// remove, so the FIFO gate holds the rest of the group. It stays heartbeated.
	errNotDeleted = errors.New("gosqs: delete failed")
)

// Options tune the batch consumer. Zero values take the defaults in withDefaults.
type Options struct {
	MaxBatch int // records per handler call; may exceed 10. Default 10.
	// Linger flushes a partial batch after this. Default 200ms. A value of at
	// least 1s is stored in whole seconds, the same truncation SQS applies to
	// Visibility, so the timer cannot outlast the receipt.
	Linger      time.Duration
	Workers     int // concurrent handler calls. Default 4.
	MaxInFlight int // received but not yet acked. Default Workers*MaxBatch + MaxBatch.
	Receivers   int // concurrent ReceiveMessage loops. Default 1.
	// WaitTime is the long-poll wait. SQS takes whole seconds in 1..20.
	// A value in (0, 1s) is rounded up to 1s, because 0 would short-poll.
	// Default 20s.
	WaitTime time.Duration
	// Visibility is set on receive and on each heartbeat. SQS takes whole
	// seconds in 1..43200 (12h). The stored value is truncated to seconds.
	// Default 30s.
	Visibility time.Duration
	// Heartbeat extends in-flight receipts this often. It is stored in whole
	// seconds, must be at least 1s, and must be at least 1s under Visibility.
	// Default Visibility/3, raised to 1s when that would be shorter.
	Heartbeat    time.Duration
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
	heartbeatSet := o.Heartbeat > 0
	defD(&o.Linger, 200*time.Millisecond)
	defD(&o.WaitTime, 20*time.Second)
	defD(&o.Visibility, 30*time.Second)
	defD(&o.Heartbeat, o.Visibility/3)
	defD(&o.MaxExtend, 15*time.Minute)
	defD(&o.DrainTimeout, 30*time.Second)
	defD(&o.AckTimeout, 10*time.Second)
	// A computed default under a second would fail the check below. Callers
	// who set Heartbeat themselves still get that error.
	if !heartbeatSet && o.Heartbeat < time.Second {
		o.Heartbeat = time.Second
	}
	// Truncate after defaults so a sub-second WaitTime cannot become a short poll.
	if o.WaitTime < time.Second {
		o.WaitTime = time.Second
	}
	if o.FIFO == nil {
		o.FIFO = aws.Bool(strings.HasSuffix(queueURL, ".fifo"))
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	// SQS applies Visibility in whole seconds, and the heartbeat period is
	// compared the same way. Store those truncated values so the ticker and
	// the queue timeout cannot drift back together. Linger of at least one
	// second is stored the same way; a sub-second linger is a local flush
	// delay (the default is 200ms) and stays as given.
	waitSec := int64(o.WaitTime / time.Second)
	visSec := int64(o.Visibility / time.Second)
	hbSec := int64(o.Heartbeat / time.Second)
	switch {
	case queueURL == "":
		return o, errors.New("gosqs: empty queue URL")
	case waitSec < 1 || waitSec > 20:
		return o, fmt.Errorf("gosqs: WaitTime %v is %ds after truncation, want 1..20", o.WaitTime, waitSec)
	case visSec < 1 || visSec > 12*60*60:
		return o, fmt.Errorf("gosqs: Visibility %v is %ds after truncation, want 1..43200", o.Visibility, visSec)
	case hbSec < 1:
		return o, fmt.Errorf("gosqs: Heartbeat %v is %ds after truncation, want at least 1s", o.Heartbeat, hbSec)
	case hbSec >= visSec:
		return o, fmt.Errorf("gosqs: Heartbeat %v (%ds) must be at least 1s under Visibility %v (%ds)", o.Heartbeat, hbSec, o.Visibility, visSec)
	case o.MaxInFlight < o.MaxBatch:
		return o, fmt.Errorf("gosqs: MaxInFlight %d < MaxBatch %d would never fill a batch", o.MaxInFlight, o.MaxBatch)
	}
	o.Visibility = time.Duration(visSec) * time.Second
	o.Heartbeat = time.Duration(hbSec) * time.Second
	if o.Linger >= time.Second {
		o.Linger = time.Duration(o.Linger/time.Second) * time.Second
	}
	if o.Linger >= o.Visibility {
		return o, fmt.Errorf("gosqs: Linger %v must be well under Visibility %v", o.Linger, o.Visibility)
	}
	return o, nil
}

// visibilitySeconds converts RetryAfter to an SQS visibility timeout.
// Zero stays zero. Any positive duration under a second becomes 1, so the
// call never sends 0 (immediately visible). Longer values truncate, capped at 12h.
func visibilitySeconds(d time.Duration) int32 {
	if d <= 0 {
		return 0
	}
	secs := int32(d / time.Second)
	if secs < 1 {
		secs = 1
	}
	const maxVis int32 = 12 * 60 * 60
	if secs > maxVis {
		return maxVis
	}
	return secs
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
	// hbMu is held for a whole heartbeat round, around a RetryAfter backoff,
	// and around the visibility refresh that precedes a delete, so those
	// never interleave.
	hbMu sync.Mutex

	gatesMu sync.Mutex
	gates   map[string]*groupGate
}

// groupGate keeps one FIFO MessageGroupId ordered across batches. seq numbers
// follow receive order, so a batch only waits on earlier batches and the wait
// cannot cycle. A failure blocks later already-received records of the group
// until none of them remain in flight.
type groupGate struct {
	mu         sync.Mutex
	cond       *sync.Cond
	inflight   int // received, not yet finished (handler + delete + backoff)
	seq        int // next sequence to assign
	nextToRun  int // lowest sequence whose batch has not finished
	blocked    bool
	retryAfter time.Duration
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
		sem: make(chan struct{}, o.MaxInFlight), inflight: map[*Record]struct{}{},
		gates: map[string]*groupGate{}}, nil
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
		// Count every message in this response before the first send. Otherwise a
		// fast worker can finish (and clear a group failure) while this loop is
		// still blocked on the accumulator and the tail is not in flight yet.
		recs := make([]*Record, len(res.Messages))
		for i, m := range res.Messages {
			recs[i] = toRecord(m, now)
			c.track(recs[i])
			c.trackGroup(recs[i])
		}
		for _, r := range recs {
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
			c.assignGroupSeq(r) // accumulator order is the order batches run in
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
	// failed is filled below. The defer reads it when process returns, after
	// delete and backoff, so the group gate stays held through both.
	var failed map[string]Failure
	defer func() {
		c.finishGroups(b, failed)
		c.release(len(b))
	}()

	run, blocked := c.waitEarlierGroups(b)
	var res Result
	if len(run) > 0 {
		res = c.call(ctx, run)
	}
	failed = c.failures(run, res)
	for _, f := range blocked {
		failed[f.ID] = f
	}

	var ack, drop []*Record
	var backoff []Failure
	byID := make(map[string]*Record, len(b))
	for _, r := range b {
		byID[r.ID] = r
		f, bad := failed[r.ID]
		switch {
		case !bad:
			ack = append(ack, r)
		case f.RetryAfter > 0:
			backoff = append(backoff, f)
		default:
			drop = append(drop, r)
		}
	}

	// RetryAfter is applied before those receipts leave the heartbeat set, under
	// hbMu, so a heartbeat cannot overwrite it. RetryAfter==0 failures are
	// untracked here too: extending them would delay the lapse the caller asked
	// for. Each side has its own AckTimeout. Ack receipts stay heartbeated until
	// DeleteMessageBatch returns.
	if len(drop) > 0 || len(backoff) > 0 {
		c.hbMu.Lock()
		if len(drop) > 0 {
			c.untrack(drop)
		}
		if len(backoff) > 0 {
			bctx, bcancel := context.WithTimeout(context.WithoutCancel(ctx), c.o.AckTimeout)
			c.backoffAll(bctx, b, backoff)
			bcancel()
			bo := make([]*Record, 0, len(backoff))
			for _, f := range backoff {
				bo = append(bo, byID[f.ID])
			}
			c.untrack(bo)
		}
		c.hbMu.Unlock()
	}
	if len(ack) > 0 {
		// A record received just after a tick can have less than AckTimeout of
		// visibility left when the handler returns. Refresh it before delete so
		// the receipt cannot become visible (and get received again) mid-ack.
		c.extendAcks(ctx, ack)
		dctx, dcancel := context.WithTimeout(context.WithoutCancel(ctx), c.o.AckTimeout)
		undeleted := c.deleteAll(dctx, ack)
		dcancel()
		// A failed delete is a group failure: finishGroups (via the defer) must
		// see it before the next batch of the group is released. Leave those
		// receipts heartbeated so the head cannot be redelivered behind the tail.
		missed := make(map[string]struct{}, len(undeleted))
		for _, r := range undeleted {
			missed[r.ID] = struct{}{}
			failed[r.ID] = Failure{ID: r.ID, Err: errNotDeleted}
		}
		var gone []*Record
		for _, r := range ack {
			if _, ok := missed[r.ID]; !ok {
				gone = append(gone, r)
			}
		}
		c.untrack(gone)
	}
	c.o.Logger.Debug("gosqs: batch done", "size", len(b), "acked", len(ack),
		"failed", len(failed), "took", time.Since(start))
}

// waitEarlierGroups holds records of a FIFO group until every earlier batch
// that contains the group has finished. Records of a group an earlier batch
// failed are returned separately: they are not passed to the handler. Other
// groups in the batch are left runnable.
func (c *BatchConsumer) waitEarlierGroups(b []*Record) (run []*Record, blocked []Failure) {
	if !c.fifo() {
		return b, nil
	}
	type decision struct {
		blocked    bool
		retryAfter time.Duration
	}
	dec := make(map[string]decision)
	for _, r := range b {
		if r.GroupID == "" {
			continue
		}
		if _, seen := dec[r.GroupID]; seen {
			continue
		}
		isBlocked, retryAfter := c.gate(r.GroupID).waitTurn(r.groupSeq)
		dec[r.GroupID] = decision{isBlocked, retryAfter}
	}
	for _, r := range b {
		d, ok := dec[r.GroupID]
		if ok && r.GroupID != "" && d.blocked {
			blocked = append(blocked, Failure{ID: r.ID, Err: ErrGroupBlocked, RetryAfter: d.retryAfter})
			continue
		}
		run = append(run, r)
	}
	return run, blocked
}

// finishGroups runs after handler, delete, and backoff. It records a group
// failure for later batches and lets the next batch of that group proceed.
// The blocked flag clears once nothing of the group is left in flight, so a
// later redelivery is processed normally.
func (c *BatchConsumer) finishGroups(b []*Record, failed map[string]Failure) {
	if !c.fifo() {
		return
	}
	seen := map[string]bool{}
	for _, r := range b {
		if r.GroupID == "" || seen[r.GroupID] {
			continue
		}
		seen[r.GroupID] = true
		var recs []*Record
		for _, x := range b {
			if x.GroupID == r.GroupID {
				recs = append(recs, x)
			}
		}
		minSeq := recs[0].groupSeq
		for _, x := range recs[1:] {
			if x.groupSeq < minSeq {
				minSeq = x.groupSeq
			}
		}
		g := c.gate(r.GroupID)
		g.mu.Lock()
		if !g.blocked {
			for _, x := range recs {
				if f, ok := failed[x.ID]; ok {
					g.blocked = true
					g.retryAfter = f.RetryAfter
					break
				}
			}
		}
		// Sequences in one batch are contiguous; the next batch starts after them.
		g.nextToRun = minSeq + len(recs)
		g.inflight -= len(recs)
		if g.inflight <= 0 {
			g.inflight = 0
			g.blocked = false
			g.retryAfter = 0
		}
		g.cond.Broadcast()
		g.mu.Unlock()
	}
}

func (c *BatchConsumer) fifo() bool { return c.o.FIFO != nil && *c.o.FIFO }

func (c *BatchConsumer) gate(id string) *groupGate {
	c.gatesMu.Lock()
	defer c.gatesMu.Unlock()
	g := c.gates[id]
	if g == nil {
		g = &groupGate{}
		g.cond = sync.NewCond(&g.mu)
		c.gates[id] = g
	}
	return g
}

// trackGroup counts the record in flight before it is queued to the
// accumulator, so the blocked flag survives until the tail is finished.
func (c *BatchConsumer) trackGroup(r *Record) {
	if !c.fifo() || r.GroupID == "" {
		return
	}
	g := c.gate(r.GroupID)
	g.mu.Lock()
	g.inflight++
	g.mu.Unlock()
}

func (c *BatchConsumer) assignGroupSeq(r *Record) {
	if !c.fifo() || r.GroupID == "" {
		return
	}
	g := c.gate(r.GroupID)
	g.mu.Lock()
	r.groupSeq = g.seq
	g.seq++
	g.mu.Unlock()
}

func (g *groupGate) waitTurn(seq int) (blocked bool, retryAfter time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for g.nextToRun < seq {
		g.cond.Wait()
	}
	return g.blocked, g.retryAfter
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
// rest of its group inside this batch. The per-group gate holds that order
// across batches; this is the backstop for records that shared a handler call.
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

// extendAcks sets ack receipts' visibility back to Visibility before delete.
// It holds hbMu so a heartbeat cannot interleave with the refresh. The lock
// is not held across the delete itself: acks stay in the heartbeat set until
// DeleteMessageBatch returns, and a heartbeat during that call is intentional.
func (c *BatchConsumer) extendAcks(ctx context.Context, recs []*Record) {
	c.hbMu.Lock()
	defer c.hbMu.Unlock()
	vis := int32(c.o.Visibility / time.Second)
	for chunk := range slices.Chunk(recs, 10) {
		entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, len(chunk))
		for i, r := range chunk {
			entries[i] = types.ChangeMessageVisibilityBatchRequestEntry{
				Id: aws.String(strconv.Itoa(i)), ReceiptHandle: aws.String(r.ReceiptHandle),
				VisibilityTimeout: vis,
			}
		}
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.o.AckTimeout)
		c.changeVisibility(cctx, entries, chunk, "ack")
		cancel()
	}
}

// deleteAll deletes recs and returns the receipts still on the queue.
// For one FIFO group, a later receipt is not sent until every earlier receipt
// in recs has been deleted. A single DeleteMessageBatch can still delete a
// later entry and fail an earlier one; holding the next call is what keeps
// that split from crossing batches.
func (c *BatchConsumer) deleteAll(ctx context.Context, recs []*Record) []*Record {
	pending := slices.Clone(recs)
	var undeleted []*Record
	for len(pending) > 0 {
		if ctx.Err() != nil {
			return append(undeleted, pending...)
		}
		n := min(len(pending), 10)
		wave := pending[:n]
		pending = pending[n:]
		failed := c.deleteChunk(ctx, wave)
		if len(failed) == 0 {
			continue
		}
		groupBad := map[string]struct{}{}
		for _, r := range failed {
			undeleted = append(undeleted, r)
			if c.fifo() && r.GroupID != "" {
				groupBad[r.GroupID] = struct{}{}
			}
		}
		if len(groupBad) == 0 {
			continue
		}
		var rest []*Record
		for _, r := range pending {
			if _, bad := groupBad[r.GroupID]; bad {
				undeleted = append(undeleted, r)
				continue
			}
			rest = append(rest, r)
		}
		pending = rest
	}
	return undeleted
}

// deleteChunk deletes one batch of at most 10. It returns the receipts that
// are still on the queue after one retry of the call and of retryable entries.
func (c *BatchConsumer) deleteChunk(ctx context.Context, chunk []*Record) []*Record {
	entries := make([]types.DeleteMessageBatchRequestEntry, len(chunk))
	left := make(map[string]bool, len(chunk))
	for i, r := range chunk {
		id := strconv.Itoa(i)
		entries[i] = types.DeleteMessageBatchRequestEntry{Id: aws.String(id), ReceiptHandle: aws.String(r.ReceiptHandle)}
		left[id] = true
	}
	// One retry for the whole call and for retryable per-entry failures.
	// Sender-fault entries are logged and dropped; retrying cannot help.
	for attempt := 0; attempt < 2 && len(entries) > 0; attempt++ {
		out, err := c.api.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{QueueUrl: aws.String(c.url), Entries: entries})
		if err != nil {
			c.o.Logger.Error("gosqs: delete batch failed", "err", err, "n", len(entries))
			continue
		}
		failedID := make(map[string]bool, len(out.Failed))
		for _, f := range out.Failed {
			failedID[aws.ToString(f.Id)] = true
		}
		for _, e := range entries {
			if !failedID[aws.ToString(e.Id)] {
				delete(left, aws.ToString(e.Id))
			}
		}
		entries = filterEntries(entries, c.retryableIDs(out.Failed, chunk))
	}
	if len(entries) > 0 {
		c.o.Logger.Error("gosqs: records not deleted, will be redelivered", "n", len(entries))
	}
	undeleted := make([]*Record, 0, len(left))
	for id := range left {
		i, _ := strconv.Atoi(id)
		undeleted = append(undeleted, chunk[i])
	}
	return undeleted
}

// retryableIDs logs sender-fault entries and returns the ids worth one retry.
// recs is the batch the entry ids were assigned from (index i has id "i");
// the entry id is logged only as a secondary field.
func (c *BatchConsumer) retryableIDs(failed []types.BatchResultErrorEntry, recs []*Record) map[string]bool {
	again := make(map[string]bool, len(failed))
	for _, f := range failed {
		id := aws.ToString(f.Id)
		if f.SenderFault { // e.g. ReceiptHandleIsInvalid: retrying cannot help
			msgID, receipt := "", ""
			if i, err := strconv.Atoi(id); err == nil && i >= 0 && i < len(recs) {
				msgID, receipt = recs[i].ID, recs[i].ReceiptHandle
			}
			c.o.Logger.Warn("gosqs: skipping sender-fault batch entry",
				"message_id", msgID, "receipt_handle", receipt,
				"id", id, "code", aws.ToString(f.Code), "message", aws.ToString(f.Message))
			continue
		}
		again[id] = true
	}
	return again
}

func filterEntries(sent []types.DeleteMessageBatchRequestEntry, retry map[string]bool) []types.DeleteMessageBatchRequestEntry {
	var again []types.DeleteMessageBatchRequestEntry
	for _, e := range sent {
		if retry[aws.ToString(e.Id)] {
			again = append(again, e)
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
		recs := make([]*Record, len(chunk))
		for i, f := range chunk {
			recs[i] = byID[f.ID]
			entries[i] = types.ChangeMessageVisibilityBatchRequestEntry{
				Id: aws.String(strconv.Itoa(i)), ReceiptHandle: aws.String(recs[i].ReceiptHandle),
				VisibilityTimeout: visibilitySeconds(f.RetryAfter),
			}
		}
		c.changeVisibility(ctx, entries, recs, "backoff")
	}
}

// changeVisibility sends one visibility batch and retries entries whose failure
// is not the caller's fault a single time. Sender-fault entries are logged.
// recs[i] is the record entries were built from under id "i".
func (c *BatchConsumer) changeVisibility(ctx context.Context, entries []types.ChangeMessageVisibilityBatchRequestEntry, recs []*Record, op string) {
	for attempt := 0; attempt < 2 && len(entries) > 0; attempt++ {
		if ctx.Err() != nil {
			return
		}
		out, err := c.api.ChangeMessageVisibilityBatch(ctx, &sqs.ChangeMessageVisibilityBatchInput{QueueUrl: aws.String(c.url), Entries: entries})
		if err != nil {
			// DeadlineExceeded is this call's AckTimeout and is worth a log.
			// Canceled means shutdown; the next loop sees ctx.Err() and returns.
			if !errors.Is(ctx.Err(), context.Canceled) {
				c.o.Logger.Warn("gosqs: "+op+" failed", "err", err, "n", len(entries))
			}
			continue
		}
		var failed []types.BatchResultErrorEntry
		if out != nil {
			failed = out.Failed
		}
		entries = filterVisibility(entries, c.retryableIDs(failed, recs))
	}
	if len(entries) > 0 && ctx.Err() == nil {
		c.o.Logger.Warn("gosqs: "+op+" entries left unchanged", "n", len(entries))
	}
}

func filterVisibility(sent []types.ChangeMessageVisibilityBatchRequestEntry, retry map[string]bool) []types.ChangeMessageVisibilityBatchRequestEntry {
	var again []types.ChangeMessageVisibilityBatchRequestEntry
	for _, e := range sent {
		if retry[aws.ToString(e.Id)] {
			again = append(again, e)
		}
	}
	return again
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
		c.changeVisibility(cctx, entries, chunk, "heartbeat")
		cancel()
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
