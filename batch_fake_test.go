package gosqs

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// fakeSQS is an in-memory queue. Received messages stay hidden until deleted
// or until the test makes them visible; that is enough to assert acks.
type fakeSQS struct {
	mu          sync.Mutex
	queue       []types.Message // not yet received
	deleted     []string        // receipt handles
	deleteCalls []int           // entries per DeleteMessageBatch
	visibility  map[string]int32
	visCalls    int
	visAttempts map[string]int // ChangeMessageVisibilityBatch entries per receipt
	receives    int
	lastWait    int32
	lastVis     int32
	recvErr     error // returned by every ReceiveMessage while set
	inflight    int
	maxInflight int
	// failDelete fails a receipt's first delete attempt as a per-entry
	// Failed[] result; the value is its SenderFault.
	failDelete map[string]bool
	// failDeleteAlways is a per-entry failure on every attempt (not a sender
	// fault), so the consumer retries once and then gives up.
	failDeleteAlways map[string]bool
	// failDeleteReceipt makes the whole DeleteMessageBatch call fail while it
	// contains this receipt.
	failDeleteReceipt string
	// hangDelete waits for the call context to end when the batch contains
	// this receipt, then returns ctx.Err() without deleting.
	hangDelete string
	// failVisibility fails a receipt's first visibility attempt; the value is
	// its SenderFault. Cleared so a retry can succeed.
	failVisibility map[string]bool
	// blockVis waits on the call context for this many visibility calls.
	blockVis int
	// onDelete runs without f.mu, before the delete is applied.
	onDelete     func(ctx context.Context)
	doubleDelete []string // receipts deleted more than once
}

func newFake(n int, group func(i int) string) *fakeSQS {
	f := &fakeSQS{visibility: map[string]int32{}, visAttempts: map[string]int{}}
	for i := range n {
		id := "m" + strconv.Itoa(i)
		attrs := map[string]string{"ApproximateReceiveCount": "1"}
		if group != nil {
			attrs["MessageGroupId"] = group(i)
		}
		f.queue = append(f.queue, types.Message{
			MessageId: aws.String(id), ReceiptHandle: aws.String("rh-" + id),
			Body: aws.String(`{"n":` + strconv.Itoa(i) + `}`), Attributes: attrs,
		})
	}
	return f
}

func (f *fakeSQS) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	f.receives++
	if f.recvErr != nil {
		err := f.recvErr
		f.mu.Unlock()
		return nil, err
	}
	f.lastWait = in.WaitTimeSeconds
	f.lastVis = in.VisibilityTimeout
	n := min(int(in.MaxNumberOfMessages), len(f.queue))
	out := f.queue[:n:n]
	f.queue = f.queue[n:]
	f.inflight += n
	f.maxInflight = max(f.maxInflight, f.inflight)
	f.mu.Unlock()
	if n == 0 { // simulate a short long-poll
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
	return &sqs.ReceiveMessageOutput{Messages: out}, nil
}

func (f *fakeSQS) DeleteMessageBatch(ctx context.Context, in *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	if len(in.Entries) > 10 {
		return nil, errors.New("TooManyEntriesInBatchRequest")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	hook := f.onDelete
	hang := f.hangDelete
	failReceipt := f.failDeleteReceipt
	f.mu.Unlock()
	contains := func(receipt string) bool {
		if receipt == "" {
			return false
		}
		for _, e := range in.Entries {
			if aws.ToString(e.ReceiptHandle) == receipt {
				return true
			}
		}
		return false
	}
	// Call-level failures happen before the hook so a test can distinguish
	// "the batch was rejected" from "the batch was applied".
	if contains(failReceipt) {
		f.noteDeleteCall(len(in.Entries))
		return nil, errors.New("delete failed")
	}
	if contains(hang) {
		<-ctx.Done()
		f.noteDeleteCall(len(in.Entries))
		return nil, ctx.Err()
	}
	if hook != nil {
		hook(ctx)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls = append(f.deleteCalls, len(in.Entries))
	out := &sqs.DeleteMessageBatchOutput{}
	for _, e := range in.Entries {
		h := aws.ToString(e.ReceiptHandle)
		if f.failDeleteAlways[h] {
			out.Failed = append(out.Failed, batchErr(e.Id, false))
			continue
		}
		if sender, ok := f.failDelete[h]; ok {
			delete(f.failDelete, h)
			out.Failed = append(out.Failed, batchErr(e.Id, sender))
			continue
		}
		if slices.Contains(f.deleted, h) {
			f.doubleDelete = append(f.doubleDelete, h)
		}
		f.deleted = append(f.deleted, h)
		f.inflight--
	}
	return out, nil
}

func batchErr(id *string, sender bool) types.BatchResultErrorEntry {
	code, msg := "InternalError", "transient"
	if sender {
		code, msg = "ReceiptHandleIsInvalid", "bad receipt"
	}
	return types.BatchResultErrorEntry{Id: id, SenderFault: sender, Code: aws.String(code), Message: aws.String(msg)}
}

func (f *fakeSQS) ChangeMessageVisibilityBatch(ctx context.Context, in *sqs.ChangeMessageVisibilityBatchInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	block := f.blockVis > 0
	if block {
		f.blockVis--
	}
	f.mu.Unlock()
	if block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.visCalls++
	if f.visAttempts == nil {
		f.visAttempts = map[string]int{}
	}
	out := &sqs.ChangeMessageVisibilityBatchOutput{}
	for _, e := range in.Entries {
		h := aws.ToString(e.ReceiptHandle)
		f.visAttempts[h]++
		if sender, ok := f.failVisibility[h]; ok {
			delete(f.failVisibility, h)
			out.Failed = append(out.Failed, batchErr(e.Id, sender))
			continue
		}
		f.visibility[h] = e.VisibilityTimeout
	}
	return out, nil
}

func (f *fakeSQS) noteDeleteCall(n int) {
	f.mu.Lock()
	f.deleteCalls = append(f.deleteCalls, n)
	f.mu.Unlock()
}

func (f *fakeSQS) visAttempt(receipt string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.visAttempts[receipt]
}

func (f *fakeSQS) snapshot() (deleted []string, calls []int, vis map[string]int32, visCalls, receives, maxInflight int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v := make(map[string]int32, len(f.visibility))
	for k, x := range f.visibility {
		v[k] = x
	}
	return append([]string(nil), f.deleted...), append([]int(nil), f.deleteCalls...), v, f.visCalls, f.receives, f.maxInflight
}
