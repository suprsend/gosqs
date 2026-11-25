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
	receives    int
	recvErr     error // returned by every ReceiveMessage while set
	inflight    int
	maxInflight int
	// failDelete fails a receipt's first delete attempt as a per-entry
	// Failed[] result; the value is its SenderFault.
	failDelete   map[string]bool
	doubleDelete []string // receipts deleted more than once
}

func newFake(n int, group func(i int) string) *fakeSQS {
	f := &fakeSQS{visibility: map[string]int32{}}
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

func (f *fakeSQS) DeleteMessageBatch(_ context.Context, in *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(in.Entries) > 10 {
		return nil, errors.New("TooManyEntriesInBatchRequest")
	}
	f.deleteCalls = append(f.deleteCalls, len(in.Entries))
	out := &sqs.DeleteMessageBatchOutput{}
	for _, e := range in.Entries {
		h := aws.ToString(e.ReceiptHandle)
		if sender, ok := f.failDelete[h]; ok {
			delete(f.failDelete, h)
			out.Failed = append(out.Failed, types.BatchResultErrorEntry{Id: e.Id, SenderFault: sender, Code: aws.String("X")})
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

func (f *fakeSQS) ChangeMessageVisibilityBatch(_ context.Context, in *sqs.ChangeMessageVisibilityBatchInput, _ ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.visCalls++
	for _, e := range in.Entries {
		f.visibility[aws.ToString(e.ReceiptHandle)] = e.VisibilityTimeout
	}
	return &sqs.ChangeMessageVisibilityBatchOutput{}, nil
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
