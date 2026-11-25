package gosqs

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

// PerMessage adapts a one-record handler (the shape gosqs.Handler has today) to
// a BatchHandler. Up to concurrency records run at once. Records of one FIFO
// group run one at a time in order, and the group stops at its first failure.
func PerMessage(h func(context.Context, *Record) error, concurrency int) BatchHandler {
	concurrency = max(concurrency, 1)
	return func(ctx context.Context, recs []*Record) Result {
		sem := make(chan struct{}, concurrency)
		var (
			mu  sync.Mutex
			res Result
			wg  sync.WaitGroup
		)
		for _, g := range groupsInOrder(recs) {
			wg.Go(func() {
				for i, r := range g {
					sem <- struct{}{}
					err := safe(ctx, h, r)
					<-sem
					if err != nil {
						mu.Lock()
						res.Failures = append(res.Failures, Failure{ID: r.ID, Err: err})
						for _, rest := range g[i+1:] {
							res.Failures = append(res.Failures, Failure{ID: rest.ID, Err: ErrGroupBlocked})
						}
						mu.Unlock()
						return
					}
				}
			})
		}
		wg.Wait()
		return res
	}
}

func safe(ctx context.Context, h func(context.Context, *Record) error, r *Record) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%w: %v", ErrPanic, p)
		}
	}()
	return h(ctx, r)
}

// groupsInOrder splits recs by FIFO group, keeping order. On standard queues
// (GroupID "") every record is its own group.
func groupsInOrder(recs []*Record) [][]*Record {
	var out [][]*Record
	idx := map[string]int{}
	for _, r := range recs {
		if r.GroupID == "" {
			out = append(out, []*Record{r})
			continue
		}
		i, ok := idx[r.GroupID]
		if !ok {
			i = len(out)
			idx[r.GroupID] = i
			out = append(out, nil)
		}
		out[i] = append(out[i], r)
	}
	return out
}

// Item pairs a record with its decoded payload.
type Item[T any] struct {
	*Record
	Value T
}

// Decoded decodes each body as JSON into T. A record that fails to decode is
// reported as a failure (it reaches the DLQ via maxReceiveCount) and later
// records of its FIFO group are held back without reaching h.
func Decoded[T any](h func(ctx context.Context, items []Item[T]) Result) BatchHandler {
	return func(ctx context.Context, recs []*Record) Result {
		items := make([]Item[T], 0, len(recs))
		var bad []Failure
		blocked := map[string]bool{}
		for _, r := range recs {
			if r.GroupID != "" && blocked[r.GroupID] {
				bad = append(bad, Failure{ID: r.ID, Err: ErrGroupBlocked})
				continue
			}
			var v T
			if err := json.Unmarshal(r.Data, &v); err != nil {
				bad = append(bad, Failure{ID: r.ID, Err: fmt.Errorf("%w: %w", ErrDecode, err)})
				blocked[r.GroupID] = r.GroupID != ""
				continue
			}
			items = append(items, Item[T]{Record: r, Value: v})
		}
		var res Result
		if len(items) > 0 {
			res = h(ctx, items)
		}
		res.Failures = append(res.Failures, bad...)
		return res
	}
}
