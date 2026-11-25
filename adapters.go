package gosqs

import (
	"context"
	"fmt"
)

const (
	dispatcherKey = contextKey("dispatcher")
)

type contextKey string

// Handler provides a standardized handler method, this is the required function composition for event handlers
type Handler func(context.Context, Message) error

// Adapter implements adapters in the context
type Adapter func(Handler) Handler

// WithRecovery is an adapter that logs a Panic error and recovers the service from a failed state
//
// recovery is deferred around the handler and is expected to call recover(). If the handler panics, the wrapped
// handler returns an error wrapping ErrPanic, so the message is not deleted and is redelivered.
func WithRecovery(recovery func()) Adapter {
	return func(fn Handler) Handler {
		return func(ctx context.Context, m Message) (err error) {
			panicked := true
			defer func() {
				if panicked {
					err = fmt.Errorf("%w: message %s", ErrPanic, m.MessageId())
				}
			}()
			defer recovery()

			err = fn(ctx, m)
			panicked = false
			return err
		}
	}
}

// WithMiddleware add middleware to the consumer service
func WithMiddleware(f func(ctx context.Context, m Message) error) Adapter {
	return func(fn Handler) Handler {
		return func(ctx context.Context, m Message) error {
			f(ctx, m)

			return fn(ctx, m)
		}
	}
}

// WithDispatcher sets an adapter to support sending async messages
func WithDispatcher(ctx context.Context, pub Publisher) context.Context {
	return context.WithValue(ctx, dispatcherKey, pub)
}

// Dispatcher retrieves the sqs dispatcher from the context for sending messeges
func Dispatcher(ctx context.Context) (Publisher, error) {
	if p, ok := ctx.Value(dispatcherKey).(Publisher); ok {
		return p, nil
	}

	return nil, ErrUndefinedPublisher
}

// MustDispatcher retrieves the sqs dispatcher from the context for sending messeges or panics if
// the Dispatcher does not exist in the context
func MustDispatcher(ctx context.Context) Publisher {
	if p, ok := ctx.Value(dispatcherKey).(Publisher); ok {
		return p
	}

	panic(ErrUndefinedPublisher.Error())
}
