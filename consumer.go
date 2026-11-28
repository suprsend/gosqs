package gosqs

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

var maxMessages = int32(10)

// defaultWaitTimeSeconds is the ReceiveMessage long-poll wait used when
// Config.WaitTimeSeconds is unset. 20s is the SQS maximum.
const defaultWaitTimeSeconds = int32(20)

// Consumer provides an interface for receiving messages through AWS SQS and SNS
type Consumer interface {
	// Consume polls for new messages and if it finds one, decodes it, sends it to the handler and deletes it
	//
	// A message is not considered dequeued until it has been sucessfully processed and deleted. There is a 30 Second
	// delay between receiving a single message and receiving the same message. This delay can be adjusted in the AWS
	// console and can also be extended during operation. If a message is successfully received 4 times but not deleted,
	// it will be considered unprocessable and sent to the DLQ automatically
	//
	// Consume uses long-polling to check and retrieve messages, if it is unable to make a connection, the aws-SDK will use its
	// advanced retrying mechanism (including exponential backoff), if all of the retries fail, then we will wait 10s before
	// trying again.
	//
	// When a new message is received, it runs in a separate go-routine that will handle the full consuming of the message, error reporting
	// and deleting
	Consume(ctx context.Context)
	// RegisterHandler registers an event listener and an associated handler. If the event matches, the handler will
	// be run
	RegisterHandler(name string, h Handler, adapters ...Adapter)
	// Message serves as the direct messaging capability within the consumer. A worker can send direct messages to other workers
	Message(ctx context.Context, queue, event string, body interface{})
	// MessageSelf serves as the self messaging capability within the consumer, a worker can send messages to itself for continued
	// processing and resiliency
	MessageSelf(ctx context.Context, event string, body interface{})
}

// consumer is a wrapper around sqs.Client
type consumer struct {
	sqs               *sqs.Client
	handlers          map[string]Handler
	env               string
	QueueURL          string
	Hostname          string
	VisibilityTimeout int
	workerPool        int
	extensionLimit    int
	attributes        []customAttribute

	logger Logger
	//
	messageHandlerName  string
	maxNumberOfMessages int32
	waitTimeSeconds     int32
}

// NewConsumer creates a new SQS instance and provides a configured consumer interface for
// receiving and sending messages
func NewConsumer(c Config, queueName string) (Consumer, error) {
	cfg, err := resolveAWSConfig(c)
	if err != nil {
		return nil, err
	}

	cons := &consumer{
		sqs:               sqs.NewFromConfig(cfg),
		env:               c.Env,
		VisibilityTimeout: 30,
		workerPool:        30,
		extensionLimit:    2,
	}

	if c.Logger != nil {
		cons.logger = c.Logger
	}

	if c.VisibilityTimeout != 0 {
		cons.VisibilityTimeout = c.VisibilityTimeout
	}

	if c.WorkerPool != 0 {
		cons.workerPool = c.WorkerPool
	}

	if c.ExtensionLimit != nil {
		cons.extensionLimit = *c.ExtensionLimit
	}

	cons.QueueURL = c.QueueURL
	// custom QueueURLs can be provided for testing and mocking purposes
	if cons.QueueURL == "" {
		name := fmt.Sprintf("%s-%s", c.Env, queueName)
		o, err := cons.sqs.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
		if err != nil {
			return nil, err
		}
		cons.QueueURL = aws.ToString(o.QueueUrl)
	}
	//
	cons.messageHandlerName = c.MessageHandlerName
	if c.MaxNumberOfMessages <= 0 || int32(c.MaxNumberOfMessages) > maxMessages {
		cons.maxNumberOfMessages = maxMessages
	} else {
		cons.maxNumberOfMessages = int32(c.MaxNumberOfMessages)
	}
	cons.waitTimeSeconds = int32(c.WaitTimeSeconds)
	if cons.waitTimeSeconds == 0 {
		cons.waitTimeSeconds = defaultWaitTimeSeconds // long polling; 0 would busy-poll an empty queue
	}
	return cons, nil
}

// Logger accesses the logging field or applies a default logger
func (c *consumer) Logger() Logger {
	if c.logger == nil {
		return &defaultLogger{}
	}
	return c.logger
}

// RegisterHandler registers an event listener and an associated handler. If the event matches, the handler will
// be run along with any included middleware
func (c *consumer) RegisterHandler(name string, h Handler, adapters ...Adapter) {
	if c.handlers == nil {
		c.handlers = make(map[string]Handler)
	}

	for i := len(adapters) - 1; i >= 0; i-- {
		h = adapters[i](h)
	}

	c.handlers[name] = func(ctx context.Context, m Message) error {
		return h(ctx, m)
	}
}

// Consume polls for new messages and if it finds one, decodes it, sends it to the handler and deletes it
//
// A message is not considered dequeued until it has been sucessfully processed and deleted. There is a 30 Second
// delay between receiving a single message and receiving the same message. This delay can be adjusted in the AWS
// console and can also be extended during operation. If a message is successfully received 4 times but not deleted,
// it will be considered unprocessable and sent to the DLQ automatically
//
// Consume uses long-polling to check and retrieve messages, if it is unable to make a connection, the aws-SDK will use its
// advanced retrying mechanism (including exponential backoff), if all of the retries fail, then we will wait 10s before
// trying again.
//
// When a new message is received, it runs in a separate go-routine that will handle the full consuming of the message, error reporting
// and deleting
func (c *consumer) Consume(ctx context.Context) {
	jobs := make(chan *message)
	for w := 1; w <= c.workerPool; w++ {
		go c.worker(w, jobs)
	}

	for {
		output, err := c.sqs.ReceiveMessage(
			ctx,
			&sqs.ReceiveMessageInput{
				MaxNumberOfMessages:   c.maxNumberOfMessages,
				QueueUrl:              aws.String(c.QueueURL),
				VisibilityTimeout:     int32(c.VisibilityTimeout),
				WaitTimeSeconds:       c.waitTimeSeconds,
				MessageAttributeNames: []string{"All"},
				MessageSystemAttributeNames: []types.MessageSystemAttributeName{
					types.MessageSystemAttributeNameSentTimestamp,
				},
			},
		)
		if err != nil {
			if ctx.Err() != nil {
				c.Logger().Println("EXITING !! gosqs.consumer.Consume(). Context cancelled.")
				close(jobs)
				return
			}
			c.Logger().Println(ErrGetMessage.Context(err).Error(), "retrying in 10s")
			if !sleep(ctx, 10*time.Second) {
				c.Logger().Println("EXITING !! gosqs.consumer.Consume(). Context cancelled.")
				close(jobs)
				return
			}
			continue
		}

		for _, m := range output.Messages {
			// ----- Messages don't need to have route attribute
			// if _, ok := m.MessageAttributes["route"]; !ok {
			//     //a message will be sent to the DLQ automatically after 4 tries if it is received but not deleted
			//     c.Logger().Println(ErrNoRoute.Error())
			//     continue
			// }

			jobs <- newMessage(m, messageRoute(m, c.messageHandlerName))
		}
	}
}

// worker is an always-on concurrent worker that will take tasks when they are added into the messages buffer
func (c *consumer) worker(id int, messages <-chan *message) {
	for m := range messages {
		if err := c.run(m); err != nil {
			c.Logger().Println(err.Error())
		}
	}
}

// run should be run within a worker
//
// if there is no handler for that route, the message is not deleted: it reappears after its visibility timeout and
// reaches the DLQ through the queue's maxReceiveCount
//
// while the handler runs, a heartbeat keeps the message invisible. The message is deleted only if the handler returns nil
func (c *consumer) run(m *message) error {
	h, ok := c.handlers[m.Route()]
	if !ok {
		return ErrNoRoute.Context(fmt.Errorf("no handler registered for route %q, message %s not deleted", m.Route(), m.MessageId()))
	}
	ctx := context.Background()

	done := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		c.heartbeat(ctx, m, done)
	}()
	// Close done on every return, including a panic that escapes the handler,
	// and wait until the heartbeat goroutine has left so a late extend cannot
	// race the caller (or a test) observing the message.
	var once sync.Once
	stopHeartbeat := func() {
		once.Do(func() {
			close(done)
			<-exited
		})
	}
	defer stopHeartbeat()

	err := h(ctx, m)
	if err != nil {
		return err
	}
	stopHeartbeat()
	return c.delete(m) //MESSAGE CONSUMED
}

// MessageSelf serves as the self messaging capability within the consumer, a worker can send messages to itself for continued
// processing and resiliency
func (c *consumer) MessageSelf(ctx context.Context, event string, body interface{}) {
	o, err := json.Marshal(body)
	if err != nil {
		log.Println(ErrMarshal.Context(err).Error(), event)
		return
	}

	out := string(o)

	sqsInput := &sqs.SendMessageInput{
		MessageBody:       aws.String(out),
		MessageAttributes: defaultSQSAttributes(event, c.attributes...),
		QueueUrl:          aws.String(c.QueueURL),
	}

	go c.sendDirectMessage(sqsInput, event)
}

// Message serves as the direct messaging capability within the consumer. A worker can send direct messages to other workers
func (c *consumer) Message(ctx context.Context, queue, event string, body interface{}) {
	name := fmt.Sprintf("%s-%s", c.env, queue)

	queueResp, err := c.sqs.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		log.Printf("%s, queue: %s", ErrQueueURL.Context(err).Error(), name)
		return
	}

	o, err := json.Marshal(body)
	if err != nil {
		log.Println(ErrMarshal.Context(err).Error(), event)
		return
	}

	out := string(o)

	sqsInput := &sqs.SendMessageInput{
		MessageBody:       aws.String(out),
		MessageAttributes: defaultSQSAttributes(event, c.attributes...),
		QueueUrl:          queueResp.QueueUrl,
	}

	go c.sendDirectMessage(sqsInput, event)
}

// sendDirectMessage is a helper that should be run concurrently since it will block the main thread if there is a connection issue
func (c *consumer) sendDirectMessage(input *sqs.SendMessageInput, event string) {
	if _, err := c.sqs.SendMessage(context.Background(), input); err != nil {
		log.Printf("%s, event: %s \nretrying in 10s", ErrPublish.Context(err).Error(), event)
		time.Sleep(10 * time.Second)
		c.sendDirectMessage(input, event)
	}
}

// delete will remove a message from the queue, this is necessary to fully and successfully consume a message
func (c *consumer) delete(m *message) error {
	_, err := c.sqs.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{QueueUrl: aws.String(c.QueueURL), ReceiptHandle: m.ReceiptHandle})
	if err != nil {
		c.Logger().Println(ErrUnableToDelete.Context(err).Error())
		return ErrUnableToDelete.Context(err)
	}
	return nil
}

// heartbeatSkew absorbs timer delay on a beat the nominal schedule accepts.
// The last off-grid beat is aimed this early so a late wake still satisfies
// time.Since(start)+vt <= budget.
const heartbeatSkew = 500 * time.Millisecond

// heartbeat keeps m invisible while its handler runs. Every VisibilityTimeout/3
// it sets visibility back to VisibilityTimeout. A beat is refused when time
// since this heartbeat started plus one visibility would pass holdBudget (the
// previous extender's maximum). When that interval would stop short of the
// budget, one last beat is taken at budget-VisibilityTimeout. An extensionLimit
// of 0 disables it. Each ChangeMessageVisibility call is bounded by
// heartbeatCallTimeout and is skipped when the handler has already finished.
// A handler should return at least one visibility before the budget so the
// delete still fits inside the hold.
func (c *consumer) heartbeat(ctx context.Context, m *message, done <-chan struct{}) {
	if c.extensionLimit <= 0 || c.VisibilityTimeout <= 0 {
		return
	}
	vt := time.Duration(c.VisibilityTimeout) * time.Second
	budget := holdBudget(vt, c.extensionLimit)
	timeout := heartbeatCallTimeout(vt)
	interval := vt / 3
	if interval <= 0 {
		return
	}
	start := time.Now()
	timer := time.NewTimer(interval)
	defer timer.Stop()
	nextGrid := interval
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		if time.Since(start)+vt > budget+heartbeatSkew {
			c.Logger().Println(ErrMessageProcessing.Error(), m.Route())
			return
		}
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		default:
		}
		callCtx, cancel := context.WithTimeout(ctx, timeout)
		_, err := c.sqs.ChangeMessageVisibility(callCtx, &sqs.ChangeMessageVisibilityInput{
			QueueUrl:          aws.String(c.QueueURL),
			ReceiptHandle:     m.ReceiptHandle,
			VisibilityTimeout: int32(c.VisibilityTimeout),
		})
		cancel()
		if err != nil {
			c.Logger().Println(ErrUnableToExtend.Error(), err.Error())
		}
		nextGrid += interval
		wait, ok := nextHeartbeatDelay(time.Since(start), nextGrid, vt, budget, heartbeatSkew)
		if !ok {
			c.waitOutBudget(m, start, budget, done, ctx)
			return
		}
		timer.Reset(wait)
	}
}

// nextHeartbeatDelay is the wait after a beat at elapsed (time since start).
// gridNext is the following VisibilityTimeout/3 point. When that point would
// hold the receipt past budget, the wait lands on the last instant that still
// fits, skew early so a late timer is still inside the budget.
func nextHeartbeatDelay(elapsed, gridNext, vt, budget, skew time.Duration) (wait time.Duration, ok bool) {
	if !beatHoldsPastBudget(gridNext, vt, budget) {
		wait = gridNext - elapsed
		if wait < 0 {
			wait = 0
		}
		return wait, true
	}
	last := budget - vt - skew
	if last <= elapsed {
		return 0, false
	}
	return last - elapsed, true
}

// waitOutBudget blocks until the hold budget or until the handler finishes.
// The "still processing" log is only for a handler that outlives the budget.
func (c *consumer) waitOutBudget(m *message, start time.Time, budget time.Duration, done <-chan struct{}, ctx context.Context) {
	remain := budget - time.Since(start)
	if remain > 0 {
		t := time.NewTimer(remain)
		defer t.Stop()
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
	select {
	case <-done:
		return
	case <-ctx.Done():
		return
	default:
	}
	c.Logger().Println(ErrMessageProcessing.Error(), m.Route())
}

// holdBudget is how long the previous extender kept a receipt invisible:
// L*(VT-10)+(L+1)*VT. The old loop slept VT-10 before each extend; a
// non-positive sleep (VT <= 10) returned immediately, and the final visibility
// was (L+1)*VT, so the slack is clamped at zero.
func holdBudget(vt time.Duration, limit int) time.Duration {
	slack := vt - 10*time.Second
	if slack < 0 {
		slack = 0
	}
	return time.Duration(limit)*slack + time.Duration(limit+1)*vt
}

// beatHoldsPastBudget reports whether a beat at elapsed (time since the
// heartbeat started) would push the receipt's visibility deadline past budget.
func beatHoldsPastBudget(elapsed, vt, budget time.Duration) bool {
	return elapsed+vt > budget
}

// heartbeatCallTimeout bounds one extend RPC, including the client's retries,
// to min(5s, VT/3) so it stays inside the gap before the next beat.
func heartbeatCallTimeout(vt time.Duration) time.Duration {
	d := vt / 3
	if d > 5*time.Second {
		return 5 * time.Second
	}
	return d
}
