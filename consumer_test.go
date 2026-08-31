package gosqs

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type testStruct struct {
	Val string `json:"val"`
}

func test(ctx context.Context, m Message) error {
	return nil
}

func extend(ctx context.Context, m Message) error {
	time.Sleep(2 * time.Second)
	return nil
}

func err(ctx context.Context, m Message) error {
	return ErrGetMessage
}

func retrieveMessage(t *testing.T, c *consumer) Message {
	output, err := c.sqs.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(c.QueueURL),
		MessageAttributeNames: []string{"All"},
	})
	if err != nil {
		t.Fatalf("unable to retrieve message, got: %v", err)
	}

	if len(output.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(output.Messages))
	}

	return newMessage(output.Messages[0], messageRoute(output.Messages[0], c.messageHandlerName))
}

func messageRoute(m types.Message, fallback string) string {
	if fallback != "" {
		return fallback
	}
	if attr, ok := m.MessageAttributes["route"]; ok {
		return aws.ToString(attr.StringValue)
	}
	return ""
}

func getConsumer(t *testing.T) *consumer {
	conf := Config{
		Region:   "local",
		Key:      "key",
		Secret:   "secret",
		Env:      "dev",
		Hostname: "http://localhost:4100",
	}
	c, err := NewConsumer(conf, "post-worker")
	if err != nil {
		t.Fatalf("could not create consumer, got %v", err)
	}

	cons := c.(*consumer)
	cons.VisibilityTimeout = 30
	cons.extensionLimit = 2
	cons.workerPool = 15

	cons.sqs.PurgeQueue(context.Background(), &sqs.PurgeQueueInput{QueueUrl: aws.String(cons.QueueURL)})
	return cons
}

func TestNewConsumer(t *testing.T) {
	conf := Config{
		Region:   "us-west2",
		Key:      "key",
		Secret:   "secret",
		Hostname: "http://localhost:4100",
		Env:      "dev",
	}
	c, err := NewConsumer(conf, "post-worker")
	if err != nil {
		t.Fatalf("error creating consumer, got %v", err)
	}
	if !strings.Contains(c.(*consumer).QueueURL, "dev-post-worker") {
		t.Fatalf("did not properly apply http result, expected queue name in URL, got %s", c.(*consumer).QueueURL)
	}
}

func TestNewConsumerWithSessionProvider(t *testing.T) {
	provider := func(c Config) (aws.Config, error) {
		creds := credentials.NewStaticCredentialsProvider("mykey", "mysecret", "")
		if _, err := creds.Retrieve(context.Background()); err != nil {
			return aws.Config{}, ErrInvalidCreds.Context(err)
		}

		return aws.Config{
			Region:       "us-west2",
			Credentials:  creds,
			BaseEndpoint: aws.String("http://localhost:4100"),
		}, nil
	}

	conf := Config{
		SessionProvider: provider,
		Env:             "dev",
	}

	c, err := NewConsumer(conf, "post-worker")
	if err != nil {
		t.Fatalf("error creating consumer, got %v", err)
	}
	if !strings.Contains(c.(*consumer).QueueURL, "dev-post-worker") {
		t.Fatalf("did not properly apply http result, expected queue name in URL, got %s", c.(*consumer).QueueURL)
	}
}

func TestRegisterHandler(t *testing.T) {
	c := getConsumer(t)
	a := []Adapter{}
	c.RegisterHandler("post_published", test, a...)

	handlers := c.handlers
	if len(handlers) != 1 {
		t.Fatalf("did not apply the handler, expected 1 got %d", len(handlers))
	}

	if _, ok := handlers["post_published"]; !ok {
		t.Fatalf("did not apply the correct handler, expected post_published, got %+v", handlers)
	}
}

func TestMessageSelf(t *testing.T) {
	c := getConsumer(t)

	c.MessageSelf(context.TODO(), "test_event", testStruct{"val"})
	msg := retrieveMessage(t, c)
	if msg.Route() != "test_event" {
		t.Errorf("unexpected route, expected test_event, got %s", msg.Route())
	}

	var ts testStruct
	msg.Decode(&ts)
	if ts.Val != "val" {
		t.Errorf("did not properly apply value body, got %s", ts.Val)
	}
}

func TestMessage(t *testing.T) {
	c := getConsumer(t)

	c.Message(context.TODO(), "post-worker", "test_event", testStruct{"val"})
	msg := retrieveMessage(t, c)
	if msg.Route() != "test_event" {
		t.Errorf("unexpected route, expected test_event, got %s", msg.Route())
	}

	var ts testStruct
	msg.Decode(&ts)
	if ts.Val != "val" {
		t.Errorf("did not properly apply value body, got %s", ts.Val)
	}
}

func TestDeleteMessage(t *testing.T) {
	c := getConsumer(t)

	c.Message(context.TODO(), "post-worker", "test_event", testStruct{"val"})
	msg := retrieveMessage(t, c)
	if msg.Route() != "test_event" {
		t.Errorf("unexpected route, expected test_event, got %s", msg.Route())
	}

	if err := c.delete(msg.(*message)); err != nil {
		t.Fatalf("unable to delete got %v", err)
	}
}

func TestRun(t *testing.T) {
	c := getConsumer(t)
	a := []Adapter{WithRecovery(func() {})}
	c.RegisterHandler("post_published", test, a...)
	c.RegisterHandler("post_event", err, a...)
	c.RegisterHandler("extend", extend, a...)

	if len(c.handlers) != 3 {
		t.Fatalf("did not apply the handler, expected 3 got %d", len(c.handlers))
	}

	t.Run("no_error", func(t *testing.T) {
		c.Message(context.TODO(), "post-worker", "post_published", testStruct{"val"})
		m := retrieveMessage(t, c)
		if err := c.run(m.(*message)); err != nil {
			t.Errorf("should not return an error, got %v", err)
		}
	})

	t.Run("error", func(t *testing.T) {
		c.Message(context.TODO(), "post-worker", "post_event", testStruct{"val"})
		m := retrieveMessage(t, c)
		if err := c.run(m.(*message)); err != ErrGetMessage {
			t.Errorf("unexpected result, expected %v, got %v", ErrGetMessage, err)
		}
	})

	t.Run("no_event", func(t *testing.T) {
		c.Message(context.TODO(), "post-worker", "no_event", testStruct{"val"})
		m := retrieveMessage(t, c)
		if err := c.run(m.(*message)); err != nil {
			t.Errorf("unexpected result, expected %v, got %v", nil, err)
		}
	})

	t.Run("renew_visibility", func(t *testing.T) {
		c.VisibilityTimeout = 11
		c.Message(context.TODO(), "post-worker", "extend", testStruct{"val"})
		m := retrieveMessage(t, c)
		if err := c.run(m.(*message)); err != nil {
			t.Errorf("unexpected result, expected %v, got %v", nil, err)
		}
	})

}
