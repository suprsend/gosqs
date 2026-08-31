package gosqs

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type sample struct {
	Val string `json:"val"`
}

func (s *sample) ModelName() string {
	return "sample"
}

func TestNewPublisher(t *testing.T) {
	t.Run("with_arn", func(t *testing.T) {
		conf := Config{
			Region:   "us-west-1",
			Key:      "key",
			Secret:   "secret",
			Hostname: "http://localhost:4100",
			TopicARN: "arn:aws:sns:local:000000000000:todolist-dev",
		}
		_, err := NewPublisher(conf)
		if err != nil {
			t.Fatalf("error creating publisher, got %v", err)
		}
	})

	t.Run("without_arn", func(t *testing.T) {
		conf := Config{
			Region:       "local",
			Key:          "key",
			Secret:       "secret",
			Env:          "dev",
			Hostname:     "http://localhost:4100",
			AWSAccountID: "000000000000",
			TopicPrefix:  "todolist",
		}
		pub, err := NewPublisher(conf)
		if err != nil {
			t.Fatalf("error creating publisher, got %v", err)
		}
		arn := pub.(*publisher).arn
		if arn != "arn:aws:sns:local:000000000000:todolist-dev" {
			t.Errorf("did not properly create the arn name, expected %s, got %s", "arn:aws:sns:local:000000000000:todolist-dev", arn)
		}
	})
}

func retrievePubMessage(t *testing.T, p *publisher, queue string) Message {
	name := fmt.Sprintf("%s-%s", p.env, queue)
	q, err := p.sqs.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		t.Fatalf("unable to get queue url, got: %v", err)
	}

	output, err := p.sqs.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
		QueueUrl:              q.QueueUrl,
		MessageAttributeNames: []string{"All"},
	})
	if err != nil {
		t.Fatalf("unable to retrieve message, got: %v", err)
	}

	if len(output.Messages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(output.Messages))
	}
	_, err = p.sqs.DeleteMessage(context.Background(), &sqs.DeleteMessageInput{
		QueueUrl:      q.QueueUrl,
		ReceiptHandle: output.Messages[0].ReceiptHandle,
	})
	if err != nil {
		t.Errorf("could not delete published message, got %v", err)
	}

	return newMessage(output.Messages[0], messageRoute(output.Messages[0], ""))
}

func getPublisher(t *testing.T) *publisher {
	conf := Config{
		Region:   "us-west-1",
		Key:      "key",
		Env:      "dev",
		Secret:   "secret",
		Hostname: "http://localhost:4100",
		TopicARN: "arn:aws:sns:local:000000000000:todolist-dev",
	}

	cfg, err := newAWSConfig(conf)
	if err != nil {
		t.Fatalf("could not create session, got %v", err)
	}

	return &publisher{
		sqs: sqs.NewFromConfig(cfg),
		sns: sns.NewFromConfig(cfg),
		arn: conf.TopicARN,
		env: conf.Env,
	}
}

func TestCreate(t *testing.T) {
	p := getPublisher(t)
	p.Create(&sample{})
	msg := retrievePubMessage(t, p, "post-worker")
	expected := "sample_created"
	if msg.Route() != expected {
		t.Fatalf("did not create correct route, expected %s, got %s", expected, msg.Route())
	}
}

func TestDelete(t *testing.T) {
	p := getPublisher(t)
	p.Delete(&sample{})
	msg := retrievePubMessage(t, p, "post-worker")
	expected := "sample_deleted"
	if msg.Route() != expected {
		t.Fatalf("did not create correct route, expected %s, got %s", expected, msg.Route())
	}
}

func TestUpdate(t *testing.T) {
	p := getPublisher(t)
	p.Update(&sample{})
	msg := retrievePubMessage(t, p, "post-worker")
	expected := "sample_updated"
	if msg.Route() != expected {
		t.Fatalf("did not create correct route, expected %s, got %s", expected, msg.Route())
	}
}

func TestModify(t *testing.T) {
	p := getPublisher(t)
	changes := map[string]string{
		"oldName": "newName",
	}
	p.Modify(&sample{Val: "val"}, &changes)
	msg := retrievePubMessage(t, p, "post-worker")
	expected := "sample_modified"
	if msg.Route() != expected {
		t.Fatalf("did not create correct route, expected %s, got %s", expected, msg.Route())
	}

	dch := map[string]string{}
	var res sample

	if err := msg.DecodeModified(&res, &dch); err != nil {
		t.Errorf("could not decode modified content, got %v", err)
	}

	if res.Val != "val" {
		t.Errorf("did not properly return struct value, expected val got %s", res.Val)
	}

	if v, ok := dch["oldName"]; !ok {
		t.Errorf("changes did not retain values, expected newName, got %s", v)
	}

}

func TestDispatch(t *testing.T) {
	p := getPublisher(t)
	p.Dispatch(&sample{}, "some_event")
	msg := retrievePubMessage(t, p, "post-worker")
	expected := "sample_some_event"
	if msg.Route() != expected {
		t.Fatalf("did not create correct route, expected %s, got %s", expected, msg.Route())
	}
}

func TestDirectMessage(t *testing.T) {
	p := getPublisher(t)
	p.Message("post-worker", "some_event", &sample{})
	msg := retrievePubMessage(t, p, "post-worker")
	expected := "some_event"
	if msg.Route() != expected {
		t.Fatalf("did not create correct route, expected %s, got %s", expected, msg.Route())
	}
}

func TestDefaultSNSAttributs(t *testing.T) {
	event := "some_event"
	att := defaultSNSAttributes(event)
	expected := map[string]snstypes.MessageAttributeValue{
		"route": {DataType: aws.String("String"), StringValue: aws.String(event)},
	}

	if !reflect.DeepEqual(expected, att) {
		t.Fatalf("unexpected results,\nexpected %+v,\ngot: %+v", expected, att)
	}
}

func TestDefaultSQSAttributs(t *testing.T) {
	event := "some_event"
	att := defaultSQSAttributes(event)
	expected := map[string]sqstypes.MessageAttributeValue{
		"route": {DataType: aws.String("String"), StringValue: aws.String(event)},
	}

	if !reflect.DeepEqual(expected, att) {
		t.Fatalf("unexpected results,\nexpected %+v,\ngot: %+v", expected, att)
	}
}
