package gosqs

import (
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

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
