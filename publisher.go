package gosqs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
)

const maxRetryCount = 5

// Notifier used for broadcasting messages
type Notifier interface {
	ModelName() string
}

// Publisher provides an interface for sending messages through AWS SQS and SNS
type Publisher interface {
	// Create sends a message using a notifier, the modelname will be prepended to the static event, e.g post_created
	Create(n Notifier)
	// Delete sends a message using a notifier, the modelname will be prepended to the static event, e.g post_deleted
	Delete(n Notifier)
	// Update sends a message using a notifier, the modelname will be prepended to the static event, e.g post_updated
	Update(n Notifier)
	// Modify sends a message using a notifier, as a map of changes. The modelname will be prepended to the static event, e.g post_modified
	//
	// a special decoder will need to be used to process these events
	Modify(n Notifier, changes interface{})
	// Dispatch sends a message using a notifier, the modelname will be prepended to the provided event, e.g post_published
	Dispatch(n Notifier, event string)
	// Message sends a direct message to an individual queue, the queueName(receiver) must be provided. The event will be sent
	// as is, no prepending will take place. No other queues will receive this message.
	Message(queue, message string, body interface{})
}

type publisher struct {
	sqs *sqs.Client
	sns *sns.Client

	arn    string
	env    string
	sqsURL string

	camelCase  bool
	attributes []customAttribute
	logger     Logger
}

// NewPublisher creates a new SQS/SNS publisher instance
func NewPublisher(c Config) (Publisher, error) {
	cfg, err := resolveAWSConfig(c)
	if err != nil {
		return nil, err
	}

	arn := c.TopicARN
	if arn == "" {
		arn = fmt.Sprintf("arn:aws:sns:%s:%s:%s-%s", c.Region, c.AWSAccountID, c.TopicPrefix, c.Env)
	}

	sqsURL := fmt.Sprintf("%s/", c.Hostname)
	if c.Hostname == "" {
		sqsURL = fmt.Sprintf("https://sqs.%s.amazonaws.com/%s/", c.Region, c.AWSAccountID)
	}

	if c.Logger == nil {
		c.Logger = &defaultLogger{}
	}

	pub := &publisher{
		sqs:    sqs.NewFromConfig(cfg),
		sns:    sns.NewFromConfig(cfg),
		arn:    arn,
		env:    c.Env,
		sqsURL: sqsURL,
	}

	return pub, nil
}

func (p *publisher) event(n Notifier, action string) string {
	if p.camelCase {
		return fmt.Sprintf("%s%s", n.ModelName(), strings.Title(action))
	}

	return fmt.Sprintf("%s_%s", n.ModelName(), action)
}

// Create sends a message using a notifier, the modelname will be prepended to the static event, e.g post_created
func (p *publisher) Create(n Notifier) {
	e := p.event(n, "created")
	go p.send(n, e)
}

// Delete sends a message using a notifier, the modelname will be prepended to the static event, e.g post_deleted
func (p *publisher) Delete(n Notifier) {
	e := p.event(n, "deleted")
	go p.send(n, e)
}

// Update sends a message using a notifier, the modelname will be prepended to the static event, e.g post_updated
func (p *publisher) Update(n Notifier) {
	e := p.event(n, "updated")
	go p.send(n, e)
}

type modify struct {
	Notifier `json:"body"`
	Changes  interface{} `json:"changes"`
}

// newModify creates a new struct with both Notifier and changes
func newModify(n Notifier, changes interface{}) *modify {
	return &modify{
		Notifier: n,
		Changes:  changes,
	}
}

// Modify sends a message using a notifier, as a map of changes. The modelname will be prepended to the static event, e.g post_modified
//
// a special decoder will need to be used to process these events
func (p *publisher) Modify(n Notifier, changes interface{}) {
	e := p.event(n, "modified")
	go p.send(newModify(n, changes), e)
}

// Dispatch sends a message using a notifier, the modelname will be prepended to the provided event, e.g post_published
func (p *publisher) Dispatch(n Notifier, event string) {
	e := p.event(n, event)
	go p.send(n, e)
}

// Message sends a direct message to an individual queue, the queueName(receiver) must be provided. The event will be sent
// as is, no prepending will take place. No other queues will receive this message.
func (p *publisher) Message(queue, event string, body interface{}) {
	name := fmt.Sprintf("%s-%s", p.env, queue)

	o, err := json.Marshal(body)
	if err != nil {
		p.logger.Println(ErrMarshal.Context(err).Error())
		return
	}

	out := string(o)

	u := p.sqsURL + name

	sqsInput := &sqs.SendMessageInput{
		MessageBody:       aws.String(out),
		MessageAttributes: defaultSQSAttributes(event, p.attributes...),
		QueueUrl:          aws.String(u),
	}

	go p.sendDirectMessage(sqsInput, event)
}

func isMessageTooLarge(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	if apiErr.ErrorCode() != "InvalidParameterValue" {
		return false
	}
	msg := apiErr.ErrorMessage()
	return strings.Contains(msg, "262144") || strings.Contains(msg, "shorter than")
}

// sendDirectMessage is used to handle sending and error failures in a separate go-routine
//
// AWS-SDK will use their own retry mechanism for a failed request utilizing exponential backoff. If they fail
// then we will wait 10 seconds before trying again
func (p *publisher) sendDirectMessage(input *sqs.SendMessageInput, event string) {
	for attempt := 0; attempt <= maxRetryCount; attempt++ {
		if _, err := p.sqs.SendMessage(context.Background(), input); err != nil {
			if isMessageTooLarge(err) {
				panic(ErrBodyOverflow.Context(err))
			}

			log.Print(ErrPublish)
			time.Sleep(10 * time.Second)
			continue
		}
		return
	}
}

// send is used to handle sending and error failures in a separate go-routine for SNS messages
//
// AWS-SDK will use their own retry mechanism for a failed request utilizing exponential backoff. If they fail
// then we will wait 10 seconds before trying again
func (p *publisher) send(body interface{}, event string) {
	o, err := json.Marshal(body)
	if err != nil {
		panic(ErrMarshal.Context(err))
	}

	snsInput := &sns.PublishInput{
		Message:           aws.String(string(o)),
		MessageAttributes: defaultSNSAttributes(event, p.attributes...),
		TopicArn:          aws.String(p.arn),
	}

	for attempt := 0; attempt <= maxRetryCount; attempt++ {
		if _, err = p.sns.Publish(context.Background(), snsInput); err != nil {
			if isMessageTooLarge(err) {
				panic(ErrBodyOverflow.Context(err))
			}

			log.Println(ErrPublish.Context(err), " retrying in 10s")
			time.Sleep(10 * time.Second)
			continue
		}
		return
	}
}

// defaultSNSAttributes provides general SNS attributes that we need for every message
func defaultSNSAttributes(event string, ca ...customAttribute) map[string]snstypes.MessageAttributeValue {
	m := map[string]snstypes.MessageAttributeValue{
		"route": {DataType: aws.String("String"), StringValue: aws.String(event)},
	}

	for _, attr := range ca {
		m[attr.Title] = snstypes.MessageAttributeValue{DataType: aws.String(attr.DataType), StringValue: aws.String(attr.Value)}
	}

	return m
}

// defaultSQSAttributes provides general SQS attributes that we need for every message
func defaultSQSAttributes(event string, ca ...customAttribute) map[string]sqstypes.MessageAttributeValue {
	m := map[string]sqstypes.MessageAttributeValue{
		"route": {DataType: aws.String("String"), StringValue: aws.String(event)},
	}

	for _, attr := range ca {
		m[attr.Title] = sqstypes.MessageAttributeValue{DataType: aws.String(attr.DataType), StringValue: aws.String(attr.Value)}
	}

	return m
}
