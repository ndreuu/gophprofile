package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"gophprofile/internal/domain"
)

type publisherClient interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	Request(context.Context, kmsg.Request) (kmsg.Response, error)
	Ping(context.Context) error
	Close()
}

type Publisher struct {
	client publisherClient
	topic  string
}

func NewPublisher(brokers []string, topic string) (*Publisher, error) {
	if len(brokers) == 0 || topic == "" {
		return nil, fmt.Errorf("kafka brokers and topic must not be empty")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}

	return &Publisher{client: client, topic: topic}, nil
}

func (p *Publisher) Publish(ctx context.Context, event domain.Event) error {
	if err := domain.ValidateEvent(event); err != nil {
		return fmt.Errorf("publish event: %w", err)
	}
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode Kafka event: %w", err)
	}

	record := &kgo.Record{Topic: p.topic, Key: []byte(event.AvatarID), Value: data}
	if err := p.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("publish Kafka event: %w", err)
	}
	return nil
}

func (p *Publisher) Ping(ctx context.Context) error { return p.client.Ping(ctx) }
func (p *Publisher) Close()                         { p.client.Close() }

func (p *Publisher) EnsureTopics(ctx context.Context) error {
	request := kmsg.NewPtrCreateTopicsRequest()
	request.TimeoutMillis = 10000
	for _, name := range []string{p.topic, p.topic + ".dlq"} {
		topic := kmsg.NewCreateTopicsRequestTopic()
		topic.Topic = name
		topic.NumPartitions = 3
		topic.ReplicationFactor = 1
		request.Topics = append(request.Topics, topic)
	}
	response, err := p.client.Request(ctx, request)
	if err != nil {
		return fmt.Errorf("create Kafka topics: %w", err)
	}
	typed, ok := response.(*kmsg.CreateTopicsResponse)
	if !ok {
		return fmt.Errorf("unexpected Kafka create-topics response %T", response)
	}
	for _, topic := range typed.Topics {
		if topic.ErrorCode != 0 && topic.ErrorCode != kerr.TopicAlreadyExists.Code {
			return fmt.Errorf("create Kafka topic %s: %w", topic.Topic, kerr.ErrorForCode(topic.ErrorCode))
		}
	}
	return nil
}

type consumerClient interface {
	PollRecords(context.Context, int) kgo.Fetches
	CommitRecords(context.Context, ...*kgo.Record) error
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	AllowRebalance()
	CloseAllowingRebalance()
}

type Consumer struct {
	client     consumerClient
	topic      string
	retryDelay time.Duration
}

func NewConsumer(brokers []string, topic, group string) (*Consumer, error) {
	if len(brokers) == 0 || topic == "" || group == "" {
		return nil, fmt.Errorf("kafka brokers, topic and group must not be empty")
	}
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.BlockRebalanceOnPoll(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("create Kafka consumer: %w", err)
	}
	return &Consumer{client: client, topic: topic, retryDelay: time.Second}, nil
}

func (c *Consumer) Close() { c.client.CloseAllowingRebalance() }

// Run handles one record at a time. A failed handler is retried before polling
// the next record, so a later offset can never acknowledge unfinished work.
func (c *Consumer) Run(ctx context.Context, handler func(context.Context, domain.Event) error) error {
	if handler == nil {
		return fmt.Errorf("kafka event handler must not be nil")
	}
	defer c.client.AllowRebalance()
	for {
		fetches := c.client.PollRecords(ctx, 1)
		if err := ctx.Err(); err != nil {
			return err
		}
		if fetches.IsClientClosed() {
			return kgo.ErrClientClosed
		}

		for _, record := range fetches.Records() {
			if err := c.handleRecord(ctx, record, handler); err != nil {
				return err
			}
		}
		c.client.AllowRebalance()
		if errs := fetches.Errors(); len(errs) != 0 {
			for _, err := range errs {
				slog.Error("Kafka fetch failed", "topic", err.Topic, "partition", err.Partition, "error", err.Err)
			}
			if err := wait(ctx, c.retryDelay); err != nil {
				return err
			}
		}
	}
}

func (c *Consumer) handleRecord(ctx context.Context, record *kgo.Record, handler func(context.Context, domain.Event) error) error {
	event, err := decodeEvent(record.Value)
	if err != nil {
		return c.deadLetterAndCommit(ctx, record, err)
	}
	delay := c.retryDelay
	for {
		err := handler(ctx, event)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if errors.Is(err, domain.ErrInvalid) || errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrUnsupported) || errors.Is(err, domain.ErrTooLarge) {
			return c.deadLetterAndCommit(ctx, record, err)
		}
		slog.Error("Kafka handler failed; retrying", "message_id", event.ID, "error", err, "retry_in", delay)
		if err := wait(ctx, delay); err != nil {
			return err
		}
		delay = nextDelay(delay)
	}
	return c.commit(ctx, record)
}

func (c *Consumer) deadLetterAndCommit(ctx context.Context, record *kgo.Record, reason error) error {

	dead := &kgo.Record{
		Topic: c.topic + ".dlq", Key: record.Key, Value: record.Value,
		Headers: []kgo.RecordHeader{
			{Key: "error", Value: []byte(reason.Error())},
			{Key: "source_topic", Value: []byte(record.Topic)},
			{Key: "source_partition", Value: []byte(strconv.Itoa(int(record.Partition)))},
			{Key: "source_offset", Value: []byte(strconv.FormatInt(record.Offset, 10))},
		},
	}
	delay := c.retryDelay
	for {
		if err := c.client.ProduceSync(ctx, dead).FirstErr(); err == nil {
			break
		} else {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("Kafka dead-letter publish failed; retrying", "error", err, "retry_in", delay)
		}
		if err := wait(ctx, delay); err != nil {
			return err
		}
		delay = nextDelay(delay)
	}
	return c.commit(ctx, record)
}

func (c *Consumer) commit(ctx context.Context, record *kgo.Record) error {

	if err := c.client.CommitRecords(ctx, record); err != nil {
		return fmt.Errorf("commit Kafka record: %w", err)
	}
	return nil
}

func decodeEvent(data []byte) (domain.Event, error) {
	var event domain.Event
	if err := json.Unmarshal(data, &event); err != nil {
		return domain.Event{}, fmt.Errorf("decode Kafka event: %w: %v", domain.ErrInvalid, err)
	}
	if err := domain.ValidateEvent(event); err != nil {
		return domain.Event{}, err
	}
	return event, nil
}

func nextDelay(delay time.Duration) time.Duration {
	if delay >= 15*time.Second {
		return 30 * time.Second
	}
	return delay * 2
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

var _ domain.Publisher = (*Publisher)(nil)
