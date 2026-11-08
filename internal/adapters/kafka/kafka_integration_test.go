//go:build integration

package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"gophprofile/internal/domain"
)

type kafkaFixture struct {
	ctx       context.Context
	cluster   *kfake.Cluster
	publisher *Publisher
	topic     string
	group     string
}

func newKafkaFixture(t *testing.T) kafkaFixture {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	topic := "avatars-" + uuid.NewString()
	publisher, err := NewPublisher(cluster.ListenAddrs(), topic)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(publisher.Close)
	if err := publisher.EnsureTopics(ctx); err != nil {
		t.Fatal(err)
	}
	return kafkaFixture{ctx: ctx, cluster: cluster, publisher: publisher, topic: topic, group: "workers-" + uuid.NewString()}
}

func integrationEvent() domain.Event {
	return domain.Event{
		ID: uuid.NewString(), Type: domain.EventUploaded, AvatarID: uuid.NewString(), UserID: "integration-user",
		S3Key: "originals/integration.png", CreatedAt: time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC),
	}
}

type consumerRun struct {
	consumer *Consumer
	cancel   context.CancelFunc
	done     chan error
	once     sync.Once
}

func startKafkaConsumer(t *testing.T, fixture kafkaFixture, handler func(context.Context, domain.Event) error) *consumerRun {
	t.Helper()
	consumer, err := NewConsumer(fixture.cluster.ListenAddrs(), fixture.topic, fixture.group)
	if err != nil {
		t.Fatal(err)
	}
	consumer.retryDelay = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(fixture.ctx)
	run := &consumerRun{consumer: consumer, cancel: cancel, done: make(chan error, 1)}
	go func() { run.done <- consumer.Run(ctx, handler) }()
	t.Cleanup(func() { run.stop(t) })
	return run
}

func (run *consumerRun) stop(t *testing.T) {
	t.Helper()
	run.once.Do(func() {
		run.cancel()
		select {
		case err := <-run.done:
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("consumer stopped before cancellation: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("consumer did not stop after context cancellation")
		}
		run.consumer.Close()
	})
}

func receiveEvent(t *testing.T, ctx context.Context, received <-chan domain.Event) domain.Event {
	t.Helper()
	select {
	case event := <-received:
		return event
	case <-ctx.Done():
		t.Fatalf("timed out waiting for Kafka delivery: %v", ctx.Err())
		return domain.Event{}
	}
}

func recordDeliveries(received chan<- domain.Event) func(context.Context, domain.Event) error {
	return func(ctx context.Context, event domain.Event) error {
		select {
		case received <- event:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func waitCommitted(t *testing.T, fixture kafkaFixture, offset int64) {
	t.Helper()
	_, err := fixture.cluster.WaitGroupInfo(fixture.ctx, fixture.group, func(info *kfake.GroupInfo) bool {
		if info == nil {
			return false
		}
		var committed int64
		for _, partition := range info.Commits[fixture.topic] {
			committed += partition.Offset
		}
		return committed >= offset
	})
	if err != nil {
		t.Fatalf("group did not commit offset %d: %v", offset, err)
	}
}

func TestKafkaIntegrationEnsuresTopicsAndDeliversEvent(t *testing.T) {
	fixture := newKafkaFixture(t)

	if err := fixture.publisher.EnsureTopics(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	for _, topic := range []string{fixture.topic, fixture.topic + ".dlq"} {
		if fixture.cluster.TopicInfo(topic) == nil || len(fixture.cluster.PartitionInfos(topic)) != 3 {
			t.Fatalf("topic %q was not created with three partitions", topic)
		}
	}
	if err := fixture.publisher.Ping(fixture.ctx); err != nil {
		t.Fatal(err)
	}
	want := integrationEvent()
	if err := fixture.publisher.Publish(fixture.ctx, want); err != nil {
		t.Fatal(err)
	}
	received := make(chan domain.Event, 4)
	run := startKafkaConsumer(t, fixture, recordDeliveries(received))
	if got := receiveEvent(t, fixture.ctx, received); !reflect.DeepEqual(got, want) {
		t.Fatalf("event changed in transit: got=%+v want=%+v", got, want)
	}
	waitCommitted(t, fixture, 1)
	run.stop(t)
}

func TestKafkaIntegrationRetriesSameMessageBeforeNextOffset(t *testing.T) {
	fixture := newKafkaFixture(t)
	first := integrationEvent()
	second := first
	second.ID = uuid.NewString()

	for _, event := range []domain.Event{first, second} {
		if err := fixture.publisher.Publish(fixture.ctx, event); err != nil {
			t.Fatal(err)
		}
	}
	attempts := make(chan domain.Event, 8)
	var firstAttempts atomic.Int32
	run := startKafkaConsumer(t, fixture, func(ctx context.Context, event domain.Event) error {
		if err := recordDeliveries(attempts)(ctx, event); err != nil {
			return err
		}
		if event.ID == first.ID && firstAttempts.Add(1) < 3 {
			return errors.New("temporary storage failure")
		}
		return nil
	})
	for index, wantID := range []string{first.ID, first.ID, first.ID, second.ID} {
		if got := receiveEvent(t, fixture.ctx, attempts); got.ID != wantID {
			t.Fatalf("attempt %d skipped unfinished work: got=%s want=%s", index+1, got.ID, wantID)
		}
	}
	waitCommitted(t, fixture, 2)
	run.stop(t)
}

func TestKafkaIntegrationRestartResumesAfterCommittedMessage(t *testing.T) {
	fixture := newKafkaFixture(t)
	first := integrationEvent()
	if err := fixture.publisher.Publish(fixture.ctx, first); err != nil {
		t.Fatal(err)
	}
	receivedBefore := make(chan domain.Event, 4)
	before := startKafkaConsumer(t, fixture, recordDeliveries(receivedBefore))
	if got := receiveEvent(t, fixture.ctx, receivedBefore); got.ID != first.ID {
		t.Fatalf("unexpected first delivery %s", got.ID)
	}
	waitCommitted(t, fixture, 1)
	before.stop(t)

	receivedAfter := make(chan domain.Event, 4)
	after := startKafkaConsumer(t, fixture, recordDeliveries(receivedAfter))
	if _, err := fixture.cluster.WaitGroupStable(fixture.ctx, fixture.group, 1); err != nil {
		t.Fatalf("restarted consumer did not join group: %v", err)
	}
	second := first
	second.ID = uuid.NewString()
	if err := fixture.publisher.Publish(fixture.ctx, second); err != nil {
		t.Fatal(err)
	}
	if got := receiveEvent(t, fixture.ctx, receivedAfter); got.ID != second.ID {
		t.Fatalf("restart replayed a committed message: got=%s want=%s", got.ID, second.ID)
	}
	waitCommitted(t, fixture, 2)
	after.stop(t)
}

func TestKafkaIntegrationPoisonJSONGoesToDLQBeforeNextMessage(t *testing.T) {
	fixture := newKafkaFixture(t)
	producer, err := kgo.NewClient(kgo.SeedBrokers(fixture.cluster.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(producer.Close)
	valid := integrationEvent()
	encoded, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	poison := &kgo.Record{Topic: fixture.topic, Partition: 0, Key: []byte(valid.AvatarID), Value: []byte("{invalid-json")}
	marker := &kgo.Record{Topic: fixture.topic, Partition: 0, Key: []byte(valid.AvatarID), Value: encoded}
	if err := producer.ProduceSync(fixture.ctx, poison, marker).FirstErr(); err != nil {
		t.Fatal(err)
	}
	received := make(chan domain.Event, 4)
	run := startKafkaConsumer(t, fixture, recordDeliveries(received))
	if got := receiveEvent(t, fixture.ctx, received); got.ID != valid.ID {
		t.Fatalf("poison payload reached the handler: %+v", got)
	}
	waitCommitted(t, fixture, 2)

	reader, err := kgo.NewClient(kgo.SeedBrokers(fixture.cluster.ListenAddrs()...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			fixture.topic + ".dlq": {0: kgo.NewOffset().AtStart(), 1: kgo.NewOffset().AtStart(), 2: kgo.NewOffset().AtStart()},
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reader.Close)
	var dead *kgo.Record
	for dead == nil {
		fetches := reader.PollRecords(fixture.ctx, 1)
		if errs := fetches.Errors(); len(errs) != 0 {
			t.Fatalf("read DLQ: %v", errs)
		}
		if err := fixture.ctx.Err(); err != nil {
			t.Fatalf("timed out waiting for DLQ: %v", err)
		}
		if records := fetches.Records(); len(records) != 0 {
			dead = records[0]
		}
	}
	if !bytes.Equal(dead.Value, poison.Value) || !bytes.Equal(dead.Key, poison.Key) {
		t.Fatalf("DLQ changed original payload or key: %+v", dead)
	}
	headers := make(map[string]string)
	for _, header := range dead.Headers {
		headers[header.Key] = string(header.Value)
	}
	if headers["error"] == "" || headers["source_topic"] != fixture.topic ||
		headers["source_partition"] != "0" || headers["source_offset"] != strconv.FormatInt(poison.Offset, 10) {
		t.Fatalf("DLQ omitted original coordinates or error: %v", headers)
	}
	run.stop(t)
}

func TestKafkaIntegrationIdleConsumerHonorsCancellation(t *testing.T) {
	fixture := newKafkaFixture(t)
	run := startKafkaConsumer(t, fixture, func(context.Context, domain.Event) error {
		t.Error("empty topic unexpectedly delivered an event")
		return nil
	})
	if _, err := fixture.cluster.WaitGroupStable(fixture.ctx, fixture.group, 1); err != nil {
		t.Fatal(err)
	}
	run.stop(t)
}
