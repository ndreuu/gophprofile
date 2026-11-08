package kafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"gophprofile/internal/domain"
)

func validEvent() domain.Event {
	return domain.Event{
		ID: "d0dbd4d8-bb8d-4208-ad6e-5dd2368c342e", Type: domain.EventUploaded,
		AvatarID: "c683c42b-2fb2-4166-bcd8-aa3fd8bcaa1e", UserID: "alice",
		S3Key: "originals/avatar.png", CreatedAt: time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC),
	}
}

func validRecord(t *testing.T) *kgo.Record {
	t.Helper()
	data, err := json.Marshal(validEvent())
	if err != nil {
		t.Fatal(err)
	}
	return &kgo.Record{Topic: "avatars", Key: []byte(validEvent().AvatarID), Value: data, Offset: 12, Partition: 1}
}

type fakeClient struct {
	fetchError  error
	records     []*kgo.Record
	committed   []*kgo.Record
	produceErrs []error
	commitErr   error
	response    kmsg.Response
	request     kmsg.Request
	requestErr  error
	cancel      context.CancelFunc
	polls       int
	allowed     int
	actions     []string
}

func (f *fakeClient) ProduceSync(_ context.Context, records ...*kgo.Record) kgo.ProduceResults {
	f.actions = append(f.actions, "produce")
	f.records = append(f.records, records...)
	var err error
	if len(f.produceErrs) != 0 {
		err, f.produceErrs = f.produceErrs[0], f.produceErrs[1:]
	}
	return kgo.ProduceResults{{Record: records[0], Err: err}}
}
func (f *fakeClient) Request(_ context.Context, request kmsg.Request) (kmsg.Response, error) {
	f.request = request
	return f.response, f.requestErr
}
func (f *fakeClient) Ping(context.Context) error { return nil }
func (f *fakeClient) Close()                     {}
func (f *fakeClient) CloseAllowingRebalance()    {}
func (f *fakeClient) AllowRebalance()            { f.allowed++ }
func (f *fakeClient) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	f.actions = append(f.actions, "commit")
	f.committed = append(f.committed, records...)
	return f.commitErr
}
func (f *fakeClient) PollRecords(ctx context.Context, _ int) kgo.Fetches {
	f.polls++
	if f.polls == 1 {
		return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "avatars", Partitions: []kgo.FetchPartition{{Partition: 1, Records: f.records}, {Partition: 2, Err: f.fetchError}}}}}}
	}
	f.cancel()
	<-ctx.Done()
	return nil
}

func TestPublisherUsesAvatarKeyAndWaitsForAcknowledgment(t *testing.T) {
	client := &fakeClient{}
	publisher := &Publisher{client: client, topic: "avatars"}
	event := validEvent()
	if err := publisher.Publish(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if len(client.records) != 1 || string(client.records[0].Key) != event.AvatarID || client.records[0].Topic != "avatars" {
		t.Fatalf("unexpected records: %+v", client.records)
	}
	got, err := decodeEvent(client.records[0].Value)
	if err != nil || got.ID != event.ID {
		t.Fatalf("got %+v err=%v", got, err)
	}
	client.produceErrs = []error{errors.New("broker unavailable")}
	if err := publisher.Publish(context.Background(), event); err == nil {
		t.Fatal("publish must surface broker rejection")
	}
	event.ID = "bad"
	before := len(client.records)
	if err := publisher.Publish(context.Background(), event); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
	if len(client.records) != before {
		t.Fatal("invalid event reached Kafka")
	}
}

func TestDecodeEventRejectsMalformedOrInvalidEnvelope(t *testing.T) {
	for _, data := range [][]byte{[]byte("invalid"), []byte("{}"), []byte("null")} {
		if _, err := decodeEvent(data); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("%s: expected invalid, got %v", data, err)
		}
	}
	event := validEvent()
	event.Type = "unknown"
	data, _ := json.Marshal(event)
	if _, err := decodeEvent(data); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
}

func TestEnsureTopicsAcceptsExistingAndRejectsErrors(t *testing.T) {
	for _, code := range []int16{0, kerr.TopicAlreadyExists.Code, kerr.TopicAuthorizationFailed.Code} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			client := &fakeClient{response: &kmsg.CreateTopicsResponse{Topics: []kmsg.CreateTopicsResponseTopic{{Topic: "avatars", ErrorCode: code}}}}
			publisher := &Publisher{client: client, topic: "avatars"}
			err := publisher.EnsureTopics(context.Background())
			if (err != nil) != (code == kerr.TopicAuthorizationFailed.Code) {
				t.Fatalf("error=%v", err)
			}
			request := client.request.(*kmsg.CreateTopicsRequest)
			if len(request.Topics) != 2 || request.Topics[1].Topic != "avatars.dlq" {
				t.Fatalf("unexpected topics %+v", request.Topics)
			}
		})
	}
}

func TestHandlerRetriesSameRecordBeforeCommit(t *testing.T) {
	client := &fakeClient{}
	consumer := &Consumer{client: client, topic: "avatars", retryDelay: time.Nanosecond}
	attempts := 0
	err := consumer.handleRecord(context.Background(), validRecord(t), func(context.Context, domain.Event) error {
		attempts++
		if len(client.committed) != 0 {
			t.Fatal("record acknowledged before handler succeeded")
		}
		if attempts < 3 {
			return errors.New("storage temporarily unavailable")
		}
		return nil
	})
	if err != nil || attempts != 3 || len(client.committed) != 1 {
		t.Fatalf("attempts=%d committed=%d err=%v", attempts, len(client.committed), err)
	}
}

func TestMalformedRecordIsDurablyDeadLetteredBeforeCommit(t *testing.T) {
	client := &fakeClient{produceErrs: []error{errors.New("unavailable"), nil}}
	consumer := &Consumer{client: client, topic: "avatars", retryDelay: time.Nanosecond}
	record := validRecord(t)
	record.Value = []byte("malformed JSON")
	err := consumer.handleRecord(context.Background(), record, func(context.Context, domain.Event) error { t.Fatal("invalid JSON must not reach handler"); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(client.records) != 2 || len(client.committed) != 1 {
		t.Fatalf("produced=%d committed=%d", len(client.records), len(client.committed))
	}
	dead := client.records[1]
	if dead.Topic != "avatars.dlq" || !bytes.Equal(dead.Value, record.Value) || len(dead.Headers) != 4 {
		t.Fatalf("unexpected dead letter %+v", dead)
	}
	if fmt.Sprint(client.actions) != "[produce produce commit]" {
		t.Fatalf("wrong acknowledgment order %v", client.actions)
	}
}

func TestPermanentHandlerErrorGoesToDeadLetter(t *testing.T) {
	client := &fakeClient{}
	consumer := &Consumer{client: client, topic: "avatars", retryDelay: time.Nanosecond}
	if err := consumer.handleRecord(context.Background(), validRecord(t), func(context.Context, domain.Event) error { return fmt.Errorf("bad image: %w", domain.ErrInvalid) }); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(client.actions) != "[produce commit]" {
		t.Fatal(client.actions)
	}
}

func TestCanceledRetryDoesNotAcknowledge(t *testing.T) {
	for _, poison := range []bool{false, true} {
		t.Run(fmt.Sprint(poison), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			client := &fakeClient{produceErrs: []error{context.Canceled}}
			consumer := &Consumer{client: client, topic: "avatars", retryDelay: time.Second}
			record := validRecord(t)
			if poison {
				record.Value = []byte("invalid")
				cancel()
			}
			err := consumer.handleRecord(ctx, record, func(context.Context, domain.Event) error { cancel(); return errors.New("network failure") })
			if !errors.Is(err, context.Canceled) || len(client.committed) != 0 {
				t.Fatalf("err=%v committed=%v", err, client.committed)
			}
		})
	}
}

func TestRunStopsOnCommitFailureWithoutPollingNext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &fakeClient{records: []*kgo.Record{validRecord(t)}, commitErr: errors.New("commit unavailable"), cancel: cancel}
	consumer := &Consumer{client: client, topic: "avatars", retryDelay: time.Nanosecond}
	if err := consumer.Run(ctx, func(context.Context, domain.Event) error { return nil }); err == nil {
		t.Fatal("expected commit error")
	}
	if client.polls != 1 || client.allowed == 0 {
		t.Fatalf("polls=%d allowed=%d", client.polls, client.allowed)
	}
}

func TestRunReturnsOnCancellationAfterSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &fakeClient{records: []*kgo.Record{validRecord(t)}, cancel: cancel}
	consumer := &Consumer{client: client, topic: "avatars", retryDelay: time.Nanosecond}
	if err := consumer.Run(ctx, func(context.Context, domain.Event) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(client.committed) != 1 {
		t.Fatalf("committed=%d", len(client.committed))
	}
}

func TestBackoffCapsAtThirtySeconds(t *testing.T) {
	delay := time.Second
	for range 20 {
		delay = nextDelay(delay)
	}
	if delay != 30*time.Second {
		t.Fatal(delay)
	}
}

func TestRunDoesNotDiscardRecordsWhenAnotherPartitionHasFetchError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &fakeClient{records: []*kgo.Record{validRecord(t)}, cancel: cancel, fetchError: errors.New("partition temporarily unavailable")}
	consumer := &Consumer{client: client, topic: "avatars", retryDelay: time.Nanosecond}
	attempts := 0
	if err := consumer.Run(ctx, func(context.Context, domain.Event) error { attempts++; return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if attempts != 1 || len(client.committed) != 1 {
		t.Fatalf("attempts=%d committed=%d", attempts, len(client.committed))
	}
}

func TestOversizedOriginalIsDeadLetteredWithoutBlockingPartition(t *testing.T) {
	client := &fakeClient{}
	consumer := &Consumer{client: client, topic: "avatars", retryDelay: time.Nanosecond}
	attempts := 0
	err := consumer.handleRecord(context.Background(), validRecord(t), func(context.Context, domain.Event) error {
		attempts++
		return fmt.Errorf("original exceeds file size limit: %w", domain.ErrTooLarge)
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 1 || fmt.Sprint(client.actions) != "[produce commit]" {
		t.Fatalf("attempts=%d actions=%v", attempts, client.actions)
	}
}
