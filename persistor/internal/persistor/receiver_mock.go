// Copyright 2024 Syntio Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package persistor

import (
	"context"
	"fmt"
	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/pkg/errors"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

type FakeTopic struct {
	MessageChan    chan broker.Message
	PublishTimeout time.Duration
	AllSent        []broker.Message
	SentMutex      sync.Mutex
	AckedIDs       map[string]struct{}
	NackedIDs      map[string]struct{}
	AckMutex       sync.Mutex
}

func NewMockTopic(chanSize int, timeout time.Duration) *FakeTopic {
	return &FakeTopic{
		MessageChan:    make(chan broker.Message, chanSize),
		PublishTimeout: timeout,
		AllSent:        []broker.Message{},
		SentMutex:      sync.Mutex{},
		AckedIDs:       map[string]struct{}{},
		NackedIDs:      map[string]struct{}{},
		AckMutex:       sync.Mutex{},
	}
}

func (topic *FakeTopic) Ack(message broker.Message) {
	topic.AckMutex.Lock()
	defer topic.AckMutex.Unlock()
	topic.AckedIDs[message.ID] = struct{}{}
}

func (topic *FakeTopic) Nack(message broker.Message) {
	topic.AckMutex.Lock()
	defer topic.AckMutex.Unlock()
	topic.NackedIDs[message.ID] = struct{}{}
}

type PublisherFakeTopic struct {
	Topic          *FakeTopic
	ShouldFail     func(msg broker.OutboundMessage) bool
	FailedMessages []broker.OutboundMessage
	FailedMutex    sync.Mutex
}

func (topic *PublisherFakeTopic) Publish(ctx context.Context, msg broker.OutboundMessage) error {
	if topic.ShouldFail != nil && topic.ShouldFail(msg) {
		topic.FailedMutex.Lock()
		topic.FailedMessages = append(topic.FailedMessages, msg)
		topic.FailedMutex.Unlock()

		return errors.New("pretend sender failed")
	}
	idSize := 20
	randID := make([]byte, idSize)

	//nolint:gosec // weakness of rng does not matter
	_, _ = rand.Read(randID)
	sendMsg := broker.Message{
		ID:            fmt.Sprintf("%d_%s", time.Now().Unix(), string(randID)),
		Key:           "",
		Data:          msg.Data,
		Attributes:    msg.Attributes,
		PublishTime:   time.Now(),
		IngestionTime: time.Now(),
	}
	sendMsg.AckFunc = func() { topic.Topic.Ack(sendMsg) }
	sendMsg.NackFunc = func() { topic.Topic.Nack(sendMsg) }

	return topic.Topic.Send(ctx, sendMsg)
}

func (topic *PublisherFakeTopic) BatchPublish(ctx context.Context, msgs ...broker.OutboundMessage) error {
	if topic.ShouldFail != nil && topic.ShouldFail(msgs[0]) {
		topic.FailedMutex.Lock()
		topic.FailedMessages = append(topic.FailedMessages, msgs...)
		topic.FailedMutex.Unlock()

		return errors.New("pretend sender failed")
	}

	idSize := 20
	for _, msg := range msgs {
		randID := make([]byte, idSize)

		//nolint:gosec // weakness of rng does not matter
		_, _ = rand.Read(randID)
		sendMsg := broker.Message{
			ID:            fmt.Sprintf("%d_%s", time.Now().Unix(), string(randID)),
			Key:           "",
			Data:          msg.Data,
			Attributes:    msg.Attributes,
			PublishTime:   time.Now(),
			IngestionTime: time.Now(),
		}
		sendMsg.AckFunc = func() { topic.Topic.Ack(sendMsg) }
		sendMsg.NackFunc = func() { topic.Topic.Nack(sendMsg) }

		if err := topic.Topic.Send(ctx, sendMsg); err != nil {
			return err
		}
	}

	return nil
}

func (topic *FakeTopic) Send(ctx context.Context, msg broker.Message) error {
	if topic.PublishTimeout > 0 {
		select {
		case <-time.After(topic.PublishTimeout):
			return errors.New("timeout ran out")
		case <-ctx.Done():
			return context.Canceled
		case topic.MessageChan <- msg:
			topic.SentMutex.Lock()
			defer topic.SentMutex.Unlock()
			topic.AllSent = append(topic.AllSent, msg)

			return nil
		}
	}
	select {
	case <-ctx.Done():
		return context.Canceled
	case topic.MessageChan <- msg:
		topic.SentMutex.Lock()
		defer topic.SentMutex.Unlock()
		topic.AllSent = append(topic.AllSent, msg)

		return nil
	}
}

const mockDefaultMaxBatchSize = 1000

func (topic *FakeTopic) Received(ctx context.Context) (map[string]struct{}, error) {
	receiver := &MockBatchReceiver{
		Timeout:      time.Second,
		MessageChan:  topic.MessageChan,
		MaxBatchSize: mockDefaultMaxBatchSize,
	}
	datas := map[string]struct{}{}
	err := receiver.ReceiveBatch(ctx, func(ctx context.Context, messages []broker.Message) {
		for _, msg := range messages {
			datas[string(msg.Data)] = struct{}{}
		}
	})

	return datas, err
}

func (topic *FakeTopic) Close() {
	close(topic.MessageChan)
}

func (topic *FakeTopic) PublishRandomMessages(ctx context.Context, numMsgs int, attributeKeys []string, attributeValues [][]string, keyMissingProbs []float32) {
	numPublished := 0

	for {
		attrs := map[string]interface{}{}

		for iKey, key := range attributeKeys {
			// skip this key for current message if probability is given
			if len(keyMissingProbs) > iKey && keyMissingProbs[iKey] > 0 {
				//nolint:gosec // weakness of rng does not matter
				if rand.Float32() < keyMissingProbs[iKey] {
					continue
				}
			}
			possibleValues := attributeValues[iKey]

			if len(possibleValues) > 0 {
				//nolint:gosec // weakness of rng does not matter
				attrs[key] = possibleValues[rand.Intn(len(possibleValues))]
			}
		}
		id := strconv.Itoa(len(topic.AllSent))
		msg := broker.Message{
			ID:            id,
			Key:           "",
			Data:          []byte(id),
			Attributes:    attrs,
			PublishTime:   time.Now(),
			IngestionTime: time.Now(),
		}
		msg.AckFunc = func() { topic.Ack(msg) }
		msg.NackFunc = func() { topic.Nack(msg) }
		err := topic.Send(ctx, msg)

		if err == nil {
			numPublished++
		}

		if numPublished == numMsgs {
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
		}
	}
}

type MockBatchReceiver struct {
	Timeout      time.Duration
	MessageChan  chan broker.Message
	MaxBatchSize int
}

func (rec *MockBatchReceiver) ReceiveBatch(ctx context.Context, callback func(context.Context, []broker.Message)) error {
	messages := make([]broker.Message, 0, rec.MaxBatchSize)

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-rec.MessageChan:
			if !ok {
				return nil
			}

			messages = append(messages, msg)
			if len(messages) >= rec.MaxBatchSize {
				callback(ctx, messages)
				messages = make([]broker.Message, 0, rec.MaxBatchSize)
			}
		case <-time.After(rec.Timeout):
			if len(messages) > 0 {
				callback(ctx, messages)
				messages = make([]broker.Message, 0, rec.MaxBatchSize)
			}
		}
	}
}
