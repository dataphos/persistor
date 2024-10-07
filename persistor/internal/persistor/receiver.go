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
	"github.com/dataphos/lib-brokers/pkg/broker/pubsub"
	"github.com/dataphos/lib-brokers/pkg/broker/servicebus"
	"github.com/dataphos/lib-brokers/pkg/brokerutil"
	"github.com/dataphos/persistor/internal/config"
)

// Rename to kafkaIterator/Receiver...

// NewPubSubReceiver returns a broker.BatchedReceiver that receives messages from a pubsub.Receiver.
func NewPubSubReceiver(ctx context.Context, config config.PubSubReceiverConfig, batchSettings config.BatchSettings) (receiver broker.BatchedReceiver, err error) {
	receiverConfig := pubsub.ReceiverConfig{
		ProjectID:      config.ProjectID,
		SubscriptionID: config.SubID,
	}
	receiveSettings := pubsub.DefaultReceiveSettings
	// we must be able to pull simultaneously at least as much as our batch settings dictate, otherwise we will be waiting for timeout to hit.
	if batchSettings.BatchSize > receiveSettings.MaxOutstandingMessages {
		receiveSettings.MaxOutstandingMessages = batchSettings.BatchSize
	}

	if batchSettings.BatchMemory > receiveSettings.MaxOutstandingBytes {
		receiveSettings.MaxOutstandingBytes = batchSettings.BatchMemory
	}

	rec, err := pubsub.NewReceiver(ctx, receiverConfig, receiveSettings)

	if err != nil {
		return nil, fmt.Errorf("receiver initialization: %w", err)
	}

	return brokerutil.ReceiverIntoBatchedReceiver(rec,
		brokerutil.IntoBatchedMessageStreamSettings{
			BatchSize:    batchSettings.BatchSize,
			BatchTimeout: batchSettings.BatchTimeout,
		},

		brokerutil.IntoBatchedReceiverSettings{NumGoroutines: 1}), nil
}

// NewServiceBusReceiver makes a service bus iterator, converts it to a broker.BatchedReceiver and uses it to build another broker.BatchedReceiver which will not call the callback until the batch is full or time has run out.
func NewServiceBusReceiver(config config.ServiceBusReceiverConfig, batchSettings config.BatchSettings) (receiver broker.BatchedReceiver, err error) {
	iteratorConfig := servicebus.IteratorConfig{
		ConnectionString: config.ConnectionString,
		Topic:            config.TopicID,
		Subscription:     config.SubID,
	}

	iterator, err := servicebus.NewBatchIterator(iteratorConfig, servicebus.BatchIteratorSettings{BatchSize: batchSettings.BatchSize})
	if err != nil {
		return nil, fmt.Errorf("iterator initialization: %w", err)
	}
	batchReceiver := brokerutil.BatchedMessageIteratorIntoBatchedReceiver(iterator, brokerutil.IntoBatchedReceiverSettings{NumGoroutines: 1})

	return batchReceiver, nil
}
