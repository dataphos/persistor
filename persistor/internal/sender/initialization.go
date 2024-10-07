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

package sender

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"

	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-brokers/pkg/broker/kafka"
	"github.com/dataphos/lib-brokers/pkg/broker/pubsub"
	"github.com/dataphos/lib-brokers/pkg/broker/servicebus"
	"github.com/dataphos/persistor/internal/config"
)

var (
	ErrSenderTypeNotRecognized       = errors.New("sender type not recognized")
	ErrTLSSenderConfigInitialization = errors.New("TLS sender config initialization failed")
)

func NewTopic(ctx context.Context, senderConfig config.SenderConfig, topicID string, batchSize, batchBytes int) (topic broker.Topic, tolerateDeadLetterErrors bool, err error) {
	var publisher broker.Publisher

	tolerateDeadLetterErrors = true

	switch senderConfig.Type {
	case config.TypePubSub:
		publishSettings := pubsub.DefaultPublishSettings
		publishSettings.MaxOutstandingMessages = batchSize
		publishSettings.CountThreshold = batchSize

		publisher, err = pubsub.NewPublisher(
			ctx,
			pubsub.PublisherConfig{
				ProjectID: senderConfig.PubSub.ProjectID,
			},
			publishSettings,
		)
	case config.TypeServiceBus:
		publisher, err = servicebus.NewPublisher(senderConfig.ServiceBus.ConnectionString)
	case config.TypeKafka:
		producerSettings := kafka.DefaultProducerSettings
		// since every batch needs to be processed before the next one is consumed, the consumer batch settings
		// give the maximum needed producer buffer size.
		producerSettings.BatchSize = batchSize
		producerSettings.BatchBytes = int64(batchBytes)

		var tlsConfig *tls.Config

		if tlsConfig, err = config.NewTLSConfig(senderConfig.Kafka.TLSConfig.Enabled, senderConfig.Kafka.TLSConfig.CertFile, senderConfig.Kafka.TLSConfig.KeyFile, senderConfig.Kafka.TLSConfig.CAFile); err != nil {
			return nil, false, fmt.Errorf("%w: %v", ErrTLSSenderConfigInitialization, err)
		}

		publisher, err = kafka.NewPublisher(
			ctx,
			kafka.ProducerConfig{
				BrokerAddr: senderConfig.Kafka.Address,
				TLS:        tlsConfig,
			},
			producerSettings,
		)
		tolerateDeadLetterErrors = false
	default:
		return nil, false, fmt.Errorf("%w: %s", ErrSenderTypeNotRecognized, senderConfig.Type)
	}

	if err != nil {
		return nil, false, fmt.Errorf("initializing publisher: %w", err)
	}

	topic, err = publisher.Topic(topicID)
	if err != nil {
		return nil, false, fmt.Errorf("initializing topic: %w", err)
	}

	return topic, tolerateDeadLetterErrors, nil
}
