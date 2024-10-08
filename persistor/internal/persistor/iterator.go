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
	"crypto/tls"
	"fmt"

	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-brokers/pkg/broker/kafka"
	"github.com/dataphos/persistor/internal/config"
)

const (
	minBatchMemory       = 1024
	maxConcurrentFetches = 3
	minBytes             = 100
)

func NewKafkaIterator(ctx context.Context, iteratorConfig config.KafkaIteratorConfig, batchSettings config.BatchSettings) (iterator broker.BatchedIterator, err error) {
	var tlsConfig *tls.Config

	if tlsConfig, err = config.NewTLSConfig(iteratorConfig.TLSConfig.Enabled, iteratorConfig.TLSConfig.CertFile, iteratorConfig.TLSConfig.KeyFile, iteratorConfig.TLSConfig.CAFile); err != nil {
		return nil, fmt.Errorf("tls iterator config initialization: %w", err)
	}

	consumerConfig := kafka.ConsumerConfig{
		BrokerAddr: iteratorConfig.Address,
		GroupID:    iteratorConfig.GroupID,
		Topic:      iteratorConfig.TopicID,
		TLS:        tlsConfig,
	}

	batchMemory := batchSettings.BatchMemory
	if batchMemory < minBatchMemory {
		batchMemory = minBatchMemory
	}

	kafkaBatchConsumerSettings := kafka.BatchConsumerSettings{
		ConsumerSettings: kafka.ConsumerSettings{
			MinBytes:             minBytes,
			MaxWait:              batchSettings.BatchTimeout,
			MaxBytes:             batchMemory,
			MaxConcurrentFetches: maxConcurrentFetches,
			Transactional:        false,
		},
		MaxPollRecords: batchSettings.BatchSize,
	}

	iterator, err = kafka.NewBatchIterator(ctx, consumerConfig, kafkaBatchConsumerSettings)
	if err != nil {
		return nil, fmt.Errorf("iterator initialization: %w", err)
	}

	return iterator, nil
}
