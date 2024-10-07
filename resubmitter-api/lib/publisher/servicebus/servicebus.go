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

// Package servicebus contains servicebus logic for Resubmitter-API
package servicebus

import (
	"context"
	"os"

	"github.com/pkg/errors"

	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-brokers/pkg/broker/servicebus"
	"github.com/dataphos/persistor-resubmitter-api/common/log"
	"github.com/dataphos/persistor-resubmitter-api/lib/persistor"
	"github.com/dataphos/persistor-resubmitter-api/lib/publisher"
)

const (
	connectionStringEnvKey = "SB_CONNECTION_STRING"
)

type Publisher struct {
	client *servicebus.Publisher
}

type Topic struct {
	client broker.Topic
}

func FromEnv() (*Publisher, error) {
	connectionString := os.Getenv(connectionStringEnvKey)
	if connectionString == "" {
		return nil, log.EnvVariableNotDefined(connectionStringEnvKey)
	}

	client, err := servicebus.NewPublisher(connectionString)
	if err != nil {
		return nil, errors.Wrap(err, "Service Bus publisher creation failed")
	}

	return &Publisher{client: client}, nil
}

func (publisher *Publisher) Topic(topicID string) (publisher.Topic, error) {
	topic, err := publisher.client.Topic(topicID)
	if err != nil {
		return nil, errors.Wrap(err, "creating topic failed")
	}

	return &Topic{client: topic}, nil
}

func (topic *Topic) Publish(ctx context.Context, record *persistor.Record) error {
	attributes := publisher.CreateAttributes(record)
	orderingKey := publisher.GetOrderingKeyFromAttributes(attributes)

	message := broker.OutboundMessage{
		Key:        orderingKey,
		Data:       record.Data,
		Attributes: attributes,
	}

	err := topic.client.Publish(ctx, message)
	if err != nil {
		return errors.Wrapf(err, "can't publish persistor record with id: %s", record.ID)
	}

	return nil
}
