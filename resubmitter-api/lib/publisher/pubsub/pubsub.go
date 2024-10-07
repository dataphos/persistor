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

package pubsub

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-brokers/pkg/broker/pubsub"
	"github.com/dataphos/persistor-resubmitter-api/common/log"
	"github.com/dataphos/persistor-resubmitter-api/lib/persistor"
	"github.com/dataphos/persistor-resubmitter-api/lib/publisher"
)

type Publisher struct {
	client *pubsub.Publisher
}

type Topic struct {
	client broker.Topic
}

func FromEnv() (publisher.Publisher, error) {
	projectID := os.Getenv(projectIDEnvKey)
	if projectID == "" {
		return nil, log.EnvVariableNotDefined(projectIDEnvKey)
	}

	settings, err := LoadPublishSettingsFromEnv()
	if err != nil {
		return nil, errors.Wrap(err, "error during publish settings creation")
	}

	config := pubsub.PublisherConfig{
		ProjectID:      projectID,
		GrpcClientConn: nil,
	}

	pubClient, err := pubsub.NewPublisher(context.Background(), config, settings)
	if err != nil {
		return nil, errors.Wrap(err, "Pub/Sub publisher creation failed")
	}

	return &Publisher{client: pubClient}, nil
}

// LoadPublishSettingsFromEnv loads pubsub.ProducerSettings from the expected environment variables.
//
// Any setting which is not set will use the default value taken from pubsub.DefaultProducerSettings.
//
// Returns an error only if a relevant environment variable is set but misconfigured.
func LoadPublishSettingsFromEnv() (pubsub.PublishSettings, error) {
	settings := pubsub.DefaultPublishSettings

	if delayThresholdStr := os.Getenv(delayThresholdEnvKey); delayThresholdStr != "" {
		delayThreshold, err := time.ParseDuration(delayThresholdStr)
		if err != nil {
			return pubsub.PublishSettings{}, errors.Wrap(err, log.ParsingEnvVariableFailed(delayThresholdEnvKey))
		}

		settings.DelayThreshold = delayThreshold
	}

	if countThresholdStr := os.Getenv(countThresholdEnvKey); countThresholdStr != "" {
		countThreshold, err := strconv.Atoi(countThresholdStr)
		if err != nil {
			return pubsub.PublishSettings{}, errors.Wrap(err, log.ParsingEnvVariableFailed(countThresholdEnvKey))
		}

		settings.CountThreshold = countThreshold
	}

	if byteThresholdStr := os.Getenv(byteThresholdEnvKey); byteThresholdStr != "" {
		byteThreshold, err := strconv.ParseFloat(byteThresholdStr, 64)
		if err != nil {
			return pubsub.PublishSettings{}, errors.Wrap(err, log.ParsingEnvVariableFailed(byteThresholdEnvKey))
		}

		byteThresholdInt := int(byteThreshold)
		if byteThresholdInt <= 0 {
			return pubsub.PublishSettings{}, errors.New(log.ParsingEnvVariableFailed(byteThresholdEnvKey))
		}

		settings.ByteThreshold = byteThresholdInt
	}

	if numGoroutinesStr := os.Getenv(numPublishGoroutinesEnvKey); numGoroutinesStr != "" {
		numGoroutines, err := strconv.Atoi(numGoroutinesStr)
		if err != nil {
			return pubsub.PublishSettings{}, errors.Wrap(err, log.ParsingEnvVariableFailed(numPublishGoroutinesEnvKey))
		}

		settings.NumGoroutines = numGoroutines
	}

	if timeoutStr := os.Getenv(timeoutEnvKey); timeoutStr != "" {
		timeout, err := time.ParseDuration(timeoutStr)
		if err != nil {
			return pubsub.PublishSettings{}, errors.Wrap(err, log.ParsingEnvVariableFailed(timeoutEnvKey))
		}

		settings.Timeout = timeout
	}

	if maxOutstandingMessagesStr := os.Getenv(maxPublishOutstandingMessagesEnvKey); maxOutstandingMessagesStr != "" {
		maxOutstandingMessages, err := strconv.Atoi(maxOutstandingMessagesStr)
		if err != nil {
			return pubsub.PublishSettings{}, errors.Wrap(err, log.ParsingEnvVariableFailed(maxPublishOutstandingMessagesEnvKey))
		}

		settings.MaxOutstandingMessages = maxOutstandingMessages
	}

	if maxOutstandingBytesStr := os.Getenv(maxPublishOutstandingBytesEnvKey); maxOutstandingBytesStr != "" {
		maxOutstandingBytes, err := strconv.ParseFloat(maxOutstandingBytesStr, 64)
		if err != nil {
			return pubsub.PublishSettings{}, errors.Wrap(err, log.ParsingEnvVariableFailed(maxPublishOutstandingBytesEnvKey))
		}

		maxOutstandingBytesInt := int(maxOutstandingBytes)
		if maxOutstandingBytesInt <= 0 {
			return pubsub.PublishSettings{}, errors.New(log.ParsingEnvVariableFailed(maxPublishOutstandingBytesEnvKey))
		}

		settings.MaxOutstandingBytes = maxOutstandingBytesInt
	}

	if enableMessageOrderingStr := os.Getenv(publishEnableMessageOrderingEnvKey); enableMessageOrderingStr != "" {
		enableMessageOrdering, err := strconv.ParseBool(enableMessageOrderingStr)
		if err != nil {
			return pubsub.PublishSettings{}, errors.Wrap(err, log.ParsingEnvVariableFailed(publishEnableMessageOrderingEnvKey))
		}
		settings.EnableMessageOrdering = enableMessageOrdering
	}

	return settings, nil
}

func (publisher *Publisher) Topic(topicId string) (publisher.Topic, error) {
	topic, err := publisher.client.Topic(topicId)
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
