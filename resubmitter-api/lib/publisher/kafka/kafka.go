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

// Package kafka contains kafka logic for Resubmitter-API
package kafka

import (
	"context"
	"crypto/tls"
	"os"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-brokers/pkg/broker/kafka"
	"github.com/dataphos/lib-httputil/pkg/httputil"
	"github.com/dataphos/persistor-resubmitter-api/common/log"
	"github.com/dataphos/persistor-resubmitter-api/lib/persistor"
	"github.com/dataphos/persistor-resubmitter-api/lib/publisher"
)

const (
	SASLUsername = "SASL_USERNAME"
	SASLPassword = "SASL_PASSWORD"
)

type Publisher struct {
	client *kafka.Publisher
}

type Topic struct {
	client broker.Topic
}

func FromEnv() (publisher.Publisher, error) {
	config, err := LoadProducerConfigFromEnv()
	if err != nil {
		return nil, errors.Wrap(err, "error during producer config creation")
	}

	settings, err := LoadProducerSettingsFromEnv()
	if err != nil {
		return nil, errors.Wrap(err, "error during producer settings creation")
	}

	pubClient, err := kafka.NewPublisher(context.Background(), config, settings)
	if err != nil {
		return nil, errors.Wrap(err, "kafka publisherClient creation failed")
	}

	return &Publisher{client: pubClient}, nil
}

// LoadProducerConfigFromEnv loads kafka.ProducerConfig from the expected environment variables.
//
// Returns an error if any environment variable is not defined or if they're misconfigured.
func LoadProducerConfigFromEnv() (kafka.ProducerConfig, error) {
	brokerAddrs := os.Getenv(brokerAddrsEnvKey)
	if brokerAddrs == "" {
		return kafka.ProducerConfig{}, log.EnvVariableNotDefined(brokerAddrsEnvKey)
	}

	var tlsConfig *tls.Config

	if useTlsStr := os.Getenv(tlsEnabledEnvKey); useTlsStr != "" {
		tlsEnabled, err := strconv.ParseBool(useTlsStr)
		if err != nil {
			return kafka.ProducerConfig{}, err
		}

		if tlsEnabled {
			tlsConfig, err = httputil.NewTLSConfigFromEnv()
			if err != nil {
				return kafka.ProducerConfig{}, err
			}
		}
	}

	var skipVerify bool

	if skipVerifyStr := os.Getenv(skipVerifyEnvKey); skipVerifyStr != "" {
		skipVerifyParsed, err := strconv.ParseBool(skipVerifyStr)
		if err != nil {
			return kafka.ProducerConfig{}, errors.Wrap(err, log.ParsingEnvVariableFailed(skipVerifyEnvKey))
		}

		skipVerify = skipVerifyParsed
	}

	var kerberosConfig *kafka.KerberosConfig

	if useKerberos := os.Getenv(enableKerberosKey); useKerberos != "" {
		kerberosEnabled, err := strconv.ParseBool(useKerberos)
		if err != nil {
			return kafka.ProducerConfig{}, err
		}

		if kerberosEnabled {
			kerberosConfig, err = LoadKerberosConfigFromEnv()
			if err != nil {
				return kafka.ProducerConfig{}, err
			}
		}
	}

	var saslConfig *kafka.PlainSASLConfig

	if useSASLStr := os.Getenv(saslEnabledEnvKey); useSASLStr != "" {
		saslEnabled, err := strconv.ParseBool(useSASLStr)
		if err != nil {
			return kafka.ProducerConfig{}, err
		}

		if saslEnabled {
			SASLUsername := os.Getenv(SASLUsername)
			if SASLUsername == "" {
				return kafka.ProducerConfig{}, log.EnvVariableNotDefined(SASLUsername)
			}

			SASLPassword := os.Getenv(SASLPassword)
			if SASLPassword == "" {
				return kafka.ProducerConfig{}, log.EnvVariableNotDefined(SASLPassword)
			}
			saslConfig = &kafka.PlainSASLConfig{
				User: SASLUsername,
				Pass: SASLPassword,
			}
		}
	}

	var disableCompression bool

	if disableCompressionStr := os.Getenv(disableCompressionEnvKey); disableCompressionStr != "" {
		disableCompressionParsed, err := strconv.ParseBool(disableCompressionStr)
		if err != nil {
			return kafka.ProducerConfig{}, errors.Wrap(err, log.ParsingEnvVariableFailed(disableCompressionEnvKey))
		}

		disableCompression = disableCompressionParsed
	}

	return kafka.ProducerConfig{
		BrokerAddr:         brokerAddrs,
		TLS:                tlsConfig,
		InsecureSkipVerify: skipVerify,
		Kerberos:           kerberosConfig,
		PlainSASL:          saslConfig,
		DisableCompression: disableCompression,
	}, nil
}

// LoadProducerSettingsFromEnv loads kafka.ProducerSettings from the expected environment variables.
//
// Any setting which is not set will use the default value taken from kafka.DefaultProducerSettings.
//
// Returns an error only if a relevant environment variable is set but misconfigured.
func LoadProducerSettingsFromEnv() (kafka.ProducerSettings, error) {
	settings := kafka.DefaultProducerSettings

	if batchSizeStr := os.Getenv(publisherBatchSize); batchSizeStr != "" {
		batchSize, err := strconv.Atoi(batchSizeStr)
		if err != nil {
			return kafka.ProducerSettings{}, errors.Wrap(err, log.ParsingEnvVariableFailed(publisherBatchSize))
		}

		settings.BatchSize = batchSize
	}

	if batchBytesStr := os.Getenv(batchBytesEnvKey); batchBytesStr != "" {
		batchBytes, err := strconv.Atoi(batchBytesStr)
		if err != nil {
			return kafka.ProducerSettings{}, errors.Wrap(err, log.ParsingEnvVariableFailed(batchBytesEnvKey))
		}

		settings.BatchBytes = int64(batchBytes)
	}

	if lingerStr := os.Getenv(lingerEnvKey); lingerStr != "" {
		linger, err := time.ParseDuration(lingerStr)
		if err != nil {
			return kafka.ProducerSettings{}, errors.Wrap(err, log.ParsingEnvVariableFailed(lingerEnvKey))
		}

		settings.Linger = linger
	}

	return settings, nil
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
