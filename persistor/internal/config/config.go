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

package config

// Package config provides the config structs used in the entire persistor project

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/dataphos/persistor/internal/common"
	"github.com/dataphos/persistor/internal/common/log"
	mongowriter "github.com/dataphos/persistor/internal/writer/mongo"
)

const (
	WriterGCS = "gcs"
	WriterABS = "abs"
)

type BrokerType string

type StorageConfig struct {
	Type             string
	Prefix           string
	MsgExtension     string
	Mask             string
	CustomValues     string // CustomValues allows user to define mask parameter aside predefined values
	Destination      string
	StorageAccountID string // StorageAccountID used with ABS writer
	TopicID          string
}

const (
	TypePubSub     BrokerType = "pubsub"
	TypeKafka      BrokerType = "kafka"
	TypeServiceBus BrokerType = "servicebus"
)

type PersistorConfig struct {
	Reader            ReaderConfig
	Storage           StorageConfig
	IndexerEnabled    bool
	DeadLetterEnabled bool
	Sender            SenderConfig
	BatchSettings     BatchSettings
}

type ReaderConfig struct {
	Type       BrokerType
	PubSub     PubSubReceiverConfig
	ServiceBus ServiceBusReceiverConfig
	Kafka      KafkaIteratorConfig
}

type SenderConfig struct {
	Type            BrokerType
	TopicID         string
	DeadLetterTopic string
	PubSub          PubSubSenderConfig
	ServiceBus      ServiceBusSenderConfig
	Kafka           KafkaSenderConfig
}

type IndexerConfig struct {
	Reader            ReaderConfig
	Mongo             mongowriter.MongoConfig
	DeadLetterEnabled bool
	Sender            SenderConfig
	BatchSettings     BatchSettings
}

type KafkaSenderConfig struct {
	Address   string
	TLSConfig TLSConfig `fig:"TLS"`
}

type PubSubSenderConfig struct {
	ProjectID string
}

type ServiceBusSenderConfig struct {
	ConnectionString string
}

type PubSubReceiverConfig struct {
	ProjectID string
	SubID     string
}

type KafkaIteratorConfig struct {
	Address   string
	GroupID   string
	TopicID   string
	TLSConfig TLSConfig `fig:"TLS"`
}

type ServiceBusReceiverConfig struct {
	ConnectionString string
	TopicID          string
	SubID            string
}

type TLSConfig struct {
	Enabled  bool
	CertFile string
	KeyFile  string
	CAFile   string
}

// BatchSettings represents the settings for batch processing
type BatchSettings struct {
	BatchSize int
	// BatchTimeout maximum amount of time to wait for a batch to fill up to BatchSize before processing it anyway
	BatchTimeout time.Duration
	// BatchMemory maximum amount of bytes allowed in a single batch
	BatchMemory int
}

const Redacted = "[redacted]"

// mapFromKeySequence when given a sequence of keys like ["k1, "k2"] will return baseMap["k1"]["k2"] is that value is a map
func mapFromKeySequence(baseMap map[string]interface{}, keys []string) *map[string]interface{} {
	for _, k := range keys {
		val, ok := baseMap[k]
		if !ok {
			return nil
		}

		var isMap bool

		baseMap, isMap = val.(map[string]interface{})
		if !isMap {
			return nil
		}
	}

	return &baseMap
}

// hideSensitiveConfigInfo removes service bus connection string from a config map so it doesn't get logged
func hideSensitiveConfigInfo(configMap *map[string]interface{}) {
	// hide reader connection string
	if readerConfig := mapFromKeySequence(*configMap, []string{"Reader"}); readerConfig != nil && (*readerConfig)["Type"] == string(TypeServiceBus) {
		if sbReaderConfig := mapFromKeySequence(*readerConfig, []string{"ServiceBus"}); sbReaderConfig != nil {
			(*sbReaderConfig)["ConnectionString"] = Redacted
		}
	}
	// hide sender connection string
	if senderConfig := mapFromKeySequence(*configMap, []string{"Sender"}); senderConfig != nil && (*senderConfig)["Type"] == string(TypeServiceBus) {
		if sbReaderConfig := mapFromKeySequence(*senderConfig, []string{"ServiceBus"}); sbReaderConfig != nil {
			(*sbReaderConfig)["ConnectionString"] = Redacted
		}
	}
}

// configToLogFields takes a map obtained from a config structure and returns log.F to be logged in json format
func configToLogFields(configMap *map[string]interface{}) log.F {
	hideSensitiveConfigInfo(configMap)

	configFields := log.F{}

	for key, val := range *configMap {
		configFields[key] = val
	}

	return configFields
}

func configToMap(config any, configMap *map[string]interface{}) error {
	if configBytes, err := json.Marshal(config); err != nil {
		return fmt.Errorf("marshaling json: %w", err)
	} else if err = json.Unmarshal(configBytes, &configMap); err != nil {
		return fmt.Errorf("unmarshaling json: %w", err)
	}

	return nil
}

// NewTLSConfig creates and returns a tls.Config if it is not enabled then the function returns nil
func NewTLSConfig(enabled bool, clientCertFile, clientKeyFile, caCertFile string) (*tls.Config, error) {
	if !enabled {
		return nil, nil
	}

	if clientCertFile == "" || clientKeyFile == "" || caCertFile == "" {
		return nil, fmt.Errorf("clientCertFile, clientKeyFile, and caCertFile cannot be empty")
	}

	tlsConfig := tls.Config{MinVersion: tls.VersionTLS13}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load client certificate: %w", err)
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	// Load CA cert
	caCert, err := os.ReadFile(caCertFile) // #nosec
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	return &tlsConfig, nil
}

// Log prints config to stdout with the help of the log package by setting the log.F fields equal to the config struct fields, and hides sensitive info
func (persistorConfig *PersistorConfig) Log() {
	configMap := map[string]interface{}{}

	if err := configToMap(persistorConfig, &configMap); err != nil {
		log.Errorw("Error printing Persistor config:", common.ConfigurationError, log.F{log.ErrorFieldKey: err.Error()})

		return
	}

	log.Infow("Persistor config", configToLogFields(&configMap))
}

// Log prints config to stdout with the help of the log package
// by setting the log.F fields equal to the config struct fields, and hides sensitive info
func (indexerConfig *IndexerConfig) Log() {
	configMap := map[string]interface{}{}

	if err := configToMap(indexerConfig, &configMap); err != nil {
		log.Errorw("Error printing Persistor config:", common.ConfigurationError, log.F{log.ErrorFieldKey: err.Error()})

		return
	}

	log.Infow("Indexer config", configToLogFields(&configMap))
}
