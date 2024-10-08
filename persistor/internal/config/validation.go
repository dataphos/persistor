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

import (
	"fmt"

	"github.com/dataphos/persistor/internal/common/log"
	mongowriter "github.com/dataphos/persistor/internal/writer/mongo"
)

func ErrorEmptyString(fieldName string) string {
	return fmt.Sprintf("%s must not be empty.", fieldName)
}

func (persistorConfig *PersistorConfig) Validate() []string {
	var errorList []string

	persistorConfig.Reader.Validate(&errorList)
	ValidateStorage(&persistorConfig.Storage, &errorList)

	if persistorConfig.IndexerEnabled {
		persistorConfig.Sender.Validate(&errorList)

		if persistorConfig.Storage.TopicID == persistorConfig.Sender.TopicID {
			errorList = append(errorList, "Source and indexer topic id cannot be the same")
		}

		if persistorConfig.Sender.TopicID == "" {
			errorList = append(errorList, "Indexer is enabled but SENDER_TOPICID is missing")
		}
	}

	if persistorConfig.DeadLetterEnabled {
		if persistorConfig.Sender.DeadLetterTopic == "" {
			errorList = append(errorList, "Dead letter topic is enabled but SENDER_DEADLETTERTOPIC is missing")
		} else if persistorConfig.IndexerEnabled && persistorConfig.Sender.TopicID == persistorConfig.Sender.DeadLetterTopic {
			errorList = append(errorList, "Dead letter and indexer topic id cannot be the same")
		}
	} else {
		if persistorConfig.Reader.Type == TypeKafka {
			errorList = append(errorList, "Dead letter must exist if kafka is used")
		} else if persistorConfig.Sender.DeadLetterTopic != "" {
			log.Warn("Dead lettering is not enabled. Ignoring dead letter topic ID.")

			persistorConfig.Sender.DeadLetterTopic = ""
		}
	}

	if !persistorConfig.IndexerEnabled && persistorConfig.Sender.TopicID != "" {
		log.Warn("Indexer topic is not enabled. Ignoring Indexer topic ID.")

		persistorConfig.Sender.TopicID = ""
	}

	ValidateBatchSettings(persistorConfig.BatchSettings, &errorList)

	return errorList
}

func (indexerConfig *IndexerConfig) Validate() []string {
	var errorList []string

	indexerConfig.Reader.Validate(&errorList)
	ValidateMongo(&indexerConfig.Mongo, &errorList)

	if indexerConfig.Sender.TopicID != "" {
		log.Warn("Ignoring Sender.TopicID in Indexer config")

		indexerConfig.Sender.TopicID = ""
	}

	if indexerConfig.Reader.Type == TypeKafka && !indexerConfig.DeadLetterEnabled {
		errorList = append(errorList, "Dead letter must exist if kafka is used")
	} else if !indexerConfig.DeadLetterEnabled && indexerConfig.Sender.DeadLetterTopic != "" {
		log.Warn("Dead lettering is not enabled. Ignoring dead letter topic ID.")

		indexerConfig.Sender.DeadLetterTopic = ""
	}

	if indexerConfig.DeadLetterEnabled && indexerConfig.Sender.DeadLetterTopic == "" {
		errorList = append(errorList, "Dead letter topic is enabled but SENDER_DEADLETTERTOPIC is missing")
	}

	ValidateBatchSettings(indexerConfig.BatchSettings, &errorList)

	return errorList
}

func (c *ReaderConfig) Validate(errorList *[]string) {
	switch c.Type {
	case TypeKafka:
		c.Kafka.Validate(errorList)
	case TypePubSub:
		c.PubSub.Validate(errorList)
	case TypeServiceBus:
		c.ServiceBus.Validate(errorList)
	case "":
		*errorList = append(*errorList, ErrorEmptyString("BrokerType"))
	default:
		*errorList = append(*errorList, fmt.Sprintf("Reader type %s is not recognized", c.Type))
	}
}

func (pubSubRecConfig *PubSubReceiverConfig) Validate(errorList *[]string) {
	if pubSubRecConfig.SubID == "" {
		*errorList = append(*errorList, ErrorEmptyString("SubID"))
	}

	if pubSubRecConfig.ProjectID == "" {
		*errorList = append(*errorList, ErrorEmptyString("ProjectID"))
	}
}

func (sbRecConfig *ServiceBusReceiverConfig) Validate(errorList *[]string) {
	if sbRecConfig.ConnectionString == "" {
		*errorList = append(*errorList, ErrorEmptyString("ConnectionString"))
	}

	if sbRecConfig.TopicID == "" {
		*errorList = append(*errorList, ErrorEmptyString("TopicID"))
	}

	if sbRecConfig.SubID == "" {
		*errorList = append(*errorList, ErrorEmptyString("SubID"))
	}
}

func (kafkaRecConfig *KafkaIteratorConfig) Validate(errorList *[]string) {
	if kafkaRecConfig.Address == "" {
		*errorList = append(*errorList, ErrorEmptyString("Address"))
	}

	if kafkaRecConfig.TopicID == "" {
		*errorList = append(*errorList, ErrorEmptyString("TopicID"))
	}
}

func ValidateBatchSettings(settings BatchSettings, errorList *[]string) {
	if settings.BatchSize < 1 {
		*errorList = append(*errorList, "Batch size must be 1 or greater")
	}

	if settings.BatchTimeout < 1 {
		*errorList = append(*errorList, "Batch timeout must be positive")
	}

	if settings.BatchMemory < 1 {
		*errorList = append(*errorList, "Batch memory must be 1 or greater")
	}
}

func (senderConfig *SenderConfig) Validate(errorList *[]string) {
	switch senderConfig.Type {
	case TypeKafka:
		senderConfig.Kafka.Validate(errorList)
	case TypePubSub:
		senderConfig.PubSub.Validate(errorList)
	case TypeServiceBus:
		senderConfig.ServiceBus.Validate(errorList)
	case "":
		*errorList = append(*errorList, ErrorEmptyString("Type"))
	default:
		*errorList = append(*errorList, fmt.Sprintf("Sender type %s is not recognized", senderConfig.Type))
	}
}

func (pubSubSenderConfig *PubSubSenderConfig) Validate(errorList *[]string) {
	if pubSubSenderConfig.ProjectID == "" {
		*errorList = append(*errorList, ErrorEmptyString("Sender.ProjectID"))
	}
}

func (sbSenderConfig *ServiceBusSenderConfig) Validate(errorList *[]string) {
	if sbSenderConfig.ConnectionString == "" {
		*errorList = append(*errorList, ErrorEmptyString("Sender.ConnectionString"))
	}
}

func (kafkaSenderConfig *KafkaSenderConfig) Validate(errorList *[]string) {
	if kafkaSenderConfig.Address == "" {
		*errorList = append(*errorList, ErrorEmptyString("Sender.Address"))
	}
}

func ValidateMongo(mongoConfig *mongowriter.MongoConfig, errorList *[]string) {
	if mongoConfig.ConnectionString == "" {
		*errorList = append(*errorList, ErrorEmptyString("Mongo.ConnectionString"))
	}

	if mongoConfig.Database == "" {
		*errorList = append(*errorList, ErrorEmptyString("Mongo.Database"))
	}

	if mongoConfig.Collection == "" {
		*errorList = append(*errorList, ErrorEmptyString("Mongo.Collection"))
	}

	if mongoConfig.Password != "" && mongoConfig.Username == "" {
		*errorList = append(*errorList, "set password without username")
	}

	if (mongoConfig.SessionTokenName != "" && mongoConfig.SessionTokenVal == "") ||
		(mongoConfig.SessionTokenName == "" && mongoConfig.SessionTokenVal != "") {
		*errorList = append(*errorList, "attempted to set session token, but missing either the token name or token value")
	}

	if err := mongoConfig.Validate(); err != nil {
		*errorList = append(*errorList, err.Error())
	}
}
