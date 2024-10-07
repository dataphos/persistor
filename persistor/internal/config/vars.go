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

// for setting environment variables
// string value must follow the field structure in config for fig package to recognize it
// values set from environment will override the corresponding fields from the toml file
const (
	ReaderTypeKey                    = "READER_TYPE"
	SenderTypeKey                    = "SENDER_TYPE"
	PubSubProjectID                  = "READER_PUBSUB_PROJECTID"
	PubSubSubID                      = "READER_PUBSUB_SUBID"
	ServiceBusReaderConnectionString = "READER_SERVICEBUS_CONNECTIONSTRING"
	ServiceBusReceiverTopicID        = "READER_SERVICEBUS_TOPICID"
	ServiceBusReceiverSubID          = "READER_SERVICEBUS_SUBID"
	KafkaReceiverAddress             = "READER_KAFKA_ADDRESS"
	KafkaReceiverTopic               = "READER_KAFKA_TOPICID"
	KafkaReceiverGroup               = "READER_KAFKA_GROUPID"

	BatchSize    = "BATCHSETTINGS_BATCHSIZE"
	BatchTimeout = "BATCHSETTINGS_BATCHTIMEOUT"
	BatchMemory  = "BATCHSETTINGS_BATCHMEMORY"
)

const (
	StorageType      = "STORAGE_TYPE"
	Prefix           = "STORAGE_PREFIX"
	Extension        = "STORAGE_EXTENSION"
	Mask             = "STORAGE_MASK"
	CustomValues     = "STORAGE_CUSTOMVALUES"
	WriteTimeout     = "STORAGE_WRITETIMEOUT"
	Destination      = "STORAGE_DESTINATION"
	StorageAccountID = "STORAGE_STORAGEACCOUNTID"
	StorageTopicID   = "STORAGE_STORAGETOPICID"
)

const (
	IndexerEnabled    = "INDEXERENABLED"
	DeadLetterEnabled = "DEADLETTERENABLED"

	SenderTopic           = "SENDER_TOPICID"
	SenderDeadLetterTopic = "SENDER_DEADLETTERTOPIC"

	PubSubSenderProjectID            = "SENDER_PUBSUB_PROJECTID"
	ServiceBusSenderConnectionString = "SENDER_SERVICEBUS_CONNECTIONSTRING"
	KafkaSenderAddress               = "SENDER_KAFKA_ADDRESS"
)

const (
	MongoConnectionString = "MONGO_CONNECTIONSTRING"
	MongoDatabase         = "MONGO_DATABASE"
	MongoCollection       = "MONGO_COLLECTION"
	MongoAuthMechanism    = "MONGO_AUTHMECHANISM"
	MongoAuthSource       = "MONGO_AUTHSOURCE"
	MongoUsername         = "MONGO_USERNAME"
	MongoPassword         = "MONGO_PASSWORD"
	MongoSessionTokenName = "MONGO_SESSIONTOKENNAME"
	MongoSessionTokenVal  = "MONGO_SESSIONTOKENVAL"
)
