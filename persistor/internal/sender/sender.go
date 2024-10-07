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

// Package sender contains code for the Sender component of Persistor.
package sender

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/pkg/errors"

	"github.com/dataphos/lib-batchproc/pkg/batchproc"
	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-streamproc/pkg/streamproc"
	"github.com/dataphos/persistor/internal/common"
)

const (
	TimestampLayout = "2006-01-02 15:04:05.99999999"
)

const (
	IndexExistingMetadata = "existing_metadata"
	IndexLocationKey      = "location_key"
	IndexLocationPosition = "location_position"
	IndexLocationBrokerID = "broker_id"

	BusinessSourceKey = "business_source_key"
	BusinessObjectKey = "business_object_key"
)

const (
	DeadLetterReasonCategory = "deadLetterErrorCategory"
	DeadLetterReason         = "deadLetterErrorReason"
	DeadLetterSource         = "deadLetterSource"

	DeadLetterSourcePersistor = "Dataphos Persistor - Core"
	DeadLetterSourceIndexer   = "Dataphos Persistor - Indexer"
)

func SendToDeadLetter(ctx context.Context, errorCategory, errorSource, errorInfo string, dlTopic broker.Topic, msgs ...streamproc.Message) error {
	outbound := make([]broker.OutboundMessage, 0, len(msgs))

	for _, msg := range msgs {
		attrs := make(map[string]interface{}, len(msg.Attributes))

		if msg.Attributes != nil {
			// copying the map is necessary because otherwise we have concurrent reads and writes.
			for k, v := range msg.Attributes {
				attrs[k] = v
			}
		}

		attrs[DeadLetterReason] = errorInfo
		attrs[DeadLetterReasonCategory] = errorCategory
		attrs[DeadLetterSource] = errorSource

		outbound = append(outbound, broker.OutboundMessage{
			Key:        msg.Key,
			Data:       msg.Data,
			Attributes: attrs,
		})
	}

	err := dlTopic.BatchPublish(ctx, outbound...)
	if err != nil {
		return errors.Wrap(err, "dead letter publisher")
	}

	return nil
}

func SendToIndexerTopic(ctx context.Context, idxTopic broker.Topic, messages []streamproc.Message, location, sourceTopicID string) *common.ProcError {
	outbound := make([]broker.OutboundMessage, len(messages))

	_ = batchproc.Parallel(ctx, len(messages), func(_ context.Context, start, end int) error {
		for iMsg := start; iMsg < end; iMsg++ {
			msg := messages[iMsg]
			msgWithMetadata := SetMetadata(msg, location, sourceTopicID, iMsg)

			indexData, err := ParseMessage(msgWithMetadata)
			if err != nil {
				return errors.Wrap(err, "extracting metadata")
			}

			indexSend, err := json.Marshal(indexData)
			if err != nil {
				return errors.Wrap(err, "reading json object from data")
			}

			outbound[iMsg] = broker.OutboundMessage{
				Data: indexSend,
			}
		}

		return nil
	})
	err := idxTopic.BatchPublish(ctx, outbound...)

	if err != nil {
		return common.NewProcessingError(len(messages), err, common.SenderPublishError)
	}

	return nil
}

func SetMetadata(msg streamproc.Message, location string, topic string, position int) streamproc.Message {
	if msg.Attributes == nil {
		msg.Attributes = map[string]interface{}{}
	}
	// put the user-defined attributes in one place; this changes the msg.Attributes
	// reference and hopefully doesn't break anything on the broker lib side.
	msg.Attributes = map[string]interface{}{IndexExistingMetadata: msg.Attributes}
	msg.Attributes[IndexLocationKey] = location
	msg.Attributes[IndexLocationPosition] = fmt.Sprintf("%d", position)
	msg.Attributes[IndexLocationBrokerID] = topic

	return msg
}

// ParseMessage implementation of the Parser interface.
func ParseMessage(msg streamproc.Message) (interface{}, error) {
	var (
		err   error
		index common.Data
	)

	// get user-defined attributes.
	existingMetadataInterface, asserted := msg.Attributes["existing_metadata"].(map[string]interface{})
	if !asserted {
		return nil, fmt.Errorf("cannot parse metadata") //nolint:goerr113 //unnecessary here.
	}

	existingMetadata := onlyStrings(existingMetadataInterface)

	// Business logic-related information. -- optional (will be empty strings if not set).
	index.BusinessSourceKey = existingMetadata[BusinessSourceKey]
	index.BusinessObjectKey = existingMetadata[BusinessObjectKey]

	// delete important keys from existing metadata - they are now members of the record itself.
	delete(existingMetadata, BusinessSourceKey)
	delete(existingMetadata, BusinessObjectKey)

	if len(index.BusinessSourceKey) > 0 && len(index.BusinessObjectKey) > 0 {
		index.IndexSourceKey = fmt.Sprintf("%s_%s", index.BusinessSourceKey, index.BusinessObjectKey)
	}

	index.AdditionalMetadata = existingMetadata
	generatedMetadata := onlyStrings(msg.Attributes) // get the generated metadata.

	// Broker-related data. -- required.
	brokerID, metadataExists := generatedMetadata["broker_id"]
	if !metadataExists {
		return nil, invalidIndex("broker_id")
	}

	index.BrokerID = brokerID
	index.BrokerMsgID = msg.ID
	index.UniqueID = fmt.Sprintf("%s_%s", brokerID, msg.ID)

	index.OrderingKey = generatedMetadata["ordering_key"]

	// Location of the message in relation to storage.
	// Location key -- required.
	locationKey, metadataExists := generatedMetadata["location_key"]
	if !metadataExists {
		return nil, invalidIndex("location_key")
	}

	index.Location.LocationKey = locationKey
	// Location position -- optional.
	locationPosition, metadataExists := generatedMetadata["location_position"]
	if metadataExists {
		convertedPos, errConv := strconv.Atoi(locationPosition)
		if errConv == nil {
			index.Location.LocationPosition = convertedPos + 1
		}
	}

	// Timestamp-related information.
	// Publish time -- optional.
	index.Timestamp.PublishTime = msg.PublishTime.UTC().Format(TimestampLayout)
	// Ingestion time -- required.
	index.Timestamp.IngestionTime = msg.IngestionTime.UTC().Format(TimestampLayout)

	return index, err
}

var ErrMissingIndexingField = errors.New("missing required indexing field in the message metadata")

// InvalidIndex returns a formatted error message when a required field is missing from the message metadata.
func invalidIndex(missingField string) error {
	return fmt.Errorf("%w: %s", ErrMissingIndexingField, missingField)
}

// onlyStrings returns a map of pairs from the original map whose values are strings and ignores the rest.
// temporary until we decide on a nicer solution.
func onlyStrings(metadata map[string]interface{}) map[string]string {
	result := make(map[string]string)

	for k, v := range metadata {
		s, ok := v.(string)
		if ok {
			result[k] = s
		}
	}

	return result
}
