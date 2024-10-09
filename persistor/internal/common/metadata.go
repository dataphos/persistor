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

package common

import "time"

// CommonData information.
type CommonData struct {
	BrokerID           string            `json:"broker_id"  bson:"broker_id"  validate:"required"`
	BrokerMsgID        string            `json:"broker_msg_id" bson:"broker_msg_id"  validate:"required"`
	BusinessSourceKey  string            `json:"business_source_key,omitempty" bson:"business_source_key,omitempty"`
	BusinessObjectKey  string            `json:"business_object_key,omitempty" bson:"business_object_key,omitempty"`
	UniqueID           string            `json:"unique_id" bson:"unique_id" validate:"required"`
	IndexSourceKey     string            `json:"index_source_key,omitempty" bson:"index_source_key,omitempty"`
	OrderingKey        string            `json:"ordering_key,omitempty" bson:"ordering_key,omitempty"`
	AdditionalMetadata map[string]string `json:"additional_metadata" bson:"additional_metadata"`
}

// Location - Indexer.
type Location struct {
	LocationKey      string `json:"location_key" bson:"location_key" validate:"required"`
	LocationPosition int    `json:"location_position,omitempty" bson:"location_position,omitempty"`
}

// StringTimestamps - Timestamps (string).
type StringTimestamps struct {
	PublishTime   string `json:"publish_time" bson:"publish_time" validate:"omitempty,datetime=2006-01-02 15:04:05.99999999"`
	IngestionTime string `json:"ingestion_time" bson:"ingestion_time" validate:"required,datetime=2006-01-02 15:04:05.999999999"`
}

// ProperTimestamps - Timestamps (as timestamps).
type ProperTimestamps struct {
	PublishTime          time.Time `json:"publish_time" bson:"publish_time"`
	IngestionTime        time.Time `json:"ingestion_time" bson:"ingestion_time"`
	IndexerIngestionTime time.Time `bson:"indexer_ingestion_time"`
}

type DefinedData struct {
	ConfirmationFlag bool `bson:"confirmation_flag"`
}

type Data struct {
	CommonData
	Location  Location         `json:"location"`
	Timestamp StringTimestamps `json:"timestamp"`
}
