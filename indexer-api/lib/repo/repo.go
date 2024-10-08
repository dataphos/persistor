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

// Package repo contains the caching, querying and mongo handling logic.
package repo

import (
	"context"
	"time"
)

type Message struct {
	BrokerID             string            `bson:"broker_id,omitempty" json:"broker_id,omitempty"`
	BrokerMessageID      string            `bson:"broker_msg_id,omitempty" json:"broker_msg_id,omitempty"`
	BusinessSourceKey    string            `bson:"business_source_key,omitempty" json:"business_source_key,omitempty"`
	BusinessObjectKey    string            `bson:"business_object_key,omitempty" json:"business_object_key,omitempty"`
	UniqueID             string            `bson:"unique_id,omitempty" json:"unique_id,omitempty"`
	IndexSourceKey       string            `bson:"index_source_key,omitempty" json:"index_source_key,omitempty"`
	OrderingKey          string            `bson:"ordering_key,omitempty" json:"ordering_key,omitempty"`
	LocationKey          string            `bson:"location_key,omitempty" json:"location_key,omitempty"`
	LocationPosition     *int              `bson:"location_position,omitempty" json:"location_position,omitempty"`
	PublishTime          *time.Time        `bson:"publish_time,omitempty" json:"publish_time,omitempty"`
	IngestionTime        *time.Time        `bson:"ingestion_time,omitempty" json:"ingestion_time,omitempty"`
	IndexerIngestionTime *time.Time        `bson:"indexer_ingestion_time,omitempty" json:"indexer_ingestion_time,omitempty"`
	AdditionalMetadata   map[string]string `bson:"additional_metadata,omitempty" json:"additional_metadata,omitempty"`
}

type QueryParameters struct {
	BrokerID             string `form:"broker_id"`
	BrokerMessageID      string `form:"broker_msg_id"`
	BusinessSourceKey    string `form:"business_source_key"`
	BusinessObjectKey    string `form:"business_object_key"`
	UniqueID             string `form:"unique_id"`
	IndexSourceKey       string `form:"index_source_key"`
	LocationKey          string `form:"location_key"`
	LocationPosition     *int   `form:"location_position"`
	OrderingKey          string `form:"ordering_key"`
	PublishTime          string `form:"publish_time"`
	IngestionTime        string `form:"ingestion_time"`
	IndexerIngestionTime string `form:"indexer_ingestion_time"`
	AdditionalMetadata   string `form:"additional_metadata"`
}

type QueryInformation struct {
	MongoCollection string
	Filters         []map[string]interface{}
	Limit           int
	Offset          int
	AttributesList  []string
}

type Repository interface {
	Get(context.Context, string, string, []string) ([]Message, error)
	GetAll(context.Context, string, []string, []string) ([]Message, error)
	GetAllInInterval(context.Context, string, time.Time, time.Time, string, int, int, []string) ([]Message, error)
	GetAllInIntervalDocumentCount(context.Context, string, time.Time, time.Time, string) (int64, error)
	GetQueried(context.Context, QueryInformation) ([]Message, error)
	GetQueriedDocumentCount(context.Context, string, []map[string]interface{}) (int64, error)
}
