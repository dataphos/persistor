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

package repo

import (
	"context"
	"time"
)

type Mock struct{}

func (Mock) Get(_ context.Context, _, _ string, _ []string) ([]Message, error) {
	return []Message{
		{
			BrokerID:             "test-topic",
			BrokerMessageID:      "123456789",
			BusinessSourceKey:    "def",
			BusinessObjectKey:    "abc",
			UniqueID:             "test-topic_123456789",
			IndexSourceKey:       "def_abc",
			LocationKey:          "f",
			LocationPosition:     nil,
			PublishTime:          &time.Time{},
			IngestionTime:        &time.Time{},
			IndexerIngestionTime: &time.Time{},
			AdditionalMetadata:   map[string]string{"key1": "value1", "key2": "value2"},
		},
	}, nil
}

func (Mock) GetAll(_ context.Context, _ string, _ []string, _ []string) ([]Message, error) {
	locationPosition := 1
	return []Message{
		{
			BrokerID:             "test-topic",
			BrokerMessageID:      "123456789",
			BusinessSourceKey:    "def",
			BusinessObjectKey:    "abc",
			UniqueID:             "test-topic_123456789",
			IndexSourceKey:       "def_abc",
			LocationKey:          "f1",
			LocationPosition:     nil,
			PublishTime:          &time.Time{},
			IngestionTime:        &time.Time{},
			IndexerIngestionTime: &time.Time{},
			AdditionalMetadata:   map[string]string{"key1": "value1", "key2": "value2"},
		},
		{
			BrokerID:             "test-topic",
			BrokerMessageID:      "234567891",
			BusinessSourceKey:    "def",
			BusinessObjectKey:    "abc",
			UniqueID:             "test-topic_234567891",
			IndexSourceKey:       "def_abc",
			LocationKey:          "f2",
			LocationPosition:     &locationPosition,
			PublishTime:          &time.Time{},
			IngestionTime:        &time.Time{},
			IndexerIngestionTime: &time.Time{},
			AdditionalMetadata:   map[string]string{"key1": "value1", "key2": "value2"},
		},
	}, nil
}

func (Mock) GetAllInInterval(_ context.Context, _ string, _, _ time.Time, _ string, _, _ int, _ []string) ([]Message, error) {
	locationPosition := 1
	return []Message{
		{
			BrokerID:             "test-topic",
			BrokerMessageID:      "123456789",
			BusinessSourceKey:    "def",
			BusinessObjectKey:    "abc",
			UniqueID:             "test-topic_123456789",
			IndexSourceKey:       "def_abc",
			LocationKey:          "f1",
			LocationPosition:     nil,
			PublishTime:          &time.Time{},
			IngestionTime:        &time.Time{},
			IndexerIngestionTime: &time.Time{},
			AdditionalMetadata:   map[string]string{"key1": "value1", "key2": "value2"},
		},
		{
			BrokerID:             "test-topic",
			BrokerMessageID:      "234567891",
			BusinessSourceKey:    "def",
			BusinessObjectKey:    "abc",
			UniqueID:             "test-topic_234567891",
			IndexSourceKey:       "def_abc",
			LocationKey:          "f2",
			LocationPosition:     &locationPosition,
			PublishTime:          &time.Time{},
			IngestionTime:        &time.Time{},
			IndexerIngestionTime: &time.Time{},
			AdditionalMetadata:   map[string]string{"key1": "value1", "key2": "value2"},
		},
	}, nil
}

func (Mock) GetAllInIntervalDocumentCount(_ context.Context, _ string, _, _ time.Time, _ string) (int64, error) {
	return 1, nil
}

func (Mock) GetQueried(_ context.Context, _ QueryInformation) ([]Message, error) {
	locationPosition := 1
	return []Message{
		{
			BrokerID:             "test-topic",
			BrokerMessageID:      "123456789",
			BusinessSourceKey:    "def",
			BusinessObjectKey:    "abc",
			UniqueID:             "test-topic_123456789",
			IndexSourceKey:       "def_abc",
			LocationKey:          "f1",
			LocationPosition:     nil,
			PublishTime:          &time.Time{},
			IngestionTime:        &time.Time{},
			IndexerIngestionTime: &time.Time{},
			AdditionalMetadata:   map[string]string{"key1": "value1", "key2": "value2"},
		},
		{
			BrokerID:             "test-topic",
			BrokerMessageID:      "234567891",
			BusinessSourceKey:    "def",
			BusinessObjectKey:    "abc",
			UniqueID:             "test-topic_234567891",
			IndexSourceKey:       "def_abc",
			LocationKey:          "f2",
			LocationPosition:     &locationPosition,
			PublishTime:          &time.Time{},
			IngestionTime:        &time.Time{},
			IndexerIngestionTime: &time.Time{},
			AdditionalMetadata:   map[string]string{"key1": "value1", "key2": "value2"},
		},
	}, nil
}

func (Mock) GetQueriedDocumentCount(_ context.Context, _ string, _ []map[string]interface{}) (int64, error) {
	return 1, nil
}
