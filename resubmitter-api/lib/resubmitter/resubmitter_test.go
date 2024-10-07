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

package resubmitter

import (
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/dataphos/persistor-resubmitter-api/common/util"
	"github.com/dataphos/persistor-resubmitter-api/lib/fetcher"
	"github.com/dataphos/persistor-resubmitter-api/lib/fetcher/abs"
	"github.com/dataphos/persistor-resubmitter-api/lib/fetcher/gs"
	"github.com/dataphos/persistor-resubmitter-api/lib/indexer"
	"github.com/dataphos/persistor-resubmitter-api/lib/persistor"
	"github.com/dataphos/persistor-resubmitter-api/lib/publisher"
	"github.com/dataphos/persistor-resubmitter-api/lib/publisher/pubsub"
	"github.com/dataphos/persistor-resubmitter-api/lib/serializer"
	"github.com/dataphos/persistor-resubmitter-api/lib/serializer/avro"
)

const (
	topicId          = "some-topic-id"
	e2eResubmitTopic = "resubmit-e2e-topic"
	mongoCollection  = "some-mongo-collection"
)

var query = util.QueryRequestBody{
	Filters: []map[string]interface{}{
		{
			"some-key":  "some-value",
			"other-key": "other-value",
		},
	},
}

func TestResubmit_Resubmit(t *testing.T) {
	records, indexerMessages, fetcherData := setExpectedResubmit()

	indexerMock := &indexer.Mock{}
	indexerMock.SetGetAllResponse(indexerMessages)

	fetcherMock := &fetcher.Mock{}
	fetcherMock.SetData(fetcherData)

	publisherMock := &publisher.MockPublisher{}

	resubmitter := New(
		indexerMock,
		fetcherMock,
		avro.New(),
		publisherMock,
	)

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.Resubmit(topicId, mongoCollection, []string{"some-unique-id", "another-unique-id", "the-best-id"})
	if len(result.IndexerErrors) != 0 || len(result.PipelineErrors) != 0 {
		t.Fatal("errors occurred")
	}

	var published []persistor.Record
	spawnedTopics := publisherMock.MockTopics
	for i := range spawnedTopics {
		published = append(published, spawnedTopics[i].PublishedRecords...)
	}
	if len(published) != len(records) {
		t.Fatalf("number of published records (%d) is different than expected (%d)", len(published), len(records))
	}
	for _, publishedRecord := range published {
		if !checkIfExists(publishedRecord, records) {
			t.Fatal("published record is not in the expected set")
		}
	}
}

func TestResubmit_ResubmitInterval(t *testing.T) {
	records, indexerMessages, batchIntervals, capacity, fetcherData := setExpectedResubmitInterval()

	indexerMock := &indexer.Mock{}
	indexerMock.SetGetAllInIntervalResponse(indexerMessages)

	fetcherMock := &fetcher.Mock{}
	fetcherMock.SetData(fetcherData)

	publisherMock := &publisher.MockPublisher{}

	resubmitter := New(
		indexerMock,
		fetcherMock,
		avro.New(),
		publisherMock,
		WithMetadataCapacity(capacity),
	)

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.ResubmitInterval(topicId, mongoCollection, "some-broker-id", time.Time{}, time.Now())
	if len(result.IndexerErrors) != 0 || len(result.PipelineErrors) != 0 {
		t.Fatal("errors occurred")
	}

	var published []persistor.Record
	topics := publisherMock.MockTopics
	for i := range topics {
		published = append(published, topics[i].PublishedRecords...)
	}
	if len(published) != len(records) {
		t.Fatalf("number of published records (%d) is different than expected (%d)", len(published), len(records))
	}
	for _, publishedRecord := range published {
		if !checkIfExists(publishedRecord, records) {
			t.Fatal("published record is not in the expected set")
		}
	}

	if !reflect.DeepEqual(batchIntervals, indexerMock.BatchIntervals) {
		t.Fatal("incorrect batching")
	}
}

func TestResubmit_ResubmitQuery(t *testing.T) {
	records, indexerMessages, batchIntervals, capacity, fetcherData := setExpectedResubmitQuery()

	indexerMock := &indexer.Mock{}
	indexerMock.SetGetQueriedResponse(indexerMessages)

	fetcherMock := &fetcher.Mock{}
	fetcherMock.SetData(fetcherData)

	publisherMock := &publisher.MockPublisher{}

	resubmitter := New(
		indexerMock,
		fetcherMock,
		avro.New(),
		publisherMock,
		WithMetadataCapacity(capacity),
	)

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}

	result := resubmitterJob.ResubmitQuery(topicId, mongoCollection, query)
	if len(result.IndexerErrors) != 0 || len(result.PipelineErrors) != 0 {
		t.Fatal("errors occurred")
	}

	var published []persistor.Record
	topics := publisherMock.MockTopics
	for i := range topics {
		published = append(published, topics[i].PublishedRecords...)
	}
	if len(published) != len(records) {
		t.Fatalf("number of published records (%d) is different than expected (%d)", len(published), len(records))
	}
	for _, publishedRecord := range published {
		if !checkIfExists(publishedRecord, records) {
			t.Fatal("published record is not in the expected set")
		}
	}

	if !reflect.DeepEqual(batchIntervals, indexerMock.BatchIntervals) {
		t.Fatal("incorrect batching")
	}
}

func setExpectedResubmit() ([]persistor.Record, []indexer.Message, map[string][]byte) {
	records := []persistor.Record{
		{
			ID:   "test-topic_123456789",
			Data: []byte("data1"),
			Metadata: map[string]string{
				"key11":                     "value11",
				"key12":                     "value12",
				persistor.BusinessObjectKey: "abc",
				persistor.BusinessSourceKey: "def",
				orderingKey:                 "ordering_key",
			},
		},
		{
			ID:   "test-topic_234567891",
			Data: []byte("data2"),
			Metadata: map[string]string{
				"key21":                     "value21",
				"key22":                     "value22",
				persistor.BusinessObjectKey: "abc",
				persistor.BusinessSourceKey: "def",
				orderingKey:                 "ordering_key",
			},
		},
		{
			ID:   "test-topic_345678912",
			Data: []byte("data3"),
			Metadata: map[string]string{
				"key31":                     "value31",
				"key32":                     "value32",
				persistor.BusinessObjectKey: "abc",
				persistor.BusinessSourceKey: "def",
				orderingKey:                 "ordering_key",
			},
		},
		{
			ID:   "test-topic_456789123",
			Data: []byte("data4"),
			Metadata: map[string]string{
				"key41":                     "value41",
				"key42":                     "value42",
				persistor.BusinessObjectKey: "abc",
				persistor.BusinessSourceKey: "def",
				orderingKey:                 "ordering_key",
			},
		},
	}

	file1Name := "f1"
	file2Name := "f2"

	locationPosition1 := 1
	locationPosition2 := 2
	locationPosition3 := 3
	indexerMessages := []indexer.Message{
		{
			BrokerID:           "test-topic",
			BrokerMessageID:    "123456789",
			BusinessSourceKey:  "def",
			BusinessObjectKey:  "abc",
			UniqueID:           "test-topic_123456789",
			IndexSourceKey:     "def_abc",
			OrderingKey:        "ordering_key",
			LocationKey:        file1Name,
			LocationPosition:   nil,
			AdditionalMetadata: map[string]string{"key11": "value11", "key12": "value12"},
		},
		{
			BrokerID:           "test-topic",
			BrokerMessageID:    "234567891",
			BusinessSourceKey:  "def",
			BusinessObjectKey:  "abc",
			UniqueID:           "test-topic_234567891",
			IndexSourceKey:     "def_abc",
			OrderingKey:        "ordering_key",
			LocationKey:        file2Name,
			LocationPosition:   &locationPosition1,
			AdditionalMetadata: map[string]string{"key21": "value21", "key22": "value22"},
		},
		{
			BrokerID:           "test-topic",
			BrokerMessageID:    "345678912",
			BusinessSourceKey:  "def",
			BusinessObjectKey:  "abc",
			UniqueID:           "test-topic_345678912",
			IndexSourceKey:     "def_abc",
			OrderingKey:        "ordering_key",
			LocationKey:        file2Name,
			LocationPosition:   &locationPosition2,
			AdditionalMetadata: map[string]string{"key31": "value31", "key32": "value32"},
		},
		{
			BrokerID:           "test-topic",
			BrokerMessageID:    "456789123",
			BusinessSourceKey:  "def",
			BusinessObjectKey:  "abc",
			UniqueID:           "test-topic_456789123",
			OrderingKey:        "ordering_key",
			LocationKey:        file2Name,
			LocationPosition:   &locationPosition3,
			AdditionalMetadata: map[string]string{"key41": "value41", "key42": "value42"},
		},
	}
	file1Records := records[0].Data

	var file2Records []persistor.Record
	file2Records = append(file2Records, records[1:]...)
	additionalFile2Records := []persistor.Record{
		{
			ID:   "test-topic_567891234",
			Data: []byte("data5"),
			Metadata: map[string]string{
				"key51":                     "value51",
				"key52":                     "value52",
				persistor.BusinessObjectKey: "abc",
				persistor.BusinessSourceKey: "def",
				orderingKey:                 "ordering_key",
			},
		},
		{
			ID:   "test-topic_678912345",
			Data: []byte("data6"),
			Metadata: map[string]string{
				"key61":                     "value61",
				"key62":                     "value62",
				persistor.BusinessObjectKey: "abc",
				persistor.BusinessSourceKey: "def",
				orderingKey:                 "ordering_key",
			},
		},
	}
	file2Records = append(file2Records, additionalFile2Records...)

	file2, err := avro.New().Serialize(file2Records)
	if err != nil {
		panic(err)
	}

	fetcherData := map[string][]byte{
		file1Name: file1Records,
		file2Name: file2,
	}

	return records, indexerMessages, fetcherData
}

func setExpectedResubmitInterval() ([]persistor.Record, []indexer.Message, []indexer.BatchInterval, int, map[string][]byte) {
	records := []persistor.Record{
		{
			ID:   "test-other_123456789",
			Data: []byte("d1"),
			Metadata: map[string]string{
				"key11":                     "v11",
				"key12":                     "v12",
				persistor.BusinessObjectKey: "lm",
				persistor.BusinessSourceKey: "no",
				orderingKey:                 "ordering_key",
			},
		},
		{
			ID:   "test-other_234567891",
			Data: []byte("d2"),
			Metadata: map[string]string{
				"key21":                     "v21",
				"key22":                     "v22",
				persistor.BusinessObjectKey: "lm",
				persistor.BusinessSourceKey: "no",
				orderingKey:                 "ordering_key",
			},
		},
		{
			ID:   "test-other_345678912",
			Data: []byte("d3"),
			Metadata: map[string]string{
				"key31":                     "v31",
				"key32":                     "v32",
				persistor.BusinessObjectKey: "lm",
				persistor.BusinessSourceKey: "no",
				orderingKey:                 "ordering_key",
			},
		},
		{
			ID:   "test-other_456789123",
			Data: []byte("d4"),
			Metadata: map[string]string{
				"key41":                     "v41",
				"key42":                     "v42",
				persistor.BusinessObjectKey: "lm",
				persistor.BusinessSourceKey: "no",
				orderingKey:                 "ordering_key",
			},
		},
		{
			ID:   "test-other_567891234",
			Data: []byte("d5"),
			Metadata: map[string]string{
				"key51":                     "v51",
				"key52":                     "v52",
				persistor.BusinessObjectKey: "lm",
				persistor.BusinessSourceKey: "no",
				orderingKey:                 "ordering_key",
			},
		},
	}

	file1Name := "file1"
	file2Name := "file2"
	file3Name := "file3"

	file2locationPosition1 := 1
	file2locationPosition2 := 2
	file3locationPosition1 := 1
	file3locationPosition2 := 2

	indexerMessages := []indexer.Message{
		{
			BrokerID:           "test-other",
			BrokerMessageID:    "123456789",
			BusinessSourceKey:  "no",
			BusinessObjectKey:  "lm",
			UniqueID:           "test-other_123456789",
			IndexSourceKey:     "no_lm",
			OrderingKey:        "ordering_key",
			LocationKey:        file1Name,
			LocationPosition:   nil,
			AdditionalMetadata: map[string]string{"key11": "v11", "key12": "v12"},
		},
		{
			BrokerID:           "test-other",
			BrokerMessageID:    "234567891",
			BusinessSourceKey:  "no",
			BusinessObjectKey:  "lm",
			UniqueID:           "test-other_234567891",
			IndexSourceKey:     "no_lm",
			OrderingKey:        "ordering_key",
			LocationKey:        file2Name,
			LocationPosition:   &file2locationPosition1,
			AdditionalMetadata: map[string]string{"key21": "v21", "key22": "v22"},
		},
		{
			BrokerID:           "test-other",
			BrokerMessageID:    "345678912",
			BusinessSourceKey:  "no",
			BusinessObjectKey:  "lm",
			UniqueID:           "test-other_345678912",
			IndexSourceKey:     "no_lm",
			OrderingKey:        "ordering_key",
			LocationKey:        file2Name,
			LocationPosition:   &file2locationPosition2,
			AdditionalMetadata: map[string]string{"key31": "v31", "key32": "v32"},
		},
		{
			BrokerID:           "test-other",
			BrokerMessageID:    "456789123",
			BusinessSourceKey:  "no",
			BusinessObjectKey:  "lm",
			UniqueID:           "test-other_456789123",
			OrderingKey:        "ordering_key",
			LocationKey:        file3Name,
			LocationPosition:   &file3locationPosition1,
			AdditionalMetadata: map[string]string{"key41": "v41", "key42": "v42"},
		},
		{
			BrokerID:           "test-other",
			BrokerMessageID:    "567891234",
			BusinessSourceKey:  "no",
			BusinessObjectKey:  "lm",
			UniqueID:           "test-other_567891234",
			OrderingKey:        "ordering_key",
			LocationKey:        file3Name,
			LocationPosition:   &file3locationPosition2,
			AdditionalMetadata: map[string]string{"key51": "v51", "key52": "v52"},
		},
	}

	capacity := 2

	batchIntervals := []indexer.BatchInterval{
		{Start: 0, End: 2},
		{Start: 2, End: 4},
		{Start: 4, End: 5},
	}

	file1Records := records[0].Data

	var file2Records []persistor.Record
	file2Records = append(file2Records, records[1:3]...)
	additionalFile2Records := []persistor.Record{
		{
			ID:   "test-other_678912345",
			Data: []byte("d6"),
			Metadata: map[string]string{
				"key61":                     "v61",
				"key62":                     "v62",
				persistor.BusinessObjectKey: "no",
				persistor.BusinessSourceKey: "lm",
				orderingKey:                 "ordering_key",
			},
		},
		{
			ID:   "test-other_789123456",
			Data: []byte("d7"),
			Metadata: map[string]string{
				"key71":                     "v71",
				"key72":                     "v72",
				persistor.BusinessObjectKey: "no",
				persistor.BusinessSourceKey: "lm",
				orderingKey:                 "ordering_key",
			},
		},
	}
	file2Records = append(file2Records, additionalFile2Records...)

	file2, err := avro.New().Serialize(file2Records)
	if err != nil {
		panic(err)
	}

	var file3Records []persistor.Record
	file3Records = append(file3Records, records[3:]...)
	additionalFile3Records := []persistor.Record{
		{
			ID:   "test-other_891234567",
			Data: []byte("d8"),
			Metadata: map[string]string{
				"key81":                     "v81",
				"key82":                     "v82",
				persistor.BusinessObjectKey: "no",
				persistor.BusinessSourceKey: "lm",
				orderingKey:                 "ordering_key",
			},
		},
		{
			ID:   "test-other_912345678",
			Data: []byte("d9"),
			Metadata: map[string]string{
				"key91":                     "v91",
				"key92":                     "v92",
				persistor.BusinessObjectKey: "no",
				persistor.BusinessSourceKey: "lm",
				orderingKey:                 "ordering_key",
			},
		},
	}
	file3Records = append(file3Records, additionalFile3Records...)

	file3, err := avro.New().Serialize(file3Records)
	if err != nil {
		panic(err)
	}

	fetcherData := map[string][]byte{
		file1Name: file1Records,
		file2Name: file2,
		file3Name: file3,
	}

	return records, indexerMessages, batchIntervals, capacity, fetcherData
}

func setExpectedResubmitQuery() ([]persistor.Record, []indexer.Message, []indexer.BatchInterval, int, map[string][]byte) {
	records := []persistor.Record{
		{
			ID:   "test-query_123456789",
			Data: []byte("dq1"),
			Metadata: map[string]string{
				"keyq11":                    "vq11",
				"keyq12":                    "vq12",
				persistor.BusinessObjectKey: "lmq",
				persistor.BusinessSourceKey: "noq",
				orderingKey:                 "ordering_key",
			},
		},
		{
			ID:   "test-query_234567891",
			Data: []byte("dq2"),
			Metadata: map[string]string{
				"keyq21":                    "vq21",
				"keyq22":                    "vq22",
				persistor.BusinessObjectKey: "lmq",
				persistor.BusinessSourceKey: "noq",
				orderingKey:                 "ordering_key",
			},
		},
		{
			ID:   "test-query_345678912",
			Data: []byte("dq3"),
			Metadata: map[string]string{
				"keyq31":                    "vq31",
				"keyq32":                    "vq32",
				persistor.BusinessObjectKey: "lmq",
				persistor.BusinessSourceKey: "noq",
				orderingKey:                 "ordering_key",
			},
		},
		{
			ID:   "test-query_456789123",
			Data: []byte("dq4"),
			Metadata: map[string]string{
				"keyq41":                    "vq41",
				"keyq42":                    "vq42",
				persistor.BusinessObjectKey: "lmq",
				persistor.BusinessSourceKey: "noq",
				orderingKey:                 "ordering_key",
			},
		},
		{
			ID:   "test-query_567891234",
			Data: []byte("dq5"),
			Metadata: map[string]string{
				"keyq51":                    "vq51",
				"keyq52":                    "vq52",
				persistor.BusinessObjectKey: "lmq",
				persistor.BusinessSourceKey: "noq",
				orderingKey:                 "ordering_key",
			},
		},
	}

	file1Name := "fileq1"
	file2Name := "fileq2"
	file3Name := "fileq3"

	file2locationPosition1 := 1
	file2locationPosition2 := 2
	file3locationPosition1 := 1
	file3locationPosition2 := 2

	indexerMessages := []indexer.Message{
		{
			BrokerID:           "test-query",
			BrokerMessageID:    "123456789",
			BusinessSourceKey:  "noq",
			BusinessObjectKey:  "lmq",
			UniqueID:           "test-query_123456789",
			IndexSourceKey:     "noq_lmq",
			OrderingKey:        "ordering_key",
			LocationKey:        file1Name,
			LocationPosition:   nil,
			AdditionalMetadata: map[string]string{"keyq11": "vq11", "keyq12": "vq12"},
		},
		{
			BrokerID:           "test-query",
			BrokerMessageID:    "234567891",
			BusinessSourceKey:  "noq",
			BusinessObjectKey:  "lmq",
			UniqueID:           "test-query_234567891",
			IndexSourceKey:     "noq_lmq",
			OrderingKey:        "ordering_key",
			LocationKey:        file2Name,
			LocationPosition:   &file2locationPosition1,
			AdditionalMetadata: map[string]string{"keyq21": "vq21", "keyq22": "vq22"},
		},
		{
			BrokerID:           "test-query",
			BrokerMessageID:    "345678912",
			BusinessSourceKey:  "noq",
			BusinessObjectKey:  "lmq",
			UniqueID:           "test-query_345678912",
			IndexSourceKey:     "noq_lmq",
			OrderingKey:        "ordering_key",
			LocationKey:        file2Name,
			LocationPosition:   &file2locationPosition2,
			AdditionalMetadata: map[string]string{"keyq31": "vq31", "keyq32": "vq32"},
		},
		{
			BrokerID:           "test-query",
			BrokerMessageID:    "456789123",
			BusinessSourceKey:  "noq",
			BusinessObjectKey:  "lmq",
			UniqueID:           "test-query_456789123",
			OrderingKey:        "ordering_key",
			LocationKey:        file3Name,
			LocationPosition:   &file3locationPosition1,
			AdditionalMetadata: map[string]string{"keyq41": "vq41", "keyq42": "vq42"},
		},
		{
			BrokerID:           "test-query",
			BrokerMessageID:    "567891234",
			BusinessSourceKey:  "noq",
			BusinessObjectKey:  "lmq",
			UniqueID:           "test-query_567891234",
			OrderingKey:        "ordering_key",
			LocationKey:        file3Name,
			LocationPosition:   &file3locationPosition2,
			AdditionalMetadata: map[string]string{"keyq51": "vq51", "keyq52": "vq52"},
		},
	}

	capacity := 2

	batchIntervals := []indexer.BatchInterval{
		{Start: 0, End: 2},
		{Start: 2, End: 4},
		{Start: 4, End: 5},
	}

	file1Records := records[0].Data

	var file2Records []persistor.Record
	file2Records = append(file2Records, records[1:3]...)
	additionalFile2Records := []persistor.Record{
		{
			ID:   "test-query_678912345",
			Data: []byte("d16"),
			Metadata: map[string]string{
				"key161":                    "v161",
				"key162":                    "v162",
				persistor.BusinessObjectKey: "noq",
				persistor.BusinessSourceKey: "lmq",
				orderingKey:                 "ordering_key",
			},
		},
		{
			ID:   "test-query_789123456",
			Data: []byte("dq7"),
			Metadata: map[string]string{
				"keyq71":                    "vq71",
				"keyq72":                    "vq72",
				persistor.BusinessObjectKey: "noq",
				persistor.BusinessSourceKey: "lmq",
				orderingKey:                 "ordering_key",
			},
		},
	}
	file2Records = append(file2Records, additionalFile2Records...)

	file2, err := avro.New().Serialize(file2Records)
	if err != nil {
		panic(err)
	}

	var file3Records []persistor.Record
	file3Records = append(file3Records, records[3:]...)
	additionalFile3Records := []persistor.Record{
		{
			ID:   "test-query_891234567",
			Data: []byte("d18"),
			Metadata: map[string]string{
				"key181":                    "v181",
				"key182":                    "v182",
				persistor.BusinessObjectKey: "noq",
				persistor.BusinessSourceKey: "lmq",
				orderingKey:                 "ordering_key",
			},
		},
		{
			ID:   "test-query_912345678",
			Data: []byte("dq9"),
			Metadata: map[string]string{
				"keyq91":                    "vq91",
				"keyq92":                    "vq92",
				persistor.BusinessObjectKey: "noq",
				persistor.BusinessSourceKey: "lmq",
				orderingKey:                 "ordering_key",
			},
		},
	}
	file3Records = append(file3Records, additionalFile3Records...)

	file3, err := avro.New().Serialize(file3Records)
	if err != nil {
		panic(err)
	}

	fetcherData := map[string][]byte{
		file1Name: file1Records,
		file2Name: file2,
		file3Name: file3,
	}

	return records, indexerMessages, batchIntervals, capacity, fetcherData
}

func checkIfExists(record persistor.Record, expected []persistor.Record) bool {
	for _, expectedRecord := range expected {
		if reflect.DeepEqual(record, expectedRecord) {
			return true
		}
	}
	return false
}

func TestResubmit_Resubmit_Indexer_Error(t *testing.T) {
	_, _, fetcherData := setExpectedResubmit()

	indexerMock := &indexer.Mock{}
	indexerMock.SetGetAllResponse(errors.New("indexer error"))

	fetcherMock := &fetcher.Mock{}
	fetcherMock.SetData(fetcherData)

	publisherMock := &publisher.MockPublisher{}

	resubmitter := New(
		indexerMock,
		fetcherMock,
		avro.New(),
		publisherMock,
	)

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.Resubmit(topicId, mongoCollection, []string{"some-unique-id", "another-unique-id", "the-best-id"})

	if len(result.IndexerErrors) == 0 {
		t.Fatal("error should have occurred")
	}
}

func TestResubmit_ResubmitInterval_Indexer_Error(t *testing.T) {
	_, _, _, _, fetcherData := setExpectedResubmitInterval()

	indexerMock := &indexer.Mock{}
	indexerMock.SetGetAllInIntervalResponse(errors.New("indexer error"))

	fetcherMock := &fetcher.Mock{}
	fetcherMock.SetData(fetcherData)

	publisherMock := &publisher.MockPublisher{}

	resubmitter := New(
		indexerMock,
		fetcherMock,
		avro.New(),
		publisherMock,
	)

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.ResubmitInterval(topicId, mongoCollection, "some-broker-id", time.Time{}, time.Now())

	if len(result.IndexerErrors) == 0 {
		t.Fatal("error should have occurred")
	}
}

func TestResubmit_ResubmitQuery_Indexer_Error(t *testing.T) {
	_, _, _, _, fetcherData := setExpectedResubmitQuery()

	indexerMock := &indexer.Mock{}
	indexerMock.SetGetQueriedResponse(errors.New("indexer error"))

	fetcherMock := &fetcher.Mock{}
	fetcherMock.SetData(fetcherData)

	publisherMock := &publisher.MockPublisher{}

	resubmitter := New(
		indexerMock,
		fetcherMock,
		avro.New(),
		publisherMock,
	)

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.ResubmitQuery(topicId, mongoCollection, query)

	if len(result.IndexerErrors) == 0 {
		t.Fatal("error should have occurred")
	}
}

func TestResubmit_Resubmit_Storage_Error(t *testing.T) {
	_, indexerMessages, _ := setExpectedResubmit()

	indexerMock := &indexer.Mock{}
	indexerMock.SetGetAllResponse(indexerMessages)

	fetcherMock := &fetcher.Mock{}
	fetcherMock.SetData(errors.New("Storage error"))

	publisherMock := &publisher.MockPublisher{}

	resubmitter := New(
		indexerMock,
		fetcherMock,
		avro.New(),
		publisherMock,
	)

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.Resubmit(topicId, mongoCollection, []string{"some-unique-id", "another-unique-id", "the-best-id"})

	if len(result.PipelineErrors) == 0 {
		t.Fatal("error should have occurred")
	}
}

func TestResubmit_ResubmitInterval_Storage_Error(t *testing.T) {
	_, indexerMessages, _, _, _ := setExpectedResubmitInterval()

	indexerMock := &indexer.Mock{}
	indexerMock.SetGetAllInIntervalResponse(indexerMessages)

	fetcherMock := &fetcher.Mock{}
	fetcherMock.SetData(errors.New("Storage error"))

	publisherMock := &publisher.MockPublisher{}

	resubmitter := New(
		indexerMock,
		fetcherMock,
		avro.New(),
		publisherMock,
	)

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.ResubmitInterval(topicId, mongoCollection, "some-broker-id", time.Time{}, time.Now())

	if len(result.PipelineErrors) == 0 {
		t.Fatal("error should have occurred")
	}
}

func TestResubmit_ResubmitQuery_Storage_Error(t *testing.T) {
	_, indexerMessages, _, _, _ := setExpectedResubmitQuery()

	indexerMock := &indexer.Mock{}
	indexerMock.SetGetQueriedResponse(indexerMessages)

	fetcherMock := &fetcher.Mock{}
	fetcherMock.SetData(errors.New("Storage error"))

	publisherMock := &publisher.MockPublisher{}

	resubmitter := New(
		indexerMock,
		fetcherMock,
		avro.New(),
		publisherMock,
	)

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.ResubmitQuery(topicId, mongoCollection, query)

	if len(result.PipelineErrors) == 0 {
		t.Fatal("error should have occurred")
	}
}

func TestResubmit_Resubmit_Serializer_Error(t *testing.T) {
	_, indexerMessages, fetcherData := setExpectedResubmit()

	indexerMock := &indexer.Mock{}
	indexerMock.SetGetAllResponse(indexerMessages)

	fetcherMock := &fetcher.Mock{}
	fetcherMock.SetData(fetcherData)

	publisherMock := &publisher.MockPublisher{}

	resubmitter := New(
		indexerMock,
		fetcherMock,
		serializer.ErrorProducingMock{},
		publisherMock,
	)

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.Resubmit(topicId, mongoCollection, []string{"some-unique-id", "another-unique-id", "the-best-id"})

	if len(result.PipelineErrors) == 0 {
		t.Fatal("error should have occurred")
	}
}

func TestResubmit_ResubmitInterval_Serializer_Error(t *testing.T) {
	_, indexerMessages, _, _, fetcherData := setExpectedResubmitInterval()

	indexerMock := &indexer.Mock{}
	indexerMock.SetGetAllInIntervalResponse(indexerMessages)

	fetcherMock := &fetcher.Mock{}
	fetcherMock.SetData(fetcherData)

	publisherMock := &publisher.MockPublisher{}

	resubmitter := New(
		indexerMock,
		fetcherMock,
		serializer.ErrorProducingMock{},
		publisherMock,
	)

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.ResubmitInterval(topicId, mongoCollection, "some-broker-id", time.Time{}, time.Time{})

	if len(result.PipelineErrors) == 0 {
		t.Fatal("error should have occurred")
	}
}

func TestResubmit_ResubmitQuery_Serializer_Error(t *testing.T) {
	_, indexerMessages, _, _, fetcherData := setExpectedResubmitQuery()

	indexerMock := &indexer.Mock{}
	indexerMock.SetGetQueriedResponse(indexerMessages)

	fetcherMock := &fetcher.Mock{}
	fetcherMock.SetData(fetcherData)

	publisherMock := &publisher.MockPublisher{}

	resubmitter := New(
		indexerMock,
		fetcherMock,
		serializer.ErrorProducingMock{},
		publisherMock,
	)

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.ResubmitQuery(topicId, mongoCollection, query)

	if len(result.PipelineErrors) == 0 {
		t.Fatal("error should have occurred")
	}
}

func TestResubmit_Resubmit_Publisher_Error(t *testing.T) {
	_, indexerMessages, fetcherData := setExpectedResubmit()

	indexerMock := &indexer.Mock{}
	indexerMock.SetGetAllResponse(indexerMessages)

	fetcherMock := &fetcher.Mock{}
	fetcherMock.SetData(fetcherData)

	resubmitter := New(
		indexerMock,
		fetcherMock,
		avro.New(),
		publisher.ErrorProducingPublisher{},
	)

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.Resubmit(topicId, mongoCollection, []string{"some-unique-id", "another-unique-id", "the-best-id"})

	if len(result.PipelineErrors) == 0 {
		t.Fatal("error should have occurred")
	}
}

func TestResubmit_ResubmitInterval_Publisher_Error(t *testing.T) {
	_, indexerMessages, _, _, fetcherData := setExpectedResubmitInterval()

	indexerMock := &indexer.Mock{}
	indexerMock.SetGetAllInIntervalResponse(indexerMessages)

	fetcherMock := &fetcher.Mock{}
	fetcherMock.SetData(fetcherData)

	resubmitter := New(
		indexerMock,
		fetcherMock,
		avro.New(),
		publisher.ErrorProducingPublisher{},
	)

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.ResubmitInterval(topicId, mongoCollection, "some-broker-id", time.Time{}, time.Time{})

	if len(result.PipelineErrors) == 0 {
		t.Fatal("error should have occurred")
	}
}

func TestResubmit_ResubmitQuery_Publisher_Error(t *testing.T) {
	_, indexerMessages, _, _, fetcherData := setExpectedResubmitInterval()

	indexerMock := &indexer.Mock{}
	indexerMock.SetGetQueriedResponse(indexerMessages)

	fetcherMock := &fetcher.Mock{}
	fetcherMock.SetData(fetcherData)

	resubmitter := New(
		indexerMock,
		fetcherMock,
		avro.New(),
		publisher.ErrorProducingPublisher{},
	)

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.ResubmitQuery(topicId, mongoCollection, query)

	if len(result.PipelineErrors) == 0 {
		t.Fatal("error should have occurred")
	}
}

func TestGroupByLocationKey(t *testing.T) {
	const firstLocationKey = "1"
	const secondLocationKey = "2"

	indexerMessage11 := indexer.Message{LocationKey: firstLocationKey}
	indexerMessage12 := indexer.Message{LocationKey: firstLocationKey}
	indexerMessage21 := indexer.Message{LocationKey: secondLocationKey}

	expected := map[string][]indexer.Message{
		firstLocationKey:  {indexerMessage11, indexerMessage12},
		secondLocationKey: {indexerMessage21},
	}
	actual := groupByLocationKey([]indexer.Message{indexerMessage11, indexerMessage12, indexerMessage21})
	if !reflect.DeepEqual(expected, actual) {
		t.Fatal("result doesn't match expected")
	}
}

func TestPackageMetadata(t *testing.T) {
	expected := map[string]string{
		"key1":                      "value1",
		"key2":                      "value2",
		persistor.BusinessSourceKey: "def",
		persistor.BusinessObjectKey: "abc",
		orderingKey:                 "ordering_key",
	}
	indexerMessage := indexer.Message{
		BusinessSourceKey: "def",
		BusinessObjectKey: "abc",
		OrderingKey:       "ordering_key",
		AdditionalMetadata: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}

	actual := packageMetadata(&indexerMessage)
	if !reflect.DeepEqual(expected, actual) {
		t.Fatal("result doesn't match expected")
	}
}

func TestResubmitterService_Resubmit_EndToEnd_GS(t *testing.T) {
	if os.Getenv("RESUBMITTER_E2E") == "" {
		t.Skip()
	}

	resubmitter, err := withGSEnvComponents()
	if err != nil {
		t.Fatal(err)
	}
	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.Resubmit(e2eResubmitTopic, mongoCollection, []string{"per-test-topic_2523966771678879", "per-test-topic_2523625873083639"})
	if len(result.IndexerErrors) != 0 || len(result.PipelineErrors) != 0 {
		t.Fatal("failures occurred")
	}
}

func TestResubmitterService_Resubmit_EndToEnd_ABS(t *testing.T) {
	if os.Getenv("RESUBMITTER_E2E") == "" {
		t.Skip()
	}

	resubmitter, err := withABSEnvComponents()
	if err != nil {
		t.Fatal(err)
	}
	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.Resubmit(e2eResubmitTopic, mongoCollection, []string{"per-test-topic_2523966771678879", "per-test-topic_2523625873083639"})
	if len(result.IndexerErrors) != 0 || len(result.PipelineErrors) != 0 {
		t.Fatal("failures occurred")
	}
}

func TestResubmitterService_ResubmitInterval_EndToEnd(t *testing.T) {
	if os.Getenv("RESUBMITTER_E2E") == "" {
		t.Skip()
	}

	resubmitter, err := withGSEnvComponents()
	if err != nil {
		t.Fatal(err)
	}

	begin, _ := time.Parse(time.RFC3339, "2021-06-11T12:57:00.000+00:00")
	end, _ := time.Parse(time.RFC3339, "2021-06-11T12:57:15.000+00:00")

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.ResubmitInterval(e2eResubmitTopic, mongoCollection, "per-test-topic", begin, end)
	if len(result.IndexerErrors) != 0 || len(result.PipelineErrors) != 0 {
		t.Fatal("failures occurred")
	}
}

func TestResubmitterService_ResubmitInterval_EndToEnd_ABS(t *testing.T) {
	if os.Getenv("RESUBMITTER_E2E") == "" {
		t.Skip()
	}

	resubmitter, err := withABSEnvComponents()
	if err != nil {
		t.Fatal(err)
	}

	begin, _ := time.Parse(time.RFC3339, "2021-06-11T12:57:00.000+00:00")
	end, _ := time.Parse(time.RFC3339, "2021-06-11T12:57:15.000+00:00")

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.ResubmitInterval(e2eResubmitTopic, mongoCollection, "per-test-topic", begin, end)
	if len(result.IndexerErrors) != 0 || len(result.PipelineErrors) != 0 {
		t.Fatal("failures occurred")
	}
}

func TestResubmitterService_ResubmitQuery_EndToEnd_GS(t *testing.T) {
	if os.Getenv("RESUBMITTER_E2E") == "" {
		t.Skip()
	}

	resubmitter, err := withGSEnvComponents()
	if err != nil {
		t.Fatal(err)
	}

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.ResubmitQuery(e2eResubmitTopic, mongoCollection, query)
	if len(result.IndexerErrors) != 0 || len(result.PipelineErrors) != 0 {
		t.Fatal("failures occurred")
	}
}

func TestResubmitterService_ResubmitQuery_EndToEnd_ABS(t *testing.T) {
	if os.Getenv("RESUBMITTER_E2E") == "" {
		t.Skip()
	}

	resubmitter, err := withABSEnvComponents()
	if err != nil {
		t.Fatal(err)
	}

	resubmitterJob := &resubmitterJob{resubmitter: *resubmitter}
	result := resubmitterJob.ResubmitQuery(e2eResubmitTopic, mongoCollection, query)
	if len(result.IndexerErrors) != 0 || len(result.PipelineErrors) != 0 {
		t.Fatal("failures occurred")
	}
}

func withGSEnvComponents() (*Resubmitter, error) {
	storageService, err := gs.FromEnv()
	if err != nil {
		return nil, err
	}

	pub, err := pubsub.FromEnv()
	if err != nil {
		return nil, err
	}

	return FromEnv(
		indexer.FromEnv(),
		storageService,
		avro.New(),
		pub,
	)
}

func withABSEnvComponents() (*Resubmitter, error) {
	storageService, err := abs.FromEnv()
	if err != nil {
		return nil, err
	}

	pub, err := pubsub.FromEnv()
	if err != nil {
		return nil, err
	}

	return FromEnv(
		indexer.FromEnv(),
		storageService,
		avro.New(),
		pub,
	)
}
