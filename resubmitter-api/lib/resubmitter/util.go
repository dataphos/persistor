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
	"strconv"
	"sync"

	"github.com/dataphos/persistor-resubmitter-api/lib/indexer"
	"github.com/dataphos/persistor-resubmitter-api/lib/persistor"
)

const (
	topicParam = "topic"
)

func groupByLocationKey(indexerMessages []indexer.Message) map[string][]indexer.Message {
	buckets := make(map[string][]indexer.Message)

	for _, indexerMessage := range indexerMessages {
		bucket := buckets[indexerMessage.LocationKey]
		bucket = append(bucket, indexerMessage)
		buckets[indexerMessage.LocationKey] = bucket
	}

	return buckets
}

func processRecords(messages []indexer.Message, records []persistor.Record, messageOrderingEnabled bool) map[string][]persistor.Record {
	groupedRecords := make(map[string][]persistor.Record, len(messages))

	for _, message := range messages {
		message := message
		targetIdx := *message.LocationPosition - 1
		record := records[targetIdx]
		record.Metadata = packageMetadata(&message)
		record.ID = message.UniqueID

		if messageOrderingEnabled {
			groupedRecords[message.OrderingKey] = append(groupedRecords[message.OrderingKey], record)
		} else {
			groupedRecords[""] = append(groupedRecords[""], record)
		}
	}

	return groupedRecords
}

func packageMetadata(message *indexer.Message) map[string]string {
	metadata := make(map[string]string)

	for key, entity := range message.AdditionalMetadata {
		metadata[key] = entity
	}

	if message.BusinessObjectKey != "" {
		metadata[persistor.BusinessObjectKey] = message.BusinessObjectKey
	}

	if message.BusinessSourceKey != "" {
		metadata[persistor.BusinessSourceKey] = message.BusinessSourceKey
	}

	if message.OrderingKey != "" {
		metadata[orderingKey] = message.OrderingKey
	}

	return metadata
}

func tagAsFailures(messages []indexer.Message, reason string, errc chan<- PipelineError) {
	for _, message := range messages {
		errc <- PipelineError{
			Id:     message.UniqueID,
			Reason: reason,
		}
	}
}

func merge(errChannels ...<-chan PipelineError) <-chan PipelineError {
	out := make(chan PipelineError, len(errChannels))

	go func() {
		defer close(out)

		var waitGroup sync.WaitGroup

		for _, errChan := range errChannels {
			waitGroup.Add(1)

			go func(errChan <-chan PipelineError) {
				defer waitGroup.Done()

				for err := range errChan {
					out <- err
				}
			}(errChan)
		}

		waitGroup.Wait()
	}()

	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}

	return b
}

func readBoolEnvVariable(envKey string) bool {
	envValue, ok := os.LookupEnv(envKey)
	if ok {
		value, err := strconv.ParseBool(envValue)
		if err == nil {
			return value
		}
	}

	return false
}
