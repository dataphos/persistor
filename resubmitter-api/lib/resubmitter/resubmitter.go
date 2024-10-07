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

// Package resubmitter contains the core Resubmitter logic
package resubmitter

import (
	"fmt"
	"os"
	"strconv"

	"github.com/dataphos/persistor-resubmitter-api/common/log"
	"github.com/dataphos/persistor-resubmitter-api/lib/fetcher"
	"github.com/dataphos/persistor-resubmitter-api/lib/indexer"
	"github.com/dataphos/persistor-resubmitter-api/lib/persistor"
	"github.com/dataphos/persistor-resubmitter-api/lib/publisher"
	"github.com/dataphos/persistor-resubmitter-api/lib/serializer"
)

type Resubmitter struct {
	Indexer      indexer.Indexer
	BlobFetcher  fetcher.Fetcher
	Deserializer serializer.Deserializer
	Publisher    publisher.Publisher
	Settings     Settings
}

type Settings struct {
	MetadataCapacity    int
	PipelineCapacity    int
	NumPackagingWorkers int
}

var DefaultSettings = Settings{
	MetadataCapacity:    10000,
	PipelineCapacity:    100,
	NumPackagingWorkers: 2,
}

type Option func(*Settings)

func WithMetadataCapacity(capacity int) Option {
	return func(settings *Settings) {
		settings.MetadataCapacity = capacity
	}
}

func WithPipelineCapacity(capacity int) Option {
	return func(settings *Settings) {
		settings.PipelineCapacity = capacity
	}
}

func WithWorkers(workers int) Option {
	return func(settings *Settings) {
		settings.NumPackagingWorkers = workers
	}
}

func FromEnv(indexer indexer.Indexer, blobFetcher fetcher.Fetcher, deserializer serializer.Deserializer, publisher publisher.Publisher) (*Resubmitter, error) {
	opts, err := loadSettingsFromEnv()
	if err != nil {
		return nil, fmt.Errorf("error during the loading of resubmitter settings from environment: %w", err)
	}

	return New(indexer, blobFetcher, deserializer, publisher, opts...), nil
}

const (
	MetadataCapacityEnv    = "RSB_META_CAPACITY"
	PipelineCapacityEnv    = "RSB_FETCH_CAPACITY"
	NumPackagingWorkersEnv = "RSB_WORKER_NUM"

	messageOrderingEnabledEnv = "RSB_ENABLE_MESSAGE_ORDERING"
)

func loadSettingsFromEnv() ([]Option, error) {
	var opts []Option

	if metadataCapacity := os.Getenv(MetadataCapacityEnv); metadataCapacity == "" {
		log.Warn(log.UsingDefaultParameterValue(MetadataCapacityEnv, DefaultSettings.MetadataCapacity))
	} else if metadataCapacity, err := strconv.Atoi(metadataCapacity); metadataCapacity > 0 {
		opts = append(opts, WithMetadataCapacity(metadataCapacity))
	} else {
		return nil, log.PositiveIntEnvVariableError(MetadataCapacityEnv, err)
	}

	if pipelineCapacity := os.Getenv(PipelineCapacityEnv); pipelineCapacity == "" {
		log.Warn(log.UsingDefaultParameterValue(PipelineCapacityEnv, DefaultSettings.PipelineCapacity))
	} else if pipelineCapacity, err := strconv.Atoi(pipelineCapacity); pipelineCapacity > 0 {
		opts = append(opts, WithPipelineCapacity(pipelineCapacity))
	} else {
		return nil, log.PositiveIntEnvVariableError(PipelineCapacityEnv, err)
	}

	if numPackagingWorkers := os.Getenv(NumPackagingWorkersEnv); numPackagingWorkers == "" {
		log.Warn(log.UsingDefaultParameterValue(NumPackagingWorkersEnv, DefaultSettings.NumPackagingWorkers))
	} else if numPackagingWorkers, err := strconv.Atoi(numPackagingWorkers); numPackagingWorkers > 0 {
		opts = append(opts, WithWorkers(numPackagingWorkers))
	} else {
		return nil, log.PositiveIntEnvVariableError(NumPackagingWorkersEnv, err)
	}

	return opts, nil
}

func New(indexer indexer.Indexer, blobFetcher fetcher.Fetcher, deserializer serializer.Deserializer, publisher publisher.Publisher, opts ...Option) *Resubmitter {
	settings := DefaultSettings
	for _, opt := range opts {
		opt(&settings)
	}

	log.Info(fmt.Sprintf("resubmitter settings: %v", settings))

	return &Resubmitter{
		Indexer:      indexer,
		BlobFetcher:  blobFetcher,
		Deserializer: deserializer,
		Publisher:    publisher,
		Settings:     settings,
	}
}

type IndexerError struct {
	Reason string `json:"reason"`
}

type PipelineError struct {
	Id     string `json:"id"`
	Reason string `json:"reason"`
}

type ResubmitResult struct {
	IndexerErrors  []IndexerError  `json:"indexer_errors,omitempty"`
	PipelineErrors []PipelineError `json:"pipeline_errors,omitempty"`
}

type source func() (<-chan []indexer.Message, <-chan IndexerError)

type fetchJob struct {
	location string
	messages []indexer.Message
}

type packageJob struct {
	blob     []byte
	messages []indexer.Message
}

type publishJob struct {
	topicID string
	records map[string][]persistor.Record
}

func (resubmitter *Resubmitter) convertToRecordsAndGroupByKey(blob []byte, messages []indexer.Message) (map[string][]persistor.Record, string, error) {
	messageOrderingEnabled := readBoolEnvVariable(messageOrderingEnabledEnv)

	if !shouldDeserialize(messages) {
		return resubmitter.convertSingle(blob, messages, messageOrderingEnabled)
	}

	return resubmitter.convertBulk(blob, messages, messageOrderingEnabled)
}

func shouldDeserialize(messages []indexer.Message) bool {
	if len(messages) == 1 && messages[0].LocationPosition == nil {
		return false
	}

	return true
}

func (resubmitter *Resubmitter) convertSingle(blob []byte, messages []indexer.Message, messageOrderingEnabled bool) (map[string][]persistor.Record, string, error) {
	var records []persistor.Record

	record := persistor.Record{
		ID:       messages[0].UniqueID,
		Data:     blob,
		Metadata: packageMetadata(&messages[0]),
	}

	records = append(records, record)

	var key string
	if messageOrderingEnabled {
		key = messages[0].OrderingKey
	}

	return map[string][]persistor.Record{key: records}, messages[0].BrokerID, nil
}

func (resubmitter *Resubmitter) convertBulk(blob []byte, messages []indexer.Message, messageOrderingEnabled bool) (map[string][]persistor.Record, string, error) {
	records, err := resubmitter.Deserializer.Deserialize(blob)
	if err != nil {
		return nil, "", err
	}

	return processRecords(messages, records, messageOrderingEnabled), messages[0].BrokerID, nil
}

func collectErrors(indexerErrChan <-chan IndexerError, pipelineErrChan <-chan PipelineError) ResubmitResult {
	indexerErrCollector := collectIndexerErrors(indexerErrChan)
	pipelineErrCollector := collectPipelineErrors(pipelineErrChan)

	return ResubmitResult{
		IndexerErrors:  <-indexerErrCollector,
		PipelineErrors: <-pipelineErrCollector,
	}
}

func collectIndexerErrors(errc <-chan IndexerError) <-chan []IndexerError {
	collectedErrChan := make(chan []IndexerError)

	go func() {
		defer close(collectedErrChan)

		var errs []IndexerError
		for err := range errc {
			errs = append(errs, err)
		}
		collectedErrChan <- errs
	}()

	return collectedErrChan
}

func collectPipelineErrors(errc <-chan PipelineError) <-chan []PipelineError {
	collectedErrChan := make(chan []PipelineError)

	go func() {
		defer close(collectedErrChan)

		var pipelineErrors []PipelineError
		for err := range errc {
			pipelineErrors = append(pipelineErrors, err)
		}
		collectedErrChan <- pipelineErrors
	}()

	return collectedErrChan
}
