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
	"context"
	"fmt"
	"github.com/dataphos/persistor-resubmitter-api/common/errcodes"
	"github.com/dataphos/persistor-resubmitter-api/common/log"
	"github.com/dataphos/persistor-resubmitter-api/common/util"
	"github.com/dataphos/persistor-resubmitter-api/lib/indexer"
	"github.com/dataphos/persistor-resubmitter-api/lib/persistor"
	"sync"
	"time"
)

const (
	orderingKey = "ordering_key"
)

type resubmitterJob struct {
	resubmitter Resubmitter

	// number of message IDs given in the request body.
	StartingCounter int `json:"starting_count,omitempty"`
	// number of messages found by Indexer.
	IndexedCounter int `json:"indexed_count"`
	// number of messages successfully fetched from the storage.
	FetchedCounter int `json:"fetched_count"`
	// number of successfully deserialized messages.
	DeserializedCounter int `json:"deserialized_count"`
	// number of successfully published messages.
	PublishedCounter int `json:"published_count"`

	startingMutex     sync.Mutex
	indexedMutex      sync.Mutex
	fetchedMutex      sync.Mutex
	deserializedMutex sync.Mutex
	publishedMutex    sync.Mutex
}

func (resubmitterJob *resubmitterJob) UpdateStartingCounter(count int) {
	resubmitterJob.startingMutex.Lock()
	resubmitterJob.StartingCounter += count
	resubmitterJob.startingMutex.Unlock()
}

func (resubmitterJob *resubmitterJob) UpdateIndexedCounter(count int) {
	resubmitterJob.indexedMutex.Lock()
	resubmitterJob.IndexedCounter += count
	resubmitterJob.indexedMutex.Unlock()
}

func (resubmitterJob *resubmitterJob) UpdateFetchedCounter(count int) {
	resubmitterJob.fetchedMutex.Lock()
	resubmitterJob.FetchedCounter += count
	resubmitterJob.fetchedMutex.Unlock()
}

func (resubmitterJob *resubmitterJob) UpdateDeserializedCounter(count int) {
	resubmitterJob.deserializedMutex.Lock()
	resubmitterJob.DeserializedCounter += count
	resubmitterJob.deserializedMutex.Unlock()
}

func (resubmitterJob *resubmitterJob) UpdatePublishedCounter(count int) {
	resubmitterJob.publishedMutex.Lock()
	resubmitterJob.PublishedCounter += count
	resubmitterJob.publishedMutex.Unlock()
}

func (resubmitterJob *resubmitterJob) resetCounters() {
	resubmitterJob.StartingCounter = 0
	resubmitterJob.IndexedCounter = 0
	resubmitterJob.FetchedCounter = 0
	resubmitterJob.DeserializedCounter = 0
	resubmitterJob.PublishedCounter = 0
}

func (resubmitterJob *resubmitterJob) Resubmit(topicId, mongoCollection string, ids []string) ResubmitResult {
	return resubmitterJob.run(topicId, func() (<-chan []indexer.Message, <-chan IndexerError) {
		return resubmitterJob.batchesFromIds(mongoCollection, ids)
	})
}

func (resubmitterJob *resubmitterJob) batchesFromIds(mongoCollection string, ids []string) (<-chan []indexer.Message, <-chan IndexerError) {
	batches := make(chan []indexer.Message, 1)
	errc := make(chan IndexerError, 1)

	generator := func(start, end int) {
		batch, err := resubmitterJob.resubmitter.Indexer.GetAll(mongoCollection, ids[start:end])
		if err != nil {
			log.Debug(err.Error(), errcodes.Indexer)
			errc <- IndexerError{Reason: err.Error()}

			return
		}

		log.Debug(fmt.Sprintf("Indexer API responded with %v messages", len(batch)), 0)

		resubmitterJob.UpdateIndexedCounter(len(batch))

		batches <- batch
	}

	collectionSize := len(ids)
	batchSize := resubmitterJob.resubmitter.Settings.MetadataCapacity
	fullBatches := collectionSize / batchSize
	totalBatches := fullBatches

	if len(ids)%resubmitterJob.resubmitter.Settings.MetadataCapacity != 0 {
		totalBatches++
	}

	go func() {
		defer close(batches)
		defer close(errc)

		var i int
		for ; i < fullBatches; i++ {
			generator(i*batchSize, (i+1)*batchSize)
		}

		if totalBatches != fullBatches {
			generator(i*batchSize, collectionSize)
		}
	}()

	resubmitterJob.UpdateStartingCounter(len(ids))

	return batches, errc
}

func (resubmitterJob *resubmitterJob) ResubmitInterval(topicId, mongoCollection, brokerId string, lb time.Time, ub time.Time) ResubmitResult {
	return resubmitterJob.run(topicId, func() (<-chan []indexer.Message, <-chan IndexerError) {
		return resubmitterJob.batchesFromInterval(mongoCollection, brokerId, lb, ub)
	})
}

func (resubmitterJob *resubmitterJob) batchesFromInterval(mongoCollection, brokerId string, from, to time.Time) (<-chan []indexer.Message, <-chan IndexerError) {
	batches := make(chan []indexer.Message, 1)
	errc := make(chan IndexerError)

	batchSize := resubmitterJob.resubmitter.Settings.MetadataCapacity

	go func() {
		defer close(batches)
		defer close(errc)

		intervalQueryResponse, err := resubmitterJob.resubmitter.Indexer.GetAllInInterval(mongoCollection, brokerId, from, to, batchSize, 0)
		if err != nil {
			log.Debug(err.Error(), errcodes.Indexer)
			errc <- IndexerError{Reason: err.Error()}

			return
		}

		log.Debug(fmt.Sprintf("Indexer API responded with %v messages", intervalQueryResponse.ReturnedCount), 0)

		batches <- intervalQueryResponse.Messages

		collectionSize := intervalQueryResponse.TotalCount
		offset := intervalQueryResponse.ReturnedCount

		resubmitterJob.UpdateIndexedCounter(intervalQueryResponse.ReturnedCount)

		for offset < collectionSize {
			intervalQueryResponse, err = resubmitterJob.resubmitter.Indexer.GetAllInInterval(mongoCollection, brokerId, from, to, batchSize, offset)
			if err != nil {
				log.Debug(err.Error(), errcodes.Indexer)
				errc <- IndexerError{Reason: err.Error()}

				return
			}

			log.Debug(fmt.Sprintf("Indexer API responded with %v messages", intervalQueryResponse.ReturnedCount), 0)

			batches <- intervalQueryResponse.Messages

			offset += intervalQueryResponse.ReturnedCount

			resubmitterJob.UpdateIndexedCounter(intervalQueryResponse.ReturnedCount)
		}
	}()

	return batches, errc
}

func (resubmitterJob *resubmitterJob) ResubmitQuery(topicId, mongoCollection string, queryBody util.QueryRequestBody) ResubmitResult {
	return resubmitterJob.run(topicId, func() (<-chan []indexer.Message, <-chan IndexerError) {
		return resubmitterJob.batchesFromQuery(mongoCollection, queryBody)
	})
}

func (resubmitterJob *resubmitterJob) batchesFromQuery(mongoCollection string, queryBody util.QueryRequestBody) (<-chan []indexer.Message, <-chan IndexerError) {
	batches := make(chan []indexer.Message, 1)
	errc := make(chan IndexerError)

	batchSize := resubmitterJob.resubmitter.Settings.MetadataCapacity

	go func() {
		defer close(batches)
		defer close(errc)

		intervalQueryResponse, err := resubmitterJob.resubmitter.Indexer.GetQueried(mongoCollection, queryBody, batchSize, 0)
		if err != nil {
			log.Debug(err.Error(), errcodes.Indexer)
			errc <- IndexerError{Reason: err.Error()}

			return
		}

		log.Debug(fmt.Sprintf("Indexer API responded with %v messages", intervalQueryResponse.ReturnedCount), 0)

		batches <- intervalQueryResponse.Messages

		collectionSize := intervalQueryResponse.TotalCount
		offset := intervalQueryResponse.ReturnedCount

		resubmitterJob.UpdateIndexedCounter(intervalQueryResponse.ReturnedCount)

		for offset < collectionSize {
			intervalQueryResponse, err = resubmitterJob.resubmitter.Indexer.GetQueried(mongoCollection, queryBody, batchSize, offset)
			if err != nil {
				log.Debug(err.Error(), errcodes.Indexer)
				errc <- IndexerError{Reason: err.Error()}

				return
			}

			log.Debug(fmt.Sprintf("Indexer API responded with %v messages", intervalQueryResponse.ReturnedCount), 0)

			batches <- intervalQueryResponse.Messages

			offset += intervalQueryResponse.ReturnedCount

			resubmitterJob.UpdateIndexedCounter(intervalQueryResponse.ReturnedCount)
		}
	}()

	return batches, errc
}

func (resubmitterJob *resubmitterJob) run(topicId string, init source) ResubmitResult {
	batches, indexerErrChan := init()
	pipelineErrChan := resubmitterJob.pipeline(context.Background(), topicId, batches)

	return collectErrors(indexerErrChan, pipelineErrChan)
}

func (resubmitterJob *resubmitterJob) pipeline(ctx context.Context, topicId string, batches <-chan []indexer.Message) <-chan PipelineError {
	groups := resubmitterJob.groupings(batches)
	blobs, fetchErrChan := resubmitterJob.fetch(ctx, groups)
	records, packagingErrChan := resubmitterJob.packaging(blobs)
	publishErrChan := resubmitterJob.publish(ctx, topicId, records)

	return merge(fetchErrChan, packagingErrChan, publishErrChan)
}

func (resubmitterJob *resubmitterJob) groupings(batches <-chan []indexer.Message) <-chan fetchJob {
	results := make(chan fetchJob, resubmitterJob.resubmitter.Settings.PipelineCapacity)

	go func() {
		defer close(results)

		for batch := range batches {
			for locationKey, messages := range groupByLocationKey(batch) {
				results <- fetchJob{
					location: locationKey,
					messages: messages,
				}
				log.Debug(fmt.Sprintf("created a fetch job for %v with %v messages", locationKey, len(messages)), 0)
			}
		}
	}()

	return results
}

func (resubmitterJob *resubmitterJob) fetch(ctx context.Context, jobs <-chan fetchJob) (<-chan packageJob, <-chan PipelineError) {
	results := make(chan packageJob, resubmitterJob.resubmitter.Settings.PipelineCapacity)
	errc := make(chan PipelineError, 1)

	fetchingWorker := func(location string, messages []indexer.Message, wg *sync.WaitGroup) {
		defer wg.Done()

		blob, err := resubmitterJob.resubmitter.BlobFetcher.Fetch(ctx, location)
		if err != nil {
			log.Debug(err.Error(), errcodes.Fetcher)
			tagAsFailures(messages, "storage_error", errc)

			return
		}

		results <- packageJob{
			blob:     blob,
			messages: messages,
		}

		resubmitterJob.UpdateFetchedCounter(len(messages))
	}

	go func() {
		defer close(results)
		defer close(errc)

		var wg sync.WaitGroup

		for job := range jobs {
			wg.Add(1)
			go fetchingWorker(job.location, job.messages, &wg)
		}
		wg.Wait()
	}()

	return results, errc
}

func (resubmitterJob *resubmitterJob) packaging(jobs <-chan packageJob) (<-chan publishJob, <-chan PipelineError) {
	results := make(chan publishJob, cap(jobs))
	errc := make(chan PipelineError, 1)

	packagingWorker := func(wg *sync.WaitGroup) {
		defer wg.Done()

		for job := range jobs {
			records, topicId, err := resubmitterJob.resubmitter.convertToRecordsAndGroupByKey(job.blob, job.messages)
			if err != nil {
				log.Debug(err.Error(), errcodes.Serializer)
				tagAsFailures(job.messages, "deserialization_error", errc)

				continue
			}

			log.Debug(fmt.Sprintf("created %v record groups", len(records)), 0)

			results <- publishJob{
				topicId: topicId,
				records: records,
			}

			count := 0

			for _, t := range records {
				count += len(t)
			}
			resubmitterJob.UpdateDeserializedCounter(count)
		}
	}

	go func() {
		defer close(results)
		defer close(errc)

		var wg sync.WaitGroup
		numWorkers := min(resubmitterJob.resubmitter.Settings.NumPackagingWorkers, cap(jobs))
		wg.Add(numWorkers)

		for i := 0; i < numWorkers; i++ {
			go packagingWorker(&wg)
		}
		wg.Wait()
	}()

	return results, errc
}

func (resubmitterJob *resubmitterJob) publish(ctx context.Context, topicId string, jobs <-chan publishJob) <-chan PipelineError {
	errc := make(chan PipelineError, 1)

	topic, err := resubmitterJob.resubmitter.Publisher.Topic(topicId)
	if err != nil {
		log.Fatal(err.Error(), errcodes.Publisher)
	}

	recordPublisher := func(record persistor.Record) {

		err := topic.Publish(ctx, &record)
		if err != nil {
			log.Debug(err.Error(), errcodes.Publisher)
			errc <- PipelineError{
				Id:     record.ID,
				Reason: "publish_error",
			}
		} else {
			resubmitterJob.UpdatePublishedCounter(1)
		}
	}

	go func() {
		defer close(errc)

		for job := range jobs {
			var wg sync.WaitGroup

			groupedRecords := job.records
			keyless := groupedRecords[""]

			log.Debug(fmt.Sprintf("starting publishing %v keyless records", len(keyless)), 0)

			for _, record := range keyless {
				wg.Add(1)

				record := record

				go func() {
					defer wg.Done()
					recordPublisher(record)
				}()
			}

			for key, group := range groupedRecords {
				if key == "" {
					continue
				}

				wg.Add(1)

				group := group
				log.Debug(fmt.Sprintf("starting publishing %v records with key %v", len(group), key), 0)

				go func() {
					defer wg.Done()

					for _, record := range group {
						record := record
						recordPublisher(record)
					}
				}()
			}
			wg.Wait()
		}
	}()

	return errc
}
