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

package persistor

import (
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-streamproc/pkg/streamproc"
	"github.com/dataphos/persistor/internal/common"
	"github.com/dataphos/persistor/internal/common/log"
	"github.com/dataphos/persistor/internal/sender"
	"github.com/dataphos/persistor/internal/writer"
)

type Persistor struct {
	Writer                   writer.BlobWriter
	Storage                  StorageProperties
	IndexerEnabled           bool
	IndexerTopic             broker.Topic
	HandlerCtx               context.Context
	Cancel                   context.CancelFunc
	DeadLetterActive         bool
	DeadLetterTopic          broker.Topic
	tolerateDeadLetterErrors bool
}

const (
	WriterErrorCategory         = "Storage error"
	IndexingSenderErrorCategory = "Metadata sender error"
)

func (p *Persistor) MiniBatchProcessor(ctx context.Context, batchPos int, batch []streamproc.Message, wg *sync.WaitGroup, parallelErrors []error) {
	defer wg.Done()
	// persisting the batch named after its first message.
	objectName := p.Storage.GenerateBlobName(batch[0])

	storageError := p.Writer.Write(ctx, p.Storage.Config.Destination, objectName, batch...)
	// if writing fails, handle error (entire mini-batch since the file is either written fully or isn't at all).
	if storageError != nil {
		err := p.HandleBatchError(ctx, WriterErrorCategory, sender.DeadLetterSourcePersistor, storageError, "Error writing to storage", common.WriterError, batch...)
		parallelErrors[batchPos] = err

		return
	}

	if !p.IndexerEnabled {
		common.UpdateSuccessMetrics(batch...)

		return
	}

	// if writing went well, send to indexer topic.
	location, locationErr := p.Storage.GetCompletePath(objectName)
	if locationErr != nil {
		common.UpdateFailureMetrics(batch...)
		parallelErrors[batchPos] = locationErr

		return
	}

	if senderErr := sender.SendToIndexerTopic(ctx, p.IndexerTopic, batch, location, p.Storage.Config.TopicID); senderErr != nil {
		// for now assume no messages in mini-batch could be sent.
		err := p.HandleBatchError(ctx, IndexingSenderErrorCategory, sender.DeadLetterSourcePersistor, senderErr, "Error sending metadata to indexer", common.WriterError, batch...)
		parallelErrors[batchPos] = err

		return
	}

	common.UpdateSuccessMetrics(batch...)
}

// HandleBatch is a function that encompasses the core functionality of the Persistor
// Flow:
// Take in a batch of messages -> separate them by versions if versioning is enabled -> write batches to storage
// -> if writing to storage fails, send to dead letter topic
// -> add metadata to messages successfully written to storage -> send messages with metadata to indexer topic
// if errors occur, send the respective messages to dead letter topic
// if dead-lettering fails or isn't enabled, return error with the integer positions of failed messages (or just a regular error if they all failed).
func (p *Persistor) HandleBatch(_ context.Context, messages []streamproc.Message) error {
	if len(messages) == 0 {
		return nil
	}
	var (
		miniBatchToBatchIndices [][]int        // the nth position holds a list of indices of messages belonging to the nth mini batch.
		failedIndices           []int          // an array that accumulates the position of all failed messages.
		processorGroup          sync.WaitGroup // wait group to wait for all goroutines to finish.
	)

	ctx := p.HandlerCtx

	batches := [][]streamproc.Message{messages}

	// separating by versions if versions are enabled.
	if p.Storage.ContainsVersionKey() {
		batches, miniBatchToBatchIndices = BatchByVersions(messages, p.Storage.VersionKeys)
	} else {
		allIndices := ZeroToN(len(messages))
		miniBatchToBatchIndices = [][]int{allIndices} // if version is not enabled, there is only one mini batch.
	}

	parallelErrors := make([]error, len(batches)) // accumulates all errors that occur in the go-routines that process the batches.

	for i, batch := range batches {
		processorGroup.Add(1)

		//nolint:contextcheck // using the handler context here that wasn't inherited from streamproc because we want the batch to finish processing if possible.
		go p.MiniBatchProcessor(ctx, i, batch, &processorGroup, parallelErrors)
	}

	// Wait for all goroutines to finish.
	processorGroup.Wait()

	var batchErrors []*common.ProcError
	fatal := false

	for iErr, err := range parallelErrors {
		if err != nil {
			fatalErr := &common.FatalError{}
			if isFatal := errors.As(err, &fatalErr); isFatal {
				fatal = true
			}

			batchErrors = append(batchErrors, common.NewProcessingError(len(batches[iErr]), err, common.ProcessingError))
			failedIndices = append(failedIndices, miniBatchToBatchIndices[iErr]...)
		}
	}

	if len(batchErrors) > 0 {
		handlerError := &common.MessageBatchError{ErrorList: batchErrors}

		if fatal {
			return &common.FatalError{Err: handlerError}
		}

		if len(batchErrors) < len(batches) {
			// some messages were processed, return failed positions.
			return &streamproc.PartiallyProcessedBatchError{
				Failed: failedIndices,
				Err:    handlerError,
			}
		} else {
			return handlerError
		}
	}

	return nil
}

// HandleBatchError function that sends messages to dead letter topic or causes them to be handled again later.
func (p *Persistor) HandleBatchError(ctx context.Context, errCategory, errSource string, err error, errorMessage string, errorCode uint64, failedBatch ...streamproc.Message) error {
	common.UpdateFailureMetrics(failedBatch...)

	if !p.DeadLetterActive {
		return err
	}

	if deadLetterErr := sender.SendToDeadLetter(ctx, errCategory, errSource, err.Error(), p.DeadLetterTopic, failedBatch...); deadLetterErr == nil {
		// sending to dead letter okay and messages will be ack-ed, but log the error that happened.
		log.Errorw(errorMessage, errorCode, log.F{log.ErrorFieldKey: err.Error()})

		return nil
	} else {
		// no need to log as we are returning the error; it will get logged in executor callbacks.
		batchErr := &common.MessageBatchError{ErrorList: []*common.ProcError{common.NewProcessingError(len(failedBatch), err, common.ProcessingError)}}
		batchErr.AddErr(common.NewProcessingError(len(failedBatch), deadLetterErr, common.SenderDeadLetterError))
		if p.tolerateDeadLetterErrors {
			return batchErr
		} else {
			return &common.FatalError{Err: batchErr}
		}
	}
}

func ZeroToN(n int) []int {
	nums := make([]int, n)
	for i := range nums {
		nums[i] = i
	}

	return nums
}

func (p *Persistor) End() {
	// when pull is canceled, allow handler to process any remaining batches if it has them.
	<-p.HandlerCtx.Done()
}
