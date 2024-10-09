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

// Package indexer contains the logic for the Indexer component.
package indexer

import (
	"context"
	"fmt"

	"github.com/go-playground/validator/v10"

	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-streamproc/pkg/streamproc"
	"github.com/dataphos/persistor/internal/common"
	"github.com/dataphos/persistor/internal/common/log"
	"github.com/dataphos/persistor/internal/sender"
	"github.com/dataphos/persistor/internal/writer"
)

type Indexer struct {
	mongoWriter              writer.RecordWriter
	handlerCtx               context.Context
	cancel                   context.CancelFunc
	validate                 *validator.Validate
	deadLetterTopic          broker.Topic
	tolerateDeadLetterErrors bool
}

const (
	MessageParserErrorCategory = "Metadata parser error"
	MongoWriterErrorCategory   = "MongoDB storage error"
)

func (indexer Indexer) HandleBatch(_ context.Context, messages []streamproc.Message) error {
	ctx := indexer.handlerCtx

	// Process the batch, preparing it to write to Mongo.
	parsedMessages, failedMsgs, batch, loaderErr, parsedPositions := indexer.TransformToIndexerData(ctx, messages...) //nolint:contextcheck // handler context to not have to call context.Background() every time
	if loaderErr != nil {
		// log parsing errors and continue processing the messages which were parsed successfully.
		log.Errorw(fmt.Sprintf("Transform to indexer data failed on %d/%d messages in batch", len(failedMsgs), len(messages)), common.DataLoadError, log.F{log.ErrorFieldKey: loaderErr.Error()})
		common.UpdateFailureMetrics(failedMsgs...)

		if indexer.deadLetterTopic != nil {
			// send messages that can't be parsed to dead letter topic.
			err := sender.SendToDeadLetter(ctx, MessageParserErrorCategory, sender.DeadLetterSourceIndexer, loaderErr.Error(), indexer.deadLetterTopic, failedMsgs...) //nolint:contextcheck // using the handler context here that wasn't inherited from streamproc because we want the batch to finish processing if possible
			if err != nil {
				// for now if we can't send them to dead letter topic, ignore them. These messages were likely sent by accident and not by Persistor.
				log.Errorw("Error sending badly formatted messages to dead letter", common.SenderDeadLetterError, log.F{log.ErrorFieldKey: err.Error()})
			}
		}
	}

	// If no messages passed the data loading phase, end the function here.
	if len(parsedMessages) == 0 {
		return nil
	}

	failedIndices, err := indexer.mongoWriter.Write(ctx, batch) //nolint:contextcheck // using the handler context here that wasn't inherited from streamproc because we want the batch to finish processing if possible

	if err == nil {
		common.UpdateSuccessMetrics(parsedMessages...)

		return nil // O.K.
	} else if len(failedIndices) == 0 {
		// we want to nack all messages, return the general error.
		common.UpdateFailureMetrics(parsedMessages...)
	} else {
		good, bad := MessagesGoodBad(parsedMessages, failedIndices)
		common.UpdateSuccessMetrics(good...)
		common.UpdateFailureMetrics(bad...)
	}

	log.Errorw("Mongo writer error", common.WriterError, log.F{log.ErrorFieldKey: err.Error()})
	// for now assume nothing got stored in mongo.
	if indexer.deadLetterTopic != nil {
		dlErr := sender.SendToDeadLetter(ctx, MongoWriterErrorCategory, sender.DeadLetterSourceIndexer, err.Error(), indexer.deadLetterTopic, parsedMessages...) //nolint:contextcheck // using the handler context here that wasn't inherited from streamproc because we want the batch to finish processing if possible
		if dlErr == nil {
			return nil // sending to dead letter worked, no error.
		} else {
			log.Errorw("Error sending failed messages to dead letter", common.SenderDeadLetterError, log.F{log.ErrorFieldKey: dlErr.Error()})

			if !indexer.tolerateDeadLetterErrors {
				return &common.FatalError{Err: dlErr}
			}
		}
	}

	if len(parsedPositions) == len(messages) {
		// no parsing errors, must reprocess everything.
		return fmt.Errorf("indexing handler: %w", err)
	}
	// mark as failed only those messages which got parsed correctly.
	return &streamproc.PartiallyProcessedBatchError{
		Failed: parsedPositions,
		Err:    err,
	}
}

func MessagesGoodBad(msgs []streamproc.Message, failed []int) (good []streamproc.Message, bad []streamproc.Message) {
	failedOrNot := map[int]int{}
	for _, failedPos := range failed {
		failedOrNot[failedPos] = 1
	}

	for i := 0; i < len(msgs); i++ {
		msgPos, msgFailed := failedOrNot[i]
		if msgFailed {
			bad = append(bad, msgs[msgPos])
		} else {
			good = append(good, msgs[msgPos])
		}
	}

	return good, bad
}

func (indexer *Indexer) End() {
	// when pull is canceled, allow handler to process any remaining batches if it has them.
	<-indexer.handlerCtx.Done()
}
