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

package persistor_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-streamproc/pkg/streamproc"
	"github.com/dataphos/persistor/internal/common"
	"github.com/dataphos/persistor/internal/common/log"
	"github.com/dataphos/persistor/internal/config"
	"github.com/dataphos/persistor/internal/persistor"
	"github.com/dataphos/persistor/internal/writer"
)

// getIDsBrkr returns ids of the given messages
func getIDsBrkr(msgs []broker.Message) map[string]struct{} {
	ids := map[string]struct{}{}
	for _, msg := range msgs {
		ids[msg.ID] = struct{}{}
	}
	return ids
}

// getIDsBrkr returns ids of the given messages
func getIDsStrprc(msgs []streamproc.Message) map[string]struct{} {
	ids := map[string]struct{}{}
	for _, msg := range msgs {
		ids[msg.ID] = struct{}{}
	}
	return ids
}

// checkStorageVersioning compares each blob name with the expected value based on the attributes of the messages inside it
func checkStorageVersioning(blobs map[string][]streamproc.Message, mask []persistor.MaskMember) []error {
	var errs []error

	for blobName, messages := range blobs {
		pathMembers := strings.Split(blobName, "/")
		pathMembers = pathMembers[:len(pathMembers)-1]
		if len(pathMembers) != len(mask) {
			errs = append(errs, fmt.Errorf("blob %s has different number of members than mask", blobName))
		}
		for iPath, pathMember := range pathMembers {
			if iPath >= len(mask) {
				break
			}
			maskMember := mask[iPath]
			if !maskMember.FromMessageAttributes {
				if maskMember.Key == "year" || maskMember.Key == "month" || maskMember.Key == "day" || maskMember.Key == "hour" {
					_, errNum := strconv.Atoi(pathMember)
					if errNum != nil {
						errs = append(errs, fmt.Errorf("blob path for %s doesn't contain number: %s", maskMember.Key, pathMember))
					}
				}
			} else {
				for _, msg := range messages {
					if value, hasValue := msg.Attributes[maskMember.Key]; !hasValue {
						if pathMember != "unknown" {
							errs = append(errs, fmt.Errorf("message %s has no attribute %s but value in path is %s", msg.ID, maskMember.Key, pathMember))
						}
					} else if value != pathMember {
						errs = append(errs, fmt.Errorf("message %s has %s=%s but value in path is %s", msg.ID, maskMember.Key, value, pathMember))
					}
				}
			}
		}
	}

	return errs
}

type messageStatus struct {
	Sent, Acked, Nacked, Stored, FailedStorage, Indexed, DeadLettered bool
}

// getStatuses returns the status of each message, accessible by its ID
func getStatuses(sentIDs, ackedIDs, nackedIDs, writtenIDs, writerFailedIDs, indexedIDs, deadLetterData map[string]struct{}) map[string]*messageStatus {
	statuses := map[string]*messageStatus{}
	var status *messageStatus
	var noted bool
	for id := range sentIDs {
		if status, noted = statuses[id]; !noted {
			status = &messageStatus{}
			statuses[id] = status
		}
		status.Sent = true
	}
	for id := range ackedIDs {
		if status, noted = statuses[id]; !noted {
			status = &messageStatus{}
			statuses[id] = status
		}
		status.Acked = true
	}
	for id := range nackedIDs {
		if status, noted = statuses[id]; !noted {
			status = &messageStatus{}
			statuses[id] = status
		}
		status.Nacked = true
	}
	for id := range writtenIDs {
		if status, noted = statuses[id]; !noted {
			status = &messageStatus{}
			statuses[id] = status
		}
		status.Stored = true
	}
	for id := range writerFailedIDs {
		if status, noted = statuses[id]; !noted {
			status = &messageStatus{}
			statuses[id] = status
		}
		status.FailedStorage = true
	}
	for id := range indexedIDs {
		if status, noted = statuses[id]; !noted {
			status = &messageStatus{}
			statuses[id] = status
		}
		status.Indexed = true
	}
	for id := range deadLetterData {
		if status, noted = statuses[id]; !noted {
			status = &messageStatus{}
			statuses[id] = status
		}
		status.DeadLettered = true
	}

	return statuses
}

// extracts ids from the payloads on indexer topic
func idsFromIdxData(idxPayloads map[string]struct{}) (unparsed, noID []string, indexedIDs map[string]struct{}) {
	indexedIDs = map[string]struct{}{}
	for idxMessageStr := range idxPayloads {
		var idxData common.Data
		jsonErr := json.Unmarshal([]byte(idxMessageStr), &idxData)
		if jsonErr != nil {
			unparsed = append(unparsed, idxMessageStr)
			continue
		}
		indexedIDs[fmt.Sprintf("%v", idxData.BrokerMsgID)] = struct{}{}
	}
	return
}

// checkIndexerData compares the IDs of messages found on storage with those on the indexer topic
func checkIndexerData(writtenIDs, idxData map[string]struct{}, idxFailed []broker.OutboundMessage) (errs []error, indexedIDs map[string]struct{}) {
	failedData := map[string]struct{}{}
	for _, msg := range idxFailed {
		failedData[string(msg.Data)] = struct{}{}
	}

	var unindexed, extraIndexed []string
	unparsed, noID, indexedIDs := idsFromIdxData(idxData)
	if len(noID) > 0 {
		errs = append(errs, fmt.Errorf("%d messages on Indexer topic have no ID", len(noID)))
	}
	unparsedFailed, noIDFailed, indexedIDsFailed := idsFromIdxData(failedData)
	if len(noIDFailed) > 0 {
		errs = append(errs, fmt.Errorf("%d failed messages on Indexer topic have no ID", len(noIDFailed)))
	}
	for sentID := range writtenIDs {
		_, isIDx := indexedIDs[sentID]
		if isIDx {
			continue
		}
		_, failedIDx := indexedIDsFailed[sentID]
		if failedIDx {
			continue
		}
		unindexed = append(unindexed, sentID)
	}
	for idxID := range indexedIDs {
		_, isSent := writtenIDs[idxID]
		if !isSent {
			extraIndexed = append(unindexed, idxID)
		}
	}
	if len(unparsed) > 0 {
		errs = append(errs, fmt.Errorf("%d Indexer topic messages cannot be parsed: %v", len(unparsed), unparsed))
	}
	if len(unparsedFailed) > 0 {
		errs = append(errs, fmt.Errorf("%d failed Indexer topic messages cannot be parsed: %v", len(unparsedFailed), unparsedFailed))
	}
	if len(unindexed) > 0 {
		errs = append(errs, fmt.Errorf("%d message IDs missing from Indexer topic: %v", len(unindexed), unindexed))
	}
	if len(extraIndexed) > 0 {
		errs = append(errs, fmt.Errorf("%d unexpected IDs found on Indexer topic: %v", len(extraIndexed), extraIndexed))
	}
	return errs, indexedIDs
}

// checkMessages confirms that ack-ed messages are correctly stored and indexed (if indexer is enabled), or sent to dead letter
func checkMessages(sent []broker.Message, expectFailures, idxEnabled bool, ackedIDs, nackedIDs, deadLetterData, idxData map[string]struct{}, idxFailed []broker.OutboundMessage, blobs map[string][]streamproc.Message, failedBatches map[string][]streamproc.Message) []error {
	var errs []error

	sentIDs := getIDsBrkr(sent)
	// writtenIDs are IDs found on storage
	writtenIDs := map[string]struct{}{}
	// failedIDs are IDs which the writer failed to write (if any)
	failedIDs := map[string]struct{}{}
	for _, blob := range blobs {
		for id := range getIDsStrprc(blob) {
			writtenIDs[id] = struct{}{}
		}
	}
	for _, blob := range failedBatches {
		for id := range getIDsStrprc(blob) {
			failedIDs[id] = struct{}{}
		}
	}

	// indexedIDs are the IDs found on the indexer topic
	indexedIDs := map[string]struct{}{}
	if idxEnabled {
		var idxErrs []error
		idxErrs, indexedIDs = checkIndexerData(writtenIDs, idxData, idxFailed)
		errs = append(errs, idxErrs...)
	}

	statuses := getStatuses(sentIDs, ackedIDs, nackedIDs, writtenIDs, failedIDs, indexedIDs, deadLetterData)

	var unexpected []string
	var notProcessedButAcked, processedButNotAcked, deadLetteredButNotAcked []string
	var processedButDL []string

	for msgID, msgStatus := range statuses {
		processed := msgStatus.Stored && (msgStatus.Indexed || !idxEnabled)
		if !msgStatus.Sent {
			unexpected = append(unexpected, msgID)
			continue
		}
		if !processed && !msgStatus.DeadLettered && msgStatus.Acked {
			notProcessedButAcked = append(notProcessedButAcked, msgID)
		}
		if (processed || msgStatus.DeadLettered) && !msgStatus.Acked {
			processedButNotAcked = append(processedButNotAcked, msgID)
		}
		if msgStatus.DeadLettered && !msgStatus.Acked {
			deadLetteredButNotAcked = append(deadLetteredButNotAcked, msgID)
		}
		if msgStatus.Stored && msgStatus.Indexed && msgStatus.DeadLettered {
			processedButDL = append(processedButDL, msgID)
		}
	}

	if !expectFailures && len(failedIDs) > 0 {
		errs = append(errs, fmt.Errorf("writer failed on %d messages", len(failedIDs)))
	}
	if len(unexpected) > 0 {
		errs = append(errs, fmt.Errorf("found %d messages which were not sent: %v", len(unexpected), unexpected))
	}
	if len(notProcessedButAcked) > 0 {
		errs = append(errs, fmt.Errorf("%d messages were not processed but were acked: %v", len(notProcessedButAcked), notProcessedButAcked))
	}
	if len(processedButNotAcked) > 0 {
		errs = append(errs, fmt.Errorf("%d messages were processed but not acked: %v", len(processedButNotAcked), processedButNotAcked))
	}
	if len(deadLetteredButNotAcked) > 0 {
		errs = append(errs, fmt.Errorf("%d messages were sent to dead letter but not acked: %v", len(deadLetteredButNotAcked), deadLetteredButNotAcked))
	}
	if len(processedButDL) > 0 {
		errs = append(errs, fmt.Errorf("%d messages were processed but sent to dead letter: %v", len(processedButDL), processedButDL))
	}
	return errs
}

func DefineRunOptionsForTest() []streamproc.RunOption {
	runOptions := []streamproc.RunOption{
		streamproc.WithErrThreshold(50),
		streamproc.WithErrInterval(1 * time.Minute),
		streamproc.WithNumRetires(0),
		streamproc.OnPullErr(func(err error) streamproc.FlowControl {
			log.Info("Error while pulling messages: " + err.Error())
			return streamproc.FlowControlStop
		}),
		streamproc.OnProcessErr(func(err error) streamproc.FlowControl {
			log.Info(fmt.Sprintf("Error occurred during processing: %v\n", err.Error()))
			return streamproc.FlowControlContinue
		}),
		streamproc.OnUnrecoverable(func(err error) streamproc.FlowControl {
			log.Errorw("Unrecoverable error encountered", common.ProcessingError, log.F{"error": err})
			return streamproc.FlowControlStop
		}),
		streamproc.OnThresholdReached(func(err error, count, threshold int64) streamproc.FlowControl {
			log.Info(fmt.Sprintf("Error threshold reached (%d >= %d)", count, threshold))
			return streamproc.FlowControlStop
		}),
	}

	return runOptions
}

// starts persistor with a simulated topic that produces messages. Returns the topic and any data on the indexer and dead letter topics.
func startSourceAndExecutor(ctx context.Context, handler *persistor.Persistor, batchSize, numMsgs int, dlTopic, idxTopic *persistor.FakeTopic, attributeKeys []string, attributeValues [][]string, keyMissingProbs []float32) (topic *persistor.FakeTopic, dlData, idxData map[string]struct{}) {
	runCtx, canc := context.WithCancel(ctx)
	defer canc()

	topic = persistor.NewMockTopic(batchSize, 0)
	defer topic.Close()
	rec := &persistor.MockBatchReceiver{
		Timeout:      time.Second,
		MessageChan:  topic.MessageChan,
		MaxBatchSize: batchSize,
	}

	eg := errgroup.Group{}
	if dlTopic != nil {
		eg.Go(func() error {
			dlData, _ = dlTopic.Received(runCtx)

			return nil
		})
	}
	if idxTopic != nil {
		eg.Go(func() error {
			idxData, _ = idxTopic.Received(runCtx)

			return nil
		})
	}

	go func() {
		exec := streamproc.NewBatchedReceiverExecutor(handler)
		runError := exec.Run(runCtx, rec, DefineRunOptionsForTest()...)
		if runError != nil {
			log.Warn("executor run error: " + runError.Error())
		}
	}()
	topic.PublishRandomMessages(runCtx, numMsgs, attributeKeys, attributeValues, keyMissingProbs)

	time.Sleep(time.Second * 5)
	canc()
	_ = eg.Wait()

	return topic, dlData, idxData
}

// mockPersistorPipelineTest initializes persistor with a mock receiver, writer and optional indexing and dead letter sender and checks the messages end up where expected.
func mockPersistorPipelineTest(conf config.PersistorConfig, numMsgs, batchSize int, attributeKeys []string, attributeValues [][]string, keyMissingProbs []float32,
	writerShouldFail func(msgs []streamproc.Message) bool, idxSenderShouldFail func(msg broker.OutboundMessage) bool) []error {

	var errs []error

	mockWriter := &writer.FakeBlobWriter{
		Blobs:         map[string][]streamproc.Message{},
		BlobMutex:     sync.Mutex{},
		ShouldFail:    writerShouldFail,
		FailedBatches: map[string][]streamproc.Message{},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	storage, err := persistor.NewStorageProperties(conf.Storage)
	if err != nil {
		errs = append(errs, err)
	}
	handler := &persistor.Persistor{
		Writer:           mockWriter,
		Storage:          storage,
		IndexerEnabled:   conf.IndexerEnabled,
		DeadLetterActive: conf.DeadLetterEnabled,
		HandlerCtx:       ctx,
		Cancel:           cancel,
	}
	var maskError error
	handler.Storage.Mask, handler.Storage.VersionKeys, maskError = persistor.GenerateMaskValuesFromMaskString(conf.Storage.Mask)

	dlTopic, idxTopic := persistor.NewMockTopic(1000, 0), persistor.NewMockTopic(1000, 0)
	mockIdx, mockDL := &persistor.PublisherFakeTopic{Topic: idxTopic, ShouldFail: idxSenderShouldFail}, &persistor.PublisherFakeTopic{Topic: dlTopic}
	if conf.IndexerEnabled {
		handler.IndexerTopic = mockIdx
	}
	if conf.DeadLetterEnabled {
		handler.DeadLetterTopic = mockDL
	}

	if maskError != nil {
		errs = append(errs, maskError)
	}

	// start the pull
	topic, deadLetterData, idxData := startSourceAndExecutor(ctx, handler, batchSize, numMsgs, dlTopic, idxTopic, attributeKeys, attributeValues, keyMissingProbs)

	// pull is done, batches should be either processed or sent to dead letter or nack-ed
	errs = append(errs, checkMessages(topic.AllSent, writerShouldFail != nil || idxSenderShouldFail != nil, conf.IndexerEnabled, topic.AckedIDs, topic.NackedIDs, deadLetterData, idxData, mockIdx.FailedMessages, mockWriter.Blobs, mockWriter.FailedBatches)...)
	errs = append(errs, checkStorageVersioning(mockWriter.Blobs, handler.Storage.Mask)...)

	return errs
}

// grade logs the accumulated errors for a test
func grade(t *testing.T, errs []error) {
	if len(errs) > 0 {
		for _, err := range errs {
			t.Log(err)
		}
		t.Fail()
	}
}

// compares the messages received from persistor topic to those found on the storage
func TestPersistorHandler(t *testing.T) {
	t.Parallel()
	var numMsgs, batchSize = 200, 10
	conf := config.PersistorConfig{
		Storage: config.StorageConfig{
			Type:             config.WriterGCS,
			Prefix:           "msg",
			MsgExtension:     "test",
			Mask:             "year",
			CustomValues:     "",
			Destination:      "mock-bucket",
			StorageAccountID: "",
			TopicID:          "mock-topic",
		},
	}
	errs := mockPersistorPipelineTest(conf, numMsgs, batchSize, nil, nil, nil, nil, nil)
	grade(t, errs)
}

// test that the metadata is correctly sent to indexer
func TestPersistorHandler_IdxSender(t *testing.T) {
	t.Parallel()
	var numMsgs, batchSize = 200, 10
	conf := config.PersistorConfig{
		Storage: config.StorageConfig{
			Type:             config.WriterGCS,
			Prefix:           "msg",
			MsgExtension:     "test",
			Mask:             "year",
			CustomValues:     "",
			Destination:      "mock-bucket",
			StorageAccountID: "",
			TopicID:          "mock-topic",
		},
		IndexerEnabled:    true,
		DeadLetterEnabled: false,
	}
	errs := mockPersistorPipelineTest(conf, numMsgs, batchSize, nil, nil, nil, nil, nil)
	grade(t, errs)
}

// check correct batching on storage based on message attributes
func TestPersistorHandler_Versions(t *testing.T) {
	t.Parallel()
	var numMsgs, batchSize = 500, 100
	conf := config.PersistorConfig{
		Storage: config.StorageConfig{
			Type:             config.WriterGCS,
			Prefix:           "msg",
			MsgExtension:     "test",
			Mask:             "year/month/{color}/{day}/{shape}",
			CustomValues:     "",
			Destination:      "mock-bucket",
			StorageAccountID: "",
			TopicID:          "mock-topic",
		},
	}
	attributeKeys := []string{"color", "spin", "shape"}
	attributeValues := [][]string{{"blue", "red", "yellow"}, {"left", "right"}, {"circle", "square", "triangle", "star"}}

	errs := mockPersistorPipelineTest(conf, numMsgs, batchSize, attributeKeys, attributeValues, nil, nil, nil)
	grade(t, errs)
}

// test batching with some version keys missing from messages
func TestPersistorHandler_VersionsWithUnknown(t *testing.T) {
	t.Parallel()
	var numMsgs, batchSize = 500, 100
	conf := config.PersistorConfig{
		Storage: config.StorageConfig{
			Type:             config.WriterGCS,
			Prefix:           "msg",
			MsgExtension:     "test",
			Mask:             "year/month/{color}/{day}/{shape}",
			CustomValues:     "",
			Destination:      "mock-bucket",
			StorageAccountID: "",
			TopicID:          "mock-topic",
		},
	}
	attributeKeys := []string{"color", "spin", "shape"}
	attributeValues := [][]string{{"blue", "red", "yellow"}, {"left", "right"}, {"circle", "square", "triangle", "star"}}
	// each key has 50% probability of not being defined in a random message
	keyMissingProbs := []float32{0.5, 0.5, 0.5}

	errs := mockPersistorPipelineTest(conf, numMsgs, batchSize, attributeKeys, attributeValues, keyMissingProbs, nil, nil)
	grade(t, errs)
}

// simulate errors when sending batches to storage
func TestPersistorHandler_WriterFail_NoDeadletter(t *testing.T) {
	t.Parallel()
	var numMsgs, batchSize = 400, 100
	conf := config.PersistorConfig{
		Storage: config.StorageConfig{
			Type:             config.WriterGCS,
			Prefix:           "msg",
			MsgExtension:     "test",
			Mask:             "year/month/{color}/{day}/{shape}",
			CustomValues:     "",
			Destination:      "mock-bucket",
			StorageAccountID: "",
			TopicID:          "mock-topic",
		},
	}
	// first key has 50% probability of not being defined in a random message
	keyMissingProbs := []float32{0.5, 0, 0}

	attributeKeys := []string{"color", "spin", "shape"}
	attributeValues := [][]string{{"blue", "red", "yellow"}, {"left", "right"}, {"circle", "square", "triangle", "star"}}

	failNext := true
	// simulate writer that fails on every other batch
	shouldFail := func(msgs []streamproc.Message) bool {
		failNext = !failNext

		return failNext
	}
	errs := mockPersistorPipelineTest(conf, numMsgs, batchSize, attributeKeys, attributeValues, keyMissingProbs, shouldFail, nil)
	grade(t, errs)
}

// simulate errors when sending batches to storage
func TestPersistorHandler_WriterFail_DL(t *testing.T) {
	t.Parallel()
	var numMsgs, batchSize = 400, 100
	conf := config.PersistorConfig{
		Storage: config.StorageConfig{
			Type:             config.WriterGCS,
			Prefix:           "msg",
			MsgExtension:     "test",
			Mask:             "year/month/{color}/{day}/{shape}",
			CustomValues:     "",
			Destination:      "mock-bucket",
			StorageAccountID: "",
			TopicID:          "mock-topic",
		},
		IndexerEnabled:    false,
		DeadLetterEnabled: true,
	}
	keyMissingProbs := []float32{0.5, 0, 0}

	attributeKeys := []string{"color", "spin", "shape"}
	attributeValues := [][]string{{"blue", "red", "yellow"}, {"left", "right"}, {"circle", "square", "triangle", "star"}}

	failNext := true
	// simulate writer that fails on every other batch
	writerShouldFail := func(msgs []streamproc.Message) bool {
		failNext = !failNext

		return failNext
	}
	errs := mockPersistorPipelineTest(conf, numMsgs, batchSize, attributeKeys, attributeValues, keyMissingProbs, writerShouldFail, nil)
	grade(t, errs)
}

// simulate errors when sending indexer data and check that the failed messages are nack-ed
func TestPersistorHandler_IdxSenderFail_NoDeadLetter(t *testing.T) {
	t.Parallel()
	var numMsgs, batchSize = 400, 100
	conf := config.PersistorConfig{
		Storage: config.StorageConfig{
			Type:             config.WriterGCS,
			Prefix:           "msg",
			MsgExtension:     "test",
			Mask:             "year/month/{color}/{day}/{shape}",
			CustomValues:     "",
			Destination:      "mock-bucket",
			StorageAccountID: "",
			TopicID:          "mock-topic",
		},
		IndexerEnabled:    true,
		DeadLetterEnabled: false,
	}
	keyMissingProbs := []float32{0.5, 0.5, 0.5}

	attributeKeys := []string{"color", "spin", "shape"}
	attributeValues := [][]string{{"blue", "red", "yellow"}, {"left", "right"}, {"circle", "square", "triangle", "star"}}

	// simulate sender that fails on every other batch
	idxSenderShouldFail := func(_ broker.OutboundMessage) bool {
		return true
	}
	errs := mockPersistorPipelineTest(conf, numMsgs, batchSize, attributeKeys, attributeValues, keyMissingProbs, nil, idxSenderShouldFail)
	grade(t, errs)
}

// simulate errors when sending indexer data and check that the failed messages are sent to dead letter
func TestPersistorHandler_IdxSenderFail_DL(t *testing.T) {
	t.Parallel()
	var numMsgs, batchSize = 400, 100
	conf := config.PersistorConfig{
		Storage: config.StorageConfig{
			Type:             config.WriterGCS,
			Prefix:           "msg",
			MsgExtension:     "test",
			Mask:             "year/month/{color}/{day}/{shape}",
			CustomValues:     "",
			Destination:      "mock-bucket",
			StorageAccountID: "",
			TopicID:          "mock-topic",
		},
		IndexerEnabled:    true,
		DeadLetterEnabled: true,
	}
	keyMissingProbs := []float32{0.5, 0.5, 0.5}

	attributeKeys := []string{"color", "spin", "shape"}
	attributeValues := [][]string{{"blue", "red", "yellow"}, {"left", "right"}, {"circle", "square", "triangle", "star"}}

	// simulate sender that fails on every other batch
	idxSenderShouldFail := func(_ broker.OutboundMessage) bool {
		return true
	}
	errs := mockPersistorPipelineTest(conf, numMsgs, batchSize, attributeKeys, attributeValues, keyMissingProbs, nil, idxSenderShouldFail)
	grade(t, errs)
}
