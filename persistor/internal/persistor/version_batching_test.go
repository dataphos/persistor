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
	"fmt"
	"testing"

	"github.com/dataphos/lib-streamproc/pkg/streamproc"
	"github.com/dataphos/persistor/internal/persistor"
)

func msgNumberToBatchNumber(t *testing.T, batchNumbers [][]int) map[int]int {
	result := map[int]int{}
	for batchNum, batch := range batchNumbers {
		for _, msgNum := range batch {
			if msgBatchNum, alreadyFound := result[msgNum]; alreadyFound {
				t.Error(fmt.Sprintf("message %d is in batches %d and %d", msgNum, msgBatchNum, batchNum))
			}
			result[msgNum] = batchNum
		}
	}

	return result
}

func presentNumbers(t *testing.T, ints []int) map[int]bool {
	present := map[int]bool{}
	for _, inputMsgNum := range ints {
		if _, alreadyFound := present[inputMsgNum]; alreadyFound {
			t.Error(fmt.Sprintf("message %d is duplicated in positions", inputMsgNum))
		} else {
			present[inputMsgNum] = true
		}
	}

	return present
}

func compareMessagePositions(t *testing.T, input, target []int) {
	if len(input) != len(target) {
		t.Error(fmt.Sprintf("incorrect number of messages in batch: %d!=%d", len(input), len(target)))
	}
	inInput, inTarget := presentNumbers(t, input), presentNumbers(t, target)

	for inputMsgNum := range inInput {
		if _, isInTarget := inTarget[inputMsgNum]; !isInTarget {
			t.Error(fmt.Sprintf("found unexpected message number %d", inputMsgNum))
		}
	}
	for targetMsgNum := range inTarget {
		if _, isInInput := inInput[targetMsgNum]; !isInInput {
			t.Error(fmt.Sprintf("missing expected message number %d", targetMsgNum))
		}
	}
}

func messagesEqual(input, target streamproc.Message) bool {
	return input.ID == target.ID
}

func compareBatchesWithExpected(t *testing.T, messages []streamproc.Message, batches [][]streamproc.Message, batchNumbers, expectedBatchNumbers [][]int) {
	if len(expectedBatchNumbers) != len(batchNumbers) {
		t.Error(fmt.Sprintf("number of matches doesn't match expected: %d!=%d", len(batchNumbers), len(expectedBatchNumbers)))
	}
	if len(batchNumbers) != len(batches) {
		t.Error(fmt.Sprintf("unequal number of batches and batch numbers: %d!=%d", len(batches), len(batchNumbers)))
	}

	msgNumToExpectedBatchPos := msgNumberToBatchNumber(t, expectedBatchNumbers)
	msgNumToBatchPos := msgNumberToBatchNumber(t, batchNumbers)
	//
	batchesCompared := map[int]bool{} // keeps track of the batches that were already compared - we can skip them

	for batchNum, batchPositions := range batchNumbers {
		messageBatch := batches[batchNum]
		for msgNumberBatch, msgNumber := range batchPositions {
			// expectedBatchNum := msgNumToExpectedBatchPos[msgNumber]
			msg := messageBatch[msgNumberBatch]
			expectedMsg := messages[msgNumber]
			if !messagesEqual(msg, expectedMsg) {
				t.Error(fmt.Sprintf("ID doesn't match for message at position %d: %s!=%s", msgNumber, msg.ID, expectedMsg.ID))
			}
		}
	}

	for msgNumber, batchNumber := range msgNumToBatchPos {
		if alreadyCompared, _ := batchesCompared[batchNumber]; alreadyCompared {
			continue
		}
		batchesCompared[batchNumber] = true

		expectedBatchNumber, expectedBatchExists := msgNumToExpectedBatchPos[msgNumber]

		if !expectedBatchExists {
			t.Error(fmt.Sprintf("found unexpected message number %d", msgNumber))
			continue
		}
		compareMessagePositions(t, batchNumbers[batchNumber], expectedBatchNumbers[expectedBatchNumber])
	}
}

func TestBatching_OneVersionKey(t *testing.T) {
	t.Parallel()
	// the list of attribute keys by which to do batching
	versionKeys := []string{"testversion"}

	// list of messages to split into batches
	msgs := []streamproc.Message{
		{ID: "0", Attributes: map[string]interface{}{"testversion": "a"}},                        // 0
		{ID: "1", Attributes: map[string]interface{}{"testversion": "a", "irrelevant": "words"}}, // 1
		{ID: "2", Attributes: map[string]interface{}{"testversion": "a", "irrelevant": "words"}}, // 2

		{ID: "3", Attributes: map[string]interface{}{"testversion": "A"}}, // 3

		{ID: "4", Attributes: map[string]interface{}{}},                      // 4
		{ID: "5", Attributes: map[string]interface{}{}},                      // 5
		{ID: "6", Attributes: map[string]interface{}{"irrelevant": "words"}}, // 6
		{ID: "7", Attributes: map[string]interface{}{"irrelevant": "words"}}, // 7

		{ID: "8", Attributes: map[string]interface{}{"testversion": "data"}},  // 8
		{ID: "9", Attributes: map[string]interface{}{"testversion": "data"}},  // 9
		{ID: "10", Attributes: map[string]interface{}{"testversion": "data"}}, // 10
	}

	expectedMsgNumbers := [][]int{
		{0, 1, 2},
		{3},
		{4, 5, 6, 7},
		{8, 9, 10},
	}

	batches, msgNumbers := persistor.BatchByVersions(msgs, versionKeys)

	compareBatchesWithExpected(t, msgs, batches, msgNumbers, expectedMsgNumbers)
}

func TestBatching_TwoVersionKeys(t *testing.T) {
	t.Parallel()
	// the list of attribute keys by which to do batching
	versionKeys := []string{"1", "2"}

	// list of messages to split into batches
	msgs := []streamproc.Message{
		{ID: "0", Attributes: map[string]interface{}{"1": "a", "2": "b"}}, // 0
		{ID: "1", Attributes: map[string]interface{}{"1": "a", "2": "b"}}, // 1
		{ID: "2", Attributes: map[string]interface{}{"1": "a", "2": "b"}}, // 2

		{ID: "3", Attributes: map[string]interface{}{"1": "A", "2": "b"}}, // 3

		{ID: "4", Attributes: map[string]interface{}{"1": "A", "2": "B"}}, // 4
		{ID: "5", Attributes: map[string]interface{}{"1": "A", "2": "B"}}, // 5

		{ID: "6", Attributes: map[string]interface{}{"1": "123", "2": "123"}}, // 6
		{ID: "7", Attributes: map[string]interface{}{"1": "123", "2": "123"}}, // 7

		{ID: "8", Attributes: map[string]interface{}{"1": "data", "2": "engineering"}},  // 8
		{ID: "9", Attributes: map[string]interface{}{"1": "data", "2": "engineering"}},  // 9
		{ID: "10", Attributes: map[string]interface{}{"1": "data", "2": "engineering"}}, // 10

		{ID: "11", Attributes: map[string]interface{}{"1": "engineering", "2": "data"}}, // 11
		{ID: "12", Attributes: map[string]interface{}{"1": "engineering", "2": "data"}}, // 12
	}

	expectedMsgNumbers := [][]int{
		{0, 1, 2},
		{3},
		{4, 5},
		{6, 7},
		{8, 9, 10},
		{11, 12},
	}

	batches, msgNumbers := persistor.BatchByVersions(msgs, versionKeys)

	compareBatchesWithExpected(t, msgs, batches, msgNumbers, expectedMsgNumbers)
}

func TestBatching_MissingVersions(t *testing.T) {
	t.Parallel()
	// the list of attribute keys by which to do batching
	versionKeys := []string{"1", "2"}

	// list of messages to split into batches
	msgs := []streamproc.Message{
		{ID: "0", Attributes: map[string]interface{}{"1": "a", "2": "b"}}, // 0
		{ID: "1", Attributes: map[string]interface{}{"1": "a", "2": "b"}}, // 1
		{ID: "2", Attributes: map[string]interface{}{"1": "a", "2": "b"}}, // 2

		{ID: "3", Attributes: map[string]interface{}{"1": "a"}}, // 3
		{ID: "4", Attributes: map[string]interface{}{"1": "a"}}, // 4

		{ID: "5", Attributes: map[string]interface{}{"1": "A"}}, // 5

		{ID: "6", Attributes: map[string]interface{}{"2": "123"}}, // 6
		{ID: "7", Attributes: map[string]interface{}{"2": "123"}}, // 7
		{ID: "8", Attributes: map[string]interface{}{"2": "123"}}, // 8

		{ID: "9", Attributes: map[string]interface{}{}},  // 9
		{ID: "10", Attributes: map[string]interface{}{}}, // 10

		{ID: "11", Attributes: map[string]interface{}{"2": "123"}}, // 11

		{ID: "12", Attributes: map[string]interface{}{}}, // 12
		{ID: "13", Attributes: map[string]interface{}{}}, // 13
	}

	expectedMsgNumbers := [][]int{
		{0, 1, 2},
		{3, 4},
		{5},
		{6, 7, 8, 11},
		{9, 10, 12, 13},
	}

	batches, msgNumbers := persistor.BatchByVersions(msgs, versionKeys)

	compareBatchesWithExpected(t, msgs, batches, msgNumbers, expectedMsgNumbers)
}
