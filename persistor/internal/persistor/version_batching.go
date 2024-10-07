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
	"fmt"

	"github.com/dataphos/lib-streamproc/pkg/streamproc"
	"github.com/dataphos/persistor/internal/common"
	"github.com/dataphos/persistor/internal/common/log"
)

// addMessageToVersionMap puts the message into one of the batches or makes a new batch for it.
// The index of the batch in the batches slice is stored in the versionMap recursively (the first version points to a map of second versions and so on)
// msgNumber is appended to the integer slice at the same index in the messagePositions.
// for example, if there are two versions and versionMap["v1"]["v2"] = 0 then all messages with versions (v1, v2) need to end up in the first mini-batch in the batches slice
func addMessageToVersionMap(versionMap map[string]interface{}, versions []string, msg streamproc.Message, batches *[]*[]streamproc.Message, msgNumber int, messagePositions *[]*[]int) {
	innerMap := versionMap
	numVersions := len(versions)
	var castOk bool

	for keyPos, version := range versions {
		mapOrBatchNumber, ok := innerMap[version]

		if !ok {
			// if the version doesn't already exist in the map, we need to add the message to a new mini-batch.
			if keyPos == numVersions-1 {
				// if this is the last version key to batch by, the mini-batch is made and its position is stored in the map.
				batch := []streamproc.Message{msg}
				positionsInBatch := []int{msgNumber}

				*batches = append(*batches, &batch)
				*messagePositions = append(*messagePositions, &positionsInBatch)

				innerMap[version] = len(*batches) - 1
			} else {
				// if there are more version keys after this one, proceed to the last to be able to make a mini-batch.
				newMap := map[string]interface{}{}

				innerMap[version] = newMap
				innerMap = newMap
			}
		} else {
			// a mini-batch with the same version set already exists, add the message to it.
			if batchNum, ok := mapOrBatchNumber.(int); ok {
				batch, positionsInBatch := (*batches)[batchNum], (*messagePositions)[batchNum]

				*batch = append(*batch, msg)
				*positionsInBatch = append(*positionsInBatch, msgNumber)
			} else {
				innerMap, castOk = mapOrBatchNumber.(map[string]interface{})
				if !castOk {
					log.Fatal(fmt.Sprintf("Unexpected internal batching error: was expecting a map of versions for the version at position %d", keyPos), common.ProcessError) // can't happen but just in case.
				}
			}
		}
	}
}

// BatchByVersions splits the slice of messages into batches according to multiple message versions (values in message attributes that correspond to versionKeys).
// it returns the batches and a slice of the same size which contains, for each batch, the message positions from the input slice.
func BatchByVersions(messages []streamproc.Message, versionKeys []string) ([][]streamproc.Message, [][]int) { //nolint:gocritic // not naming these returns for now.
	// nolint:prealloc // would prealloc these slices to a specific length, but we don't know in advance how many batches there will be and besides, that's not a huge problem.
	var batchPointers []*[]streamproc.Message // slice of pointers to message batches where each batch has the same versions.
	// nolint:prealloc // same as above.
	var msgNumberPointers []*[]int // slice of pointers to lists of original message positions in each batch.

	versionMap := map[string]interface{}{}

	for msgNum, msg := range messages {
		versions := versionsFromVersionKeys(msg.Attributes, versionKeys)
		addMessageToVersionMap(versionMap, versions, msg, &batchPointers, msgNum, &msgNumberPointers)
	}

	// nolint:prealloc // same as above.
	var batches [][]streamproc.Message // slice of pointers to message batches where each batch has the same versions.
	// nolint:prealloc // same as above.
	var msgNumbers [][]int // slice of pointers to lists of original message positions in each batch.

	for _, batch := range batchPointers {
		batches = append(batches, *batch)
	}

	for _, batchNumbers := range msgNumberPointers {
		msgNumbers = append(msgNumbers, *batchNumbers)
	}

	return batches, msgNumbers
}

// versionsFromVersionKeys returns the versions from the attributes map in the same order in which the keys are given.
func versionsFromVersionKeys(attributes map[string]interface{}, versionKeys []string) []string {
	versions := make([]string, len(versionKeys))

	for iKey, key := range versionKeys {
		if version, exists := attributes[key]; !exists {
			versions[iKey] = UnknownVersion
		} else {
			versions[iKey] = fmt.Sprintf("%s", version)
		}
	}

	return versions
}
