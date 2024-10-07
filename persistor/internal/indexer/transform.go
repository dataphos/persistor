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

package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"

	batchproc "github.com/dataphos/lib-batchproc/pkg/batchproc"
	"github.com/dataphos/lib-streamproc/pkg/streamproc"
	"github.com/dataphos/persistor/internal/common"
)

// ObserveError searches for the error message in the map of errors that  already occurred and increments
// the numFailed in the found ProcError. If the error message is not found, it makes a new one with count 1.
func ObserveError(err error, errCode int, errorsMap map[string]*common.ProcError) {
	errMsg := err.Error()
	errStore, found := errorsMap[errMsg]

	if !found {
		errStore = common.NewProcessingError(1, err, errCode)
		errorsMap[errMsg] = errStore
	} else {
		errStore.NumFailed++
	}
}

// Validate validates the received indexer data.
func Validate(validate *validator.Validate, idxData common.Data) error {
	err := validate.Struct(idxData)
	if err != nil {
		return fmt.Errorf("error in data validation: %w", err)
	}

	return nil
}

func TransformMessage(msg streamproc.Message, val *validator.Validate) (loadedData interface{}, err error) {
	var indexData common.Data

	decoder := json.NewDecoder(strings.NewReader(string(msg.Data)))
	decoder.DisallowUnknownFields()

	err = decoder.Decode(&indexData)
	if err != nil {
		err = fmt.Errorf("error while unmarshalling message: %w", err)

		return
	}

	err = Validate(val, indexData)
	if err != nil {
		err = fmt.Errorf("indexer data transformation: %w", err)

		return
	}

	loadedData, err = FormatData(indexData)
	if err != nil {
		err = fmt.Errorf("indexer data transformation: %w", err)

		return
	}

	return
}

// TransformToIndexerData transforms json of n messages into array of Messages succMsgs, loadedData, procErr, parsedPositions.
func (indexer *Indexer) TransformToIndexerData(ctx context.Context, msgs ...streamproc.Message) (goodMsgs []streamproc.Message, failedMsgs []streamproc.Message, loadedDataInterface []interface{}, procError *common.MessageBatchError, parsedPositions []int) {
	records := make([]interface{}, len(msgs))
	errors := make([]error, len(msgs))

	// here we can accumulate errors so that they aren't logged for each message separately.
	uniqueErrors := map[string]*common.ProcError{}

	// parse the data into records, storing the record for each message
	// into its position in the records slice and an error if necessary.
	_ = batchproc.Parallel(ctx, len(msgs), func(ctx context.Context, start, end int) error {
		for i := start; i < end; i++ {
			records[i], errors[i] = TransformMessage(msgs[i], indexer.validate) //nolint:contextcheck // no need.
		}

		return nil
	})

	// iterate over the records slice.
	for iRec, record := range records {
		err := errors[iRec]
		// if no error, append the record to the return slice.
		if err == nil {
			loadedDataInterface = append(loadedDataInterface, record)
			goodMsgs = append(goodMsgs, msgs[iRec])
			parsedPositions = append(parsedPositions, iRec)
		} else {
			failedMsgs = append(failedMsgs, msgs[iRec])
			ObserveError(err, common.DataLoadError, uniqueErrors)
		}
	}

	var transformError *common.MessageBatchError
	if len(failedMsgs) > 0 {
		transformError = &common.MessageBatchError{}
		for _, err := range uniqueErrors {
			transformError.AddErr(err)
		}
	}

	return goodMsgs, failedMsgs, loadedDataInterface, transformError, parsedPositions
}
