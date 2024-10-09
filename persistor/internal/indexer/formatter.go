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
	"fmt"
	"time"

	"github.com/dataphos/persistor/internal/common"
	"github.com/dataphos/persistor/internal/common/log"
)

const layout = "2006-01-02 15:04:05.99999999"

type FormattedIndexerData struct {
	common.CommonData       `bson:",inline,omitempty"`
	common.Location         `bson:",inline,omitempty"`
	common.ProperTimestamps `bson:",inline"`
	common.DefinedData      `bson:",inline"`
}

// FormatData takes the batch of received messages and transforms them into suitable format.
// The main purpose is to transform all message data into appropriate data types that cannot be
// automatically detected and transformed by Mongo.
func FormatData(record common.Data) (FormattedIndexerData, error) {
	var (
		publishTS time.Time
		formData  FormattedIndexerData
		ingestTS  time.Time
		err       error
	)

	idxIngestionTime := time.Now().UTC()

	// Assuming the timestamps are in the 'yyyy-mm-dd hh-mm-ss.mmmmmm' format (milliseconds are optional).
	publishTS, err = ParseTimestamp(record.Timestamp.PublishTime)
	if err != nil {
		return formData, err
	}

	ingestTS, err = ParseTimestamp(record.Timestamp.IngestionTime)
	if err != nil {
		return formData, err
	}

	return FormattedIndexerData{
		CommonData: record.CommonData,
		ProperTimestamps: common.ProperTimestamps{
			PublishTime:          publishTS,
			IngestionTime:        ingestTS,
			IndexerIngestionTime: idxIngestionTime,
		},
		Location: record.Location,
		DefinedData: common.DefinedData{
			ConfirmationFlag: false,
		},
	}, nil
}

func ParseTimestamp(str string) (time.Time, error) {
	// Parsing timestamps in Go is a bit tricky, when creating the layout pay attention to the parts Goland offers.
	// If your just write random date, Go interpreter can, for example, switch day and month and Parse returns unwanted result.
	// The least significant numbers are set to nines simply because it allows us to have variable number of digits in
	// millisecond part.
	timestamp, err := time.Parse(layout, str)
	if err != nil {
		log.Warnw("Failed to parse timestamps!", log.F{log.ErrorFieldKey: err.Error()})

		err = fmt.Errorf("parsing timestamp: %w", err)
	}

	return timestamp, err
}
