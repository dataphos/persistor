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
	"time"

	"github.com/dataphos/persistor-resubmitter-api/common/util"
)

type Mock struct {
	getAllResponse           interface{}
	getAllInIntervalResponse interface{}
	getQueriedResponse       interface{}
	BatchIntervals           []BatchInterval
}

type BatchInterval struct {
	Start int
	End   int
}

func (i *Mock) SetGetAllResponse(messages interface{}) {
	i.getAllResponse = messages
}

func (i *Mock) SetGetAllInIntervalResponse(messages interface{}) {
	i.getAllInIntervalResponse = messages
}

func (i *Mock) SetGetQueriedResponse(messages interface{}) {
	i.getQueriedResponse = messages
}

func (i *Mock) GetAll(_ string, _ []string) ([]Message, error) {
	switch response := i.getAllResponse.(type) {
	case []Message:
		return response, nil
	case error:
		return nil, response
	}

	return nil, nil
}

func (i *Mock) Get(_ string) ([]Message, error) {
	switch response := i.getAllResponse.(type) {
	case []Message:
		var messages []Message
		messages = append(messages, response[0])
		return messages, nil
	case error:
		return nil, response
	}

	return nil, nil
}

func (i *Mock) GetAllInInterval(_ string, _ string, _, _ time.Time, limit, offset int) (*IntervalQueryResponse, error) {
	switch response := i.getAllInIntervalResponse.(type) {
	case []Message:
		messages := response
		totalCount := len(messages)

		messageEnd := offset + limit
		if messageEnd > len(messages) {
			messageEnd = len(messages)
		}

		messages = messages[offset:messageEnd]

		i.BatchIntervals = append(i.BatchIntervals, BatchInterval{offset, messageEnd})

		return &IntervalQueryResponse{
			TotalCount:    totalCount,
			ReturnedCount: len(messages),
			Limit:         limit,
			Offset:        offset,
			Messages:      messages,
		}, nil
	case error:
		return nil, response
	}

	return nil, nil
}

func (i *Mock) GetQueried(_ string, _ util.QueryRequestBody, limit, offset int) (*IntervalQueryResponse, error) {
	switch response := i.getQueriedResponse.(type) {
	case []Message:
		messages := response
		totalCount := len(messages)

		messageEnd := offset + limit
		if messageEnd > len(messages) {
			messageEnd = len(messages)
		}

		messages = messages[offset:messageEnd]

		i.BatchIntervals = append(i.BatchIntervals, BatchInterval{offset, messageEnd})

		return &IntervalQueryResponse{
			TotalCount:    totalCount,
			ReturnedCount: len(messages),
			Limit:         limit,
			Offset:        offset,
			Messages:      messages,
		}, nil
	case error:
		return nil, response
	}

	return nil, nil
}
