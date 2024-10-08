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
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/dataphos/persistor-indexer-api/common/log"
	"github.com/dataphos/persistor-indexer-api/lib/repo"
)

const (
	OnBadRequestMessage = "bad request"
	OnFailureMessage    = "request failed"
)

type paginationQueryParams struct {
	limit  int
	offset int
}

var defaultPaginationQueryParams = paginationQueryParams{
	limit:  20,
	offset: 0,
}

const (
	limitParam  = "limit"
	offsetParam = "offset"
)

func extractPaginationQueryParams(context *gin.Context) paginationQueryParams {
	params := defaultPaginationQueryParams

	limit, _ := strconv.Atoi(context.Query(limitParam))
	if limit > 0 {
		params.limit = limit
	} else {
		log.Info(log.UsingDefaultParameterValue(limitParam, strconv.Itoa(params.limit)))
	}

	offset, _ := strconv.Atoi(context.Query(offsetParam))
	if offset >= 0 {
		params.offset = offset
	} else {
		log.Info(log.UsingDefaultParameterValue(offsetParam, strconv.Itoa(params.offset)))
	}

	return params
}

type intervalQueryParams struct {
	to   time.Time
	from time.Time
}

const (
	toParam    = "to"
	fromParam  = "from"
	dateFormat = "2006-01-02T15:04:05.99999999Z"
)

func extractIntervalQueryParams(context *gin.Context) (*intervalQueryParams, error) {
	var (
		to   time.Time //nolint:varnamelen // fine length
		from time.Time
		err  error
	)

	lbString := context.Query(fromParam)
	if len(lbString) == 0 {
		to = time.Time{}
		log.Info(log.UsingDefaultParameterValue(fromParam, to.Format(dateFormat)))
	} else {
		to, err = time.Parse(dateFormat, lbString)
		if err != nil {
			return nil, fmt.Errorf(log.MalformedQueryParameter(fromParam, err.Error()))
		}
	}

	ubString := context.Query(toParam)
	now := time.Now()

	if len(ubString) == 0 {
		from = now
		log.Info(log.UsingDefaultParameterValue(toParam, from.Format(dateFormat)))
	} else {
		from, err = time.Parse(dateFormat, ubString)
		if err != nil {
			return nil, fmt.Errorf(log.MalformedQueryParameter(toParam, err.Error()))
		}

		if from.After(now) {
			return nil, fmt.Errorf("error during upper interval bound parsing: "+
				"the given bound can't be in the future (current time: %v, given time:%v)", now, from)

		}
	}

	return &intervalQueryParams{
		to:   to,
		from: from,
	}, nil
}

var (
	validAttributes = []string{
		repo.BrokerID,
		repo.BrokerMessageID,
		repo.BusinessSourceKey,
		repo.BusinessObjectKey,
		repo.UniqueID,
		repo.IndexSourceKey,
		repo.OrderingKey,
		repo.AdditionalMetadata,
		repo.LocationKey,
		repo.LocationPosition,
		repo.PublishTime,
		repo.IngestionTime,
		repo.IndexerIngestionTime,
		repo.ConfirmationFlag,
	}

	timestampAttributes = []string{
		repo.PublishTime,
		repo.IngestionTime,
		repo.IndexerIngestionTime,
	}

	uniqueRequestAttributes = []string{
		repo.BrokerID,
		repo.BrokerMessageID,
		repo.BusinessSourceKey,
		repo.BusinessObjectKey,
		repo.UniqueID,
		repo.IndexSourceKey,
		repo.OrderingKey,
		repo.AdditionalMetadata,
		repo.LocationKey,
		repo.LocationPosition,
		repo.PublishTime,
		repo.IngestionTime,
		repo.IndexerIngestionTime,
		repo.ConfirmationFlag,
	}

	intervalRequest = []string{
		repo.BrokerID,
		repo.BrokerMessageID,
		repo.BusinessSourceKey,
		repo.BusinessObjectKey,
		repo.UniqueID,
		repo.OrderingKey,
		repo.AdditionalMetadata,
		repo.LocationKey,
		repo.LocationPosition,
		repo.PublishTime,
	}

	queryRequest = []string{
		repo.BrokerID,
		repo.BrokerMessageID,
		repo.BusinessSourceKey,
		repo.BusinessObjectKey,
		repo.UniqueID,
		repo.OrderingKey,
		repo.AdditionalMetadata,
		repo.LocationKey,
		repo.LocationPosition,
		repo.PublishTime,
	}
)

func checkForInvalidQueryKeys(filters []map[string]interface{}) error {
	var invalidQueryKeys []string

	for _, filter := range filters {
		for key := range filter {
			if !checkIsKeyAValidAttribute(key) {
				invalidQueryKeys = append(invalidQueryKeys, key)
			}
		}
	}

	if len(invalidQueryKeys) != 0 {
		return fmt.Errorf("invalid query filter keys in query request body - %v", invalidQueryKeys)
	} else {
		return nil
	}
}

func checkIsKeyAValidAttribute(key string) bool {
	if strings.HasPrefix(key, repo.AdditionalMetadata) {
		return true
	}

	for _, attribute := range validAttributes {
		if key == attribute {
			return true
		}
	}

	return false
}

func convertTimestamps(filters []map[string]interface{}) error {
	invalidTimestampFields := make(map[string]error)

	for _, filter := range filters {
		for key, value := range filter {
			if isKeyATimestampAttribute(key) {
				timestamp, err := convertToTime(value)
				if err != nil {
					invalidTimestampFields[key] = err
				}

				filter[key] = timestamp
			}
		}
	}

	if len(invalidTimestampFields) != 0 {
		return fmt.Errorf("invalid timestamp values in query request body - %v", invalidTimestampFields)
	} else {
		return nil
	}
}

func isKeyATimestampAttribute(key string) bool {
	for _, attribute := range timestampAttributes {
		if key == attribute {
			return true
		}
	}

	return false
}

func convertToTime(value interface{}) (interface{}, error) {
	switch valueType := value.(type) {
	case string:
		timestamp, err := time.Parse(dateFormat, valueType)
		if err != nil {
			return nil, fmt.Errorf("failed to parse time: %w", err)
		}

		return timestamp, nil
	case map[string]interface{}:
		condition := make(map[string]time.Time)

		for operator, timeStr := range valueType {
			timeStr, ok := timeStr.(string)
			if !ok {
				return nil, fmt.Errorf("time for operator %s is not a string", operator)
			}

			timestamp, err := time.Parse(dateFormat, timeStr)
			if err != nil {
				return nil, fmt.Errorf("failed to parse time for operator %s: %w", operator, err)
			}

			condition[operator] = timestamp
		}

		return condition, nil
	default:
		return nil, fmt.Errorf("invalid time format")
	}
}
