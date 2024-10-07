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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/pkg/errors"

	"github.com/dataphos/persistor-resubmitter-api/common/errcodes"
	"github.com/dataphos/persistor-resubmitter-api/common/log"
	"github.com/dataphos/persistor-resubmitter-api/common/util"
)

type Message struct {
	BrokerID             string            `bson:"broker_id,omitempty" json:"broker_id,omitempty"`
	BrokerMessageID      string            `bson:"broker_msg_id,omitempty" json:"broker_msg_id,omitempty"`
	BusinessSourceKey    string            `bson:"business_source_key,omitempty" json:"business_source_key,omitempty"`
	BusinessObjectKey    string            `bson:"business_object_key,omitempty" json:"business_object_key,omitempty"`
	UniqueId             string            `bson:"unique_id,omitempty" json:"unique_id,omitempty"`
	IndexSourceKey       string            `bson:"index_source_key,omitempty" json:"index_source_key,omitempty"`
	OrderingKey          string            `bson:"ordering_key,omitempty" json:"ordering_key,omitempty"`
	LocationKey          string            `bson:"location_key,omitempty" json:"location_key,omitempty"`
	LocationPosition     *int              `bson:"location_position,omitempty" json:"location_position,omitempty"`
	PublishTime          *time.Time        `bson:"publish_time,omitempty" json:"publish_time,omitempty"`
	IngestionTime        *time.Time        `bson:"ingestion_time,omitempty" json:"ingestion_time,omitempty"`
	IndexerIngestionTime *time.Time        `bson:"indexer_ingestion_time,omitempty" json:"indexer_ingestion_time,omitempty"`
	AdditionalMetadata   map[string]string `bson:"additional_metadata,omitempty" json:"additional_metadata,omitempty"`
}

type IntervalQueryResponse struct {
	TotalCount    int       `json:"total_count"`
	ReturnedCount int       `json:"returned_count"`
	Limit         int       `json:"limit"`
	Offset        int       `json:"offset"`
	Messages      []Message `json:"messages"`
}

type Indexer interface {
	Get(string) ([]Message, error)
	GetAll(string, []string) ([]Message, error)
	GetAllInInterval(string, string, time.Time, time.Time, int, int) (*IntervalQueryResponse, error)
	GetQueried(string, util.QueryRequestBody, int, int) (*IntervalQueryResponse, error)
}

type indexer struct {
	baseUrl string
}

func FromEnv() Indexer {
	return New(os.Getenv(BaseUrlEnv))
}

const (
	BaseUrlEnv                       = "INDEXER_URL"
	GetByUniqueIdEndpoint            = "/exact/"
	GetAllEndpoint                   = "/all"
	GetByIntervalAndBrokerIDEndpoint = "/range/"
	GetByQueryParamsEndpoint         = "/query/"

	requestCreationFailedMessage = "request creation failed"
)

func New(baseUrl string) Indexer {
	return &indexer{baseUrl: baseUrl}
}

func (indexer *indexer) Get(id string) ([]Message, error) {
	url := indexer.constructGetByUniqueIdUrl(id)
	log.Debug(fmt.Sprintf("sending request to Indexer API on %s", url), 0)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Wrap(err, requestCreationFailedMessage)
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "http call for unique id failed")
	}

	defer func() {
		err = response.Body.Close()
		if err != nil {
			log.Debug(errors.Wrap(err, "can't close response body").Error(), errcodes.Indexer)
		}
	}()

	return extractIndexerMessage(response)
}

func (indexer *indexer) constructGetByUniqueIdUrl(id string) string {
	return indexer.baseUrl + GetByUniqueIdEndpoint + id
}

func extractIndexerMessage(response *http.Response) ([]Message, error) {
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, errors.Wrap(err, "can't read from response body")
	}

	var messages []Message
	err = json.Unmarshal(body, &messages)
	if err != nil {
		return nil, errors.Wrap(err, "can't unmarshall response body")
	}

	return messages, nil
}

func (indexer *indexer) GetAll(mongoCollection string, ids []string) ([]Message, error) {
	body, err := constructGetAllRequestBody(ids)
	if err != nil {
		return nil, errors.Wrap(err, "can't construct request body")
	}

	url := indexer.constructGetAllUrl(mongoCollection)
	log.Debug(fmt.Sprintf("sending request to Indexer API on %s", url), 0)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, body)
	if err != nil {
		return nil, errors.Wrap(err, requestCreationFailedMessage)
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "http call for get all failed")
	}

	defer func() {
		err = response.Body.Close()
		if err != nil {
			log.Debug(errors.Wrap(err, "can't close response body").Error(), errcodes.Indexer)
		}
	}()

	return extractIndexerMessages(response)
}

func constructGetAllRequestBody(ids []string) (io.Reader, error) {
	request := struct {
		Ids []string `json:"ids"`
	}{
		Ids: ids,
	}

	body, err := json.Marshal(&request)
	if err != nil {
		return nil, errors.Wrap(err, "can't marshall request body")
	}

	return bytes.NewReader(body), nil
}

func (indexer *indexer) constructGetAllUrl(mongoCollection string) string {
	return indexer.baseUrl + GetAllEndpoint + fmt.Sprintf("/%s", mongoCollection)
}

func extractIndexerMessages(response *http.Response) ([]Message, error) {
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, errors.Wrap(err, "can't read from response body")
	}

	var messages []Message

	err = json.Unmarshal(body, &messages)
	if err != nil {
		return nil, errors.Wrap(err, "can't unmarshall response body")
	}

	return messages, nil
}

func (indexer *indexer) GetAllInInterval(mongoCollection, brokerId string, from, to time.Time, limit, offset int) (*IntervalQueryResponse, error) {
	url := indexer.constructGetByIntervalAndBrokerIdUrl(mongoCollection, brokerId, from, to, limit, offset)
	log.Debug(fmt.Sprintf("sending request to Indexer API on %s", url), 0)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Wrap(err, requestCreationFailedMessage)
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "http call for interval failed")
	}

	defer func() {
		err = response.Body.Close()
		if err != nil {
			log.Debug(errors.Wrap(err, "can't close response body").Error(), errcodes.Indexer)
		}
	}()

	return extractIndexerIntervalQueryResponse(response)
}

const (
	Limit  = "limit"
	Offset = "offset"
	From   = "from"
	To     = "to"

	DateFormat = "2006-01-02T15:04:05.99999999Z"
)

func (indexer *indexer) constructGetByIntervalAndBrokerIdUrl(mongoCollection, brokerId string, from, to time.Time, limit, offset int) string {
	return fmt.Sprintf(
		"%s%s%s/%s?%s=%s&%s=%s&%s=%v&%s=%v",
		indexer.baseUrl,
		GetByIntervalAndBrokerIDEndpoint,
		mongoCollection,
		brokerId,
		From, from.Format(DateFormat),
		To, to.Format(DateFormat),
		Limit, limit,
		Offset, offset,
	)
}

func extractIndexerIntervalQueryResponse(response *http.Response) (*IntervalQueryResponse, error) {
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, errors.Wrap(err, "can't read from response body")
	}

	if response.StatusCode != 200 {
		var indexerResponse map[string]string
		err = json.Unmarshal(body, &indexerResponse)
		if err != nil {
			return nil, errors.Wrap(err, "can't read error from response body")
		}
		return nil, errors.New(indexerResponse["error"])
	}

	var message IntervalQueryResponse

	err = json.Unmarshal(body, &message)
	if err != nil {
		return nil, errors.Wrap(err, "can't unmarshall response body")
	}

	return &message, nil
}

func (indexer *indexer) GetQueried(mongoCollection string, queryBody util.QueryRequestBody, limit, offset int) (*IntervalQueryResponse, error) {
	url := indexer.constructGetQueriedURL(mongoCollection, limit, offset)
	log.Debug(fmt.Sprintf("sending request to Indexer API on %s", url), 0)

	body, err := json.Marshal(queryBody)
	if err != nil {
		return nil, errors.Wrap(err, "couldn't marshal request body into bytes")
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, errors.Wrap(err, requestCreationFailedMessage)
	}

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "http call for interval failed")
	}

	defer func() {
		err = response.Body.Close()
		if err != nil {
			log.Debug(errors.Wrap(err, "can't close response body").Error(), errcodes.Indexer)
		}
	}()

	// should probably change the method name since it now has a more general use.
	return extractIndexerIntervalQueryResponse(response)
}

func (indexer *indexer) constructGetQueriedURL(mongoCollection string, limit, offset int) string {
	return fmt.Sprintf(
		"%s%s%s?%s=%v&%s=%v",
		indexer.baseUrl,
		GetByQueryParamsEndpoint,
		mongoCollection,
		Limit, limit,
		Offset, offset,
	)
}
