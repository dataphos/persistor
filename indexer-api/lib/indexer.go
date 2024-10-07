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
	"time"

	"github.com/dataphos/persistor-indexer-api/lib/repo"
)

type Indexer struct {
	Repository repo.Repository
}

func New(repository repo.Repository) *Indexer {
	return &Indexer{Repository: repository}
}

func (indexer *Indexer) Get(mongoCollection, id string, attributes []string) ([]repo.Message, error) {
	return indexer.Repository.Get(context.Background(), mongoCollection, id, attributes)
}

func (indexer *Indexer) GetAll(mongoCollection string, ids, attributes []string) ([]repo.Message, error) {
	return indexer.Repository.GetAll(context.Background(), mongoCollection, ids, attributes)
}

type Interval struct {
	TotalCount    int            `json:"total_count"`
	ReturnedCount int            `json:"returned_count"`
	Limit         int            `json:"limit"`
	Offset        int            `json:"offset"`
	Messages      []repo.Message `json:"messages"`
}

func (indexer *Indexer) GetAllInInterval(mongoCollection, brokerId string, from, to time.Time, limit, offset int, attributes []string) (*Interval, error) {
	messages, err := indexer.Repository.GetAllInInterval(context.Background(), mongoCollection, from, to, brokerId, limit, offset, attributes)
	if err != nil {
		return nil, err
	}

	count, err := indexer.Repository.GetAllInIntervalDocumentCount(context.Background(), mongoCollection, from, to, brokerId)
	if err != nil {
		return nil, err
	}

	return &Interval{
		TotalCount:    int(count),
		ReturnedCount: len(messages),
		Limit:         limit,
		Offset:        offset,
		Messages:      messages,
	}, nil
}

func (indexer *Indexer) GetQueried(queryInfo repo.QueryInformation) (*Interval, error) {
	messages, err := indexer.Repository.GetQueried(context.Background(), queryInfo)
	if err != nil {
		return nil, err
	}

	count, err := indexer.Repository.GetQueriedDocumentCount(
		context.Background(),
		queryInfo.MongoCollection,
		queryInfo.Filters)

	if err != nil {
		return nil, err
	}

	return &Interval{
		TotalCount:    int(count),
		ReturnedCount: len(messages),
		Limit:         queryInfo.Limit,
		Offset:        queryInfo.Offset,
		Messages:      messages,
	}, nil
}
