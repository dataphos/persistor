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

package repo

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"
)

const (
	mongoCollection = "some_mongo_collection"
)

func TestLocalCacheWrapperByUniqueID(t *testing.T) {
	ctx := context.Background()
	const key = "anything"
	attributes := []string{"anything1", "anything2"}

	wrapper, err := WithLocalCache(Mock{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = wrapper.Get(ctx, mongoCollection, key, attributes)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLocalCacheWrapperGetByIntervalAndBrokerID(t *testing.T) {
	ctx := context.Background()
	const (
		brokerId = "anything"
		limit    = 20
		offset   = 0
	)
	attributes := []string{"anything1", "anything2"}
	var lb = time.Time{}
	var ub = time.Time{}

	wrapper, err := WithLocalCache(Mock{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = wrapper.GetAllInInterval(ctx, mongoCollection, lb, ub, brokerId, limit, offset, attributes)
	if err != nil {
		t.Fatal(err)
	}
}

func TestConstructDocumentCountKeyFromParams(t *testing.T) {
	const brokerId = "anything"
	var lb = time.Time{}
	var ub = time.Time{}

	expected := fmt.Sprintf("%v_%v_%v_%v", lb, ub, brokerId, mongoCollection)
	actual := constructDocumentCountKeyFromParams(lb, ub, brokerId, mongoCollection)

	if expected != actual {
		log.Fatalf("expected and actual key values mismatch (%s != %s)", expected, actual)
	}
}

func TestLocalCacheWrapperGetIntervalAndBrokerIDDocumentCount(t *testing.T) {
	ctx := context.Background()

	const brokerId = "anything"
	var lb = time.Time{}
	var ub = time.Time{}

	wrapper, err := WithLocalCache(Mock{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = wrapper.GetAllInIntervalDocumentCount(ctx, mongoCollection, lb, ub, brokerId)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLocalCacheWrapperGetQueried(t *testing.T) {
	ctx := context.Background()
	queryFilters := []map[string]interface{}{
		{IndexSourceKey: "anyKey1Value1"},
		{BusinessSourceKey: "anyKey1"},
	}
	const (
		limit  = 20
		offset = 0
	)
	attributes := []string{"anything1", "anything2"}

	queryInfo := QueryInformation{
		MongoCollection: mongoCollection,
		Filters:         queryFilters,
		Limit:           limit,
		Offset:          offset,
		AttributesList:  attributes,
	}

	wrapper, err := WithLocalCache(Mock{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = wrapper.GetQueried(ctx, queryInfo)
	if err != nil {
		t.Fatal(err)
	}
}

func TestLocalCacheWrapperGetQueriedDocumentCount(t *testing.T) {
	ctx := context.Background()

	queryFilters := []map[string]interface{}{
		{IndexSourceKey: "anyKey1Value1"},
		{BusinessSourceKey: "anyKey1"},
	}

	wrapper, err := WithLocalCache(Mock{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = wrapper.GetQueriedDocumentCount(ctx, mongoCollection, queryFilters)
	if err != nil {
		t.Fatal(err)
	}
}
