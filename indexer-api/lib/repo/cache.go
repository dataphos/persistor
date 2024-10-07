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
	"time"

	lru "github.com/hnlq715/golang-lru"
)

type cached struct {
	Repository Repository
	countCache *lru.TwoQueueCache
	mtx        KeyMutex
}

type CacheConfig struct {
	CountCacheSize int
}

var DefaultCacheConfig = CacheConfig{
	CountCacheSize: 100,
}

type CacheOption func(*CacheConfig)

func WithSize(size int) CacheOption {
	return func(config *CacheConfig) {
		config.CountCacheSize = size
	}
}

func WithLocalCache(repository Repository, opts ...CacheOption) (Repository, error) {
	cacheConfig := DefaultCacheConfig
	for _, opt := range opts {
		opt(&cacheConfig)
	}

	countCache, err := lru.New2Q(cacheConfig.CountCacheSize)
	if err != nil {
		return nil, err
	}

	return &cached{
		Repository: repository,
		countCache: countCache,
		mtx:        NewKeyMutex(),
	}, nil
}

func (cacheRepo *cached) Get(ctx context.Context, mongoCollection, id string, attributesList []string) ([]Message, error) {
	return cacheRepo.Repository.Get(ctx, mongoCollection, id, attributesList)
}

func (cacheRepo *cached) GetAll(ctx context.Context, mongoCollection string, ids, attributesList []string) ([]Message, error) {
	return cacheRepo.Repository.GetAll(ctx, mongoCollection, ids, attributesList)
}

// GetAllInInterval returns a collection of a subset of metadata for the specific time interval and broker_id.
// The metadata subset that is returned consists of message_id, message count, and location (both path and position).
func (cacheRepo *cached) GetAllInInterval(ctx context.Context, mongoCollection string, to, from time.Time, brokerId string, limit, offset int, attributesList []string) ([]Message, error) {
	return cacheRepo.Repository.GetAllInInterval(ctx, mongoCollection, to, from, brokerId, limit, offset, attributesList)
}

// GetAllInIntervalDocumentCount returns the count of documents that would be returned by the GetAllInInterval.
func (cacheRepo *cached) GetAllInIntervalDocumentCount(ctx context.Context, mongoCollection string, to, from time.Time, brokerId string) (int64, error) {
	key := constructDocumentCountKeyFromParams(to, from, brokerId, mongoCollection)

	cacheRepo.mtx.RLock(key)
	count, hit := cacheRepo.tryGetIntervalAndBrokerIDDocumentCount(key)
	cacheRepo.mtx.RUnlock(key)
	if hit {
		return count, nil
	}

	cacheRepo.mtx.Lock(key)
	defer cacheRepo.mtx.Unlock(key)

	count, err := cacheRepo.Repository.GetAllInIntervalDocumentCount(ctx, mongoCollection, to, from, brokerId)
	if err != nil {
		return 0, err
	}

	cacheRepo.storeIntervalAndBrokerIDDocumentCount(key, count)

	return count, err
}

func constructDocumentCountKeyFromParams(to, from time.Time, brokerId, mongoCollection string) string {
	return fmt.Sprintf("%v_%v_%v_%v", to, from, brokerId, mongoCollection)
}

func (cacheRepo *cached) tryGetIntervalAndBrokerIDDocumentCount(key string) (int64, bool) {
	count, hit := cacheRepo.countCache.Get(key)
	if hit {
		return count.(int64), hit
	}
	return 0, hit
}

func (cacheRepo *cached) storeIntervalAndBrokerIDDocumentCount(key string, count int64) {
	cacheRepo.countCache.Add(key, count)
}

func (cacheRepo *cached) GetQueried(ctx context.Context, queryInfo QueryInformation) ([]Message, error) {
	return cacheRepo.Repository.GetQueried(ctx, queryInfo)
}

func (cacheRepo *cached) GetQueriedDocumentCount(ctx context.Context, mongoCollection string, filters []map[string]interface{}) (int64, error) {
	return cacheRepo.Repository.GetQueriedDocumentCount(ctx, mongoCollection, filters)
}
