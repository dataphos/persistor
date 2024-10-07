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

package publisher

import (
	"context"
	"errors"
	"sync"

	"github.com/dataphos/persistor-resubmitter-api/lib/persistor"
)

type MockPublisher struct {
	MockTopics []*MockTopic
	mu         sync.Mutex
}
type MockTopic struct {
	PublishedRecords []persistor.Record
	mu               *sync.Mutex
}

func (p *MockPublisher) Topic(_ string) (Topic, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	mockTopic := &MockTopic{mu: &sync.Mutex{}}
	p.MockTopics = append(p.MockTopics, mockTopic)

	return mockTopic, nil
}

func (s *MockTopic) Publish(_ context.Context, record *persistor.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.PublishedRecords = append(s.PublishedRecords, *record)

	return nil
}

type ErrorProducingPublisher struct{}
type ErrorProducingTopic struct{}

func (ErrorProducingPublisher) Topic(_ string) (Topic, error) {
	return ErrorProducingTopic{}, nil
}

func (ErrorProducingTopic) Publish(_ context.Context, _ *persistor.Record) error {
	return errors.New("failed to publish")
}
