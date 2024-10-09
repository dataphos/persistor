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

package fetcher

import (
	"bytes"
	"context"
	"io"
)

type Mock struct {
	records interface{}
}

func (s Mock) Fetch(ctx context.Context, location string) ([]byte, error) {
	switch records := s.records.(type) {
	case map[string][]byte:
		reader, err := s.Reader(ctx, location)
		if err != nil {
			return nil, err
		}

		data, err := Read(reader)
		if err != nil {
			return nil, err
		}

		err = reader.Close()
		if err != nil {
			return nil, err
		}

		return data, nil
	case error:
		return nil, records
	}

	return nil, nil
}

func (s *Mock) SetData(recordMap interface{}) {
	s.records = recordMap
}

func (s Mock) Reader(_ context.Context, location string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(s.records.(map[string][]byte)[location])), nil //nolint:forcetypeassert // no need
}
