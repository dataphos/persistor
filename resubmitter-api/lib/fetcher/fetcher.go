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

// Package fetcher contains the reading/fetching logic for Resubmitter.
package fetcher

import (
	"context"
	"io"

	"github.com/pkg/errors"
)

type Fetcher interface {
	Fetch(context.Context, string) ([]byte, error)
}

type Reader interface {
	Reader(context.Context, string) (io.ReadCloser, error)
}

type BlobStorage interface {
	Fetcher
	Reader
}

func Read(reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, errors.Wrap(err, "can't read from file")
	}

	return data, nil
}
