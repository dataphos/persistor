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

// Package gs contains the Google Cloud Storage logic for the fetcher component.
package gs

import (
	"context"
	"io"
	"os"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/option"

	"github.com/dataphos/persistor-resubmitter-api/common/errcodes"
	"github.com/dataphos/persistor-resubmitter-api/common/log"
	"github.com/dataphos/persistor-resubmitter-api/lib/fetcher"
)

type GoogleStorage struct {
	client *storage.Client
}

const CredentialsEnv = "CREDENTIALS_GS"

func FromEnv() (fetcher.BlobStorage, error) {
	return New(os.Getenv(CredentialsEnv))
}

func New(filename string) (fetcher.BlobStorage, error) {
	client, err := storage.NewClient(context.Background(), option.WithCredentialsFile(filename))
	if err != nil {
		return nil, errors.Wrap(err, "can't create gs client")
	}

	return &GoogleStorage{
		client: client,
	}, nil
}

func (gs *GoogleStorage) Fetch(ctx context.Context, object string) ([]byte, error) {
	reader, err := gs.Reader(ctx, object)
	if err != nil {
		return nil, errors.Wrap(err, "can't acquire reader")
	}

	data, err := fetcher.Read(reader)
	if err != nil {
		return nil, errors.Wrap(err, "reader failed")
	}

	err = reader.Close()
	if err != nil {
		log.Debug(errors.Wrap(err, "can't close reader object").Error(), errcodes.Fetcher)
	}

	return data, nil
}

func (gs *GoogleStorage) Reader(ctx context.Context, locationKey string) (io.ReadCloser, error) {
	bucket, object := extractBucketAndObjectFromLocationKey(locationKey)

	reader, err := gs.client.Bucket(bucket).Object(object).NewReader(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "can't acquire reader for object: %s", object)
	}

	return reader, nil
}

func extractBucketAndObjectFromLocationKey(locationKey string) (string, string) {
	parameters := strings.Split(locationKey, "/")
	bucket := parameters[2]
	object := strings.Join(parameters[3:], "/")

	return bucket, object
}
