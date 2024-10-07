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

// Package abs contains the Azure Blob Storage logic for the fetcher component
package abs

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/pkg/errors"

	"github.com/dataphos/persistor-resubmitter-api/common/errcodes"
	"github.com/dataphos/persistor-resubmitter-api/common/log"
	"github.com/dataphos/persistor-resubmitter-api/lib/fetcher"
)

const (
	azureStorageAccountNameEnv = "AZURE_STORAGE_ACCOUNT_NAME"

	azureStorageProtocol = "https://"
	azureStorageSuffix   = ".blob.core.windows.net/"
)

type AzureBlobStorage struct {
	client *azblob.Client
}

func FromEnv() (fetcher.BlobStorage, error) {
	return New(os.Getenv(azureStorageAccountNameEnv))
}

func New(storageName string) (fetcher.BlobStorage, error) {
	client, err := newClient(storageName)
	if err != nil {
		return nil, errors.Wrap(err, "can't create ABS client")
	}

	return &AzureBlobStorage{
		client: client,
	}, nil
}

func newClient(storageName string) (*azblob.Client, error) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, errors.Wrap(err, "can't create default Azure credentials")
	}

	url := fmt.Sprintf("%s%s%s", azureStorageProtocol, storageName, azureStorageSuffix)
	client, err := azblob.NewClient(url, credential, nil)

	if err != nil {
		return nil, err
	}

	return client, nil
}

func (abs *AzureBlobStorage) Fetch(ctx context.Context, object string) ([]byte, error) {
	reader, err := abs.Reader(ctx, object)
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

func (abs *AzureBlobStorage) Reader(ctx context.Context, locationKey string) (io.ReadCloser, error) {
	container, blob := extractContainerAndBlobFromLocationKey(locationKey)

	downloadResponse, err := abs.client.DownloadStream(ctx, container, blob, nil)
	if err != nil {
		return nil, err
	}

	reader := downloadResponse.Body

	return reader, nil
}

func extractContainerAndBlobFromLocationKey(locationKey string) (string, string) {
	parameters := strings.Split(locationKey, "/")
	bucket := parameters[3]
	object := strings.Join(parameters[4:], "/")

	return bucket, object
}
