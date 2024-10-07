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

package azure

import (
	"bytes"
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/dataphos/lib-streamproc/pkg/streamproc"
	"github.com/dataphos/persistor/internal/common"
	"github.com/dataphos/persistor/internal/common/log"
)

const (
	// BucketProtocol Protocol used to connect to the azure container.
	BucketProtocol = "https://"
)

type ABSWriter struct {
	client azblob.Client
}

type ABSAvroWriter struct {
	ABSWriter
	record common.AvroRecord
}

// Write stores the data of messages to storage in batches.
func (writer *ABSAvroWriter) Write(ctx context.Context, bucketName string, objectName string, msgs ...streamproc.Message) *common.ProcError {
	// Start with an empty byte array.
	var bufBytes []byte

	// Byte buffer implements io.Writer.
	buf := bytes.NewBuffer(bufBytes)

	// Write messages to the Avro format to the writer's given schema.
	errCode, err := common.WriteToAvro(msgs, buf, writer.record.Schema, writer.record.Loader)
	if err != nil {
		return common.NewProcessingError(len(msgs), err, errCode)
	}

	// Upload the buffer to the blob.
	_, err = writer.client.UploadBuffer(ctx, bucketName, objectName, buf.Bytes(), &azblob.UploadBufferOptions{})
	if err != nil {
		return common.NewProcessingError(len(msgs), err, common.WriterError)
	}

	return nil
}

func NewABSAvroBatchWriter(storageAccountID string, schema string, loader common.SchemaLoader) (*ABSAvroWriter, error) {
	absWriter, err := NewABSWriter(storageAccountID)

	return &ABSAvroWriter{
		ABSWriter: *absWriter,
		record: common.AvroRecord{
			Schema: schema,
			Loader: loader,
		},
	}, err
}

func NewABSWriter(storageAccountID string) (*ABSWriter, error) {
	serviceURL := fmt.Sprintf("%s%s.blob.core.windows.net/", BucketProtocol, storageAccountID)
	client, err := NewAzblobClient(serviceURL)

	return &ABSWriter{
		client: client,
	}, err
}

// NewAzblobClient constructor for container client which will be using DefaultAzureCredential with environment variables.
// All environment variables must be set in order, for credentials, to be created.
func NewAzblobClient(serviceURL string) (azblob.Client, error) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		log.Debugw("Unable to create default azure credentials.", common.StorageInitializationError, log.F{log.ErrorFieldKey: err.Error()})

		return azblob.Client{}, fmt.Errorf("creating credentials: %w", err)
	}

	client, err := azblob.NewClient(serviceURL, credential, nil)
	if err != nil {
		log.Debugw("Failed to initialize the storage client!", common.StorageInitializationError, log.F{log.ErrorFieldKey: err.Error()})

		err = fmt.Errorf("ABS client initialization: %w", err)
	}

	return *client, err
}
