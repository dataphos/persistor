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

package gcp

import (
	"bytes"
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"

	"github.com/dataphos/lib-streamproc/pkg/streamproc"
	"github.com/dataphos/persistor/internal/common"
)

// GCSWriter used for basic message storing to buckets.
type GCSWriter struct {
	client storage.Client
}

// GCSAvroWriter takes a batch of messages and stores them to a single blob in Avro format.
type GCSAvroWriter struct {
	GCSWriter
	record common.AvroRecord
}

// Write stores a batch of messages with a given name to GCS in avro format.
func (writer *GCSAvroWriter) Write(ctx context.Context, bucketName string, objectName string, msgs ...streamproc.Message) *common.ProcError {
	// Start with an empty byte array.
	var bufBytes []byte

	// Byte buffer implements io.Writer.
	buf := bytes.NewBuffer(bufBytes)

	errCode, err := common.WriteToAvro(msgs, buf, writer.record.Schema, writer.record.Loader)
	if err != nil {
		// If an error happens here, it means that something is wrong with the schema
		// given to the Avro Record.
		return common.NewProcessingError(len(msgs), err, errCode)
	}
	bucketWriter := writer.client.Bucket(bucketName).Object(objectName).NewWriter(ctx)

	if _, err = bucketWriter.Write(buf.Bytes()); err != nil {
		_ = bucketWriter.Close()

		return common.NewProcessingError(len(msgs), err, common.WriterError)
	}
	// Close the GCS Blob writer.
	// If an error is returned here, it means that writing to blob
	// was not successful.
	if err = bucketWriter.Close(); err != nil {
		return common.NewProcessingError(len(msgs), errors.Wrap(err, "error closing writer"), common.WriterError)
	}

	return nil
}

// NewGCSWriter creates a writer based on the config.
func NewGCSWriter() (*GCSWriter, error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		err = fmt.Errorf("GCS client initialization: %w", err)
	}

	return &GCSWriter{
		client: *client,
	}, err
}

func NewGCSAvroBatchWriter(schema string, loader common.SchemaLoader) (*GCSAvroWriter, error) {
	writer, err := NewGCSWriter()

	return &GCSAvroWriter{
		GCSWriter: *writer,
		record: common.AvroRecord{
			Schema: schema,
			Loader: loader,
		},
	}, err
}
