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

// Package writer contains code for Persistor's writer component.
package writer

import (
	"context"

	"github.com/dataphos/lib-streamproc/pkg/streamproc"
	"github.com/dataphos/persistor/internal/common"
)

// BlobWriter is a blob storage writer that writes message batches to blobs.
type BlobWriter interface {
	Write(ctx context.Context, bucketName string, objectName string, msgs ...streamproc.Message) *common.ProcError
}

// RecordWriter is a database writer for message metadata.
type RecordWriter interface {
	Write(ctx context.Context, buffer []interface{}) ([]int, error)
}
