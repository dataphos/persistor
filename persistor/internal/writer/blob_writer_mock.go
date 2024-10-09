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

package writer

import (
	"context"
	"sync"

	"github.com/pkg/errors"

	"github.com/dataphos/lib-streamproc/pkg/streamproc"
	"github.com/dataphos/persistor/internal/common"
)

type FakeBlobWriter struct {
	Blobs         map[string][]streamproc.Message
	BlobMutex     sync.Mutex
	ShouldFail    func(msgs []streamproc.Message) bool
	FailedBatches map[string][]streamproc.Message
}

func (w *FakeBlobWriter) Write(ctx context.Context, _ string, objectName string, msgs ...streamproc.Message) *common.ProcError {
	select {
	case <-ctx.Done():
		if ctx.Err() != nil {
			return common.NewProcessingError(len(msgs), ctx.Err(), common.WriterError)
		}

		return common.NewProcessingError(len(msgs), context.Canceled, common.WriterError)
	default:
	}

	w.BlobMutex.Lock()
	defer w.BlobMutex.Unlock()

	if w.ShouldFail != nil && w.ShouldFail(msgs) {
		w.FailedBatches[objectName] = msgs

		return common.NewProcessingError(len(msgs), errors.New("pretend writer failed"), common.WriterError)
	}

	w.Blobs[objectName] = msgs

	return nil
}
