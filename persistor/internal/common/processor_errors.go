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

package common

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

// ProcError An error that occurred on a batch of messages for an individual step of the processing.
// A batch of messages could've had the same error be their cause of failing; this avoids having
// every message have one error code attached to it.
type ProcError struct {
	ErrorCode  int
	CauseError error
	NumFailed  int
}

func NewProcessingError(numFailed int, err error, errCode int) *ProcError {
	return &ProcError{NumFailed: numFailed, CauseError: err, ErrorCode: errCode}
}

func (procErr *ProcError) Error() string {
	return fmt.Sprintf("Processing Error | Code: %d | Error: %s | Number of messages failed: %d",
		procErr.ErrorCode, procErr.CauseError.Error(), procErr.NumFailed)
}

// MessageBatchError is the type returned by the processor.
// Contains a list of ProcError, which can be easily iterated through.
type MessageBatchError struct {
	ErrorList []*ProcError
}

func (batchErr *MessageBatchError) Error() string {
	errStrings := make([]string, 0, len(batchErr.ErrorList))

	for _, err := range batchErr.ErrorList {
		errStrings = append(errStrings, err.Error())
	}

	return strings.Join(errStrings, "\n")
}

func (batchErr *MessageBatchError) AddErr(procErr *ProcError) {
	batchErr.ErrorList = append(batchErr.ErrorList, procErr)
}

func (batchErr *MessageBatchError) AppendErr(otherErr *MessageBatchError) {
	batchErr.ErrorList = append(batchErr.ErrorList, otherErr.ErrorList...)
}

func (batchErr *MessageBatchError) IsEmpty() bool {
	return len(batchErr.ErrorList) == 0
}

// Temporary returns false if the batch error contains an unrecoverable error.
func (batchErr *MessageBatchError) Temporary() bool {
	for _, procErr := range batchErr.ErrorList {
		var fatalErr *FatalError
		if errors.As(procErr.CauseError, &fatalErr) {
			return false
		}
	}

	return true
}

// FatalError represents an unrecoverable error and can be returned by batch handler
// to stop the streamproc executor while allowing clean termination of the program.
// Implements Temporary interface and returns false which tells the executor that it should stop.
type FatalError struct {
	Err error
}

func (err *FatalError) Error() string {
	return err.Err.Error()
}

func (*FatalError) Temporary() bool {
	return false
}
