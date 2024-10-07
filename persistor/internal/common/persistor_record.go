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

// Package common contains multiple helper functions and structs used in Persistor.
package common

import (
	"github.com/dataphos/lib-streamproc/pkg/streamproc"
)

// PersistorRecord is a helper structure for converting the Message data into the Avro-relevant format.
type PersistorRecord struct {
	ID       string
	Data     []byte
	Metadata map[string]interface{}
}

// NewPersistorRecord is a simple constructor that places core.Message into the PersistorRecord schema.
func NewPersistorRecord(msg streamproc.Message) interface{} {
	return PersistorRecord{ID: msg.ID, Data: msg.Data, Metadata: msg.Attributes}
}

// SchemaLoader is an alias for a method that loads a message into a schema's struct.
type SchemaLoader func(message streamproc.Message) interface{}

// LoadedData contains the information on a batch of Messages transformed to their Records/structs.
// Since a batch of N messages may be converted to M structs, the RecordToMessage field maps
// the index of the record to the message. The values are an array, for cases where N > M. (Messages are
// merged into a single record by some criteria by the loader).
// If this field is nil, assume the mapping is 1:1.
type LoadedData struct {
	Records         []interface{}
	RecordToMessage map[int][]int
}

// DataLoader is an alias for a method that takes one or more messages and loads them all
// into an appropriate schema from the start.
type DataLoader func(...streamproc.Message) ([]streamproc.Message, LoadedData, *ProcError)
