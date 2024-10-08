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

// Package persistor contains necessary persistor structures.
package persistor

const (
	BusinessSourceKey = "business_source_key"
	BusinessObjectKey = "business_object_key"
)

type Record struct {
	ID       string
	Data     []byte
	Metadata map[string]string
}

const AvroSchema = `{
	"type": "record",
	"name": "persistorrecord",
	"namespace": "com.syntio.dataphos",
	"fields" : [
		{"name": "ID", "type": "string"},
		{"name": "Data", "type": "bytes"},
		{"name": "Metadata", "type": {"type": "map", "values": "string"}}
	]
}`
