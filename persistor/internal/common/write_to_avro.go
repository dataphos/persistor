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
	"bytes"
	"fmt"

	"github.com/dataphos/lib-streamproc/pkg/streamproc"
	"github.com/hamba/avro/ocf"
	"github.com/pkg/errors"
)

// AvroRecord is a generic help type used for managing Avro schemas.
type AvroRecord struct {
	Schema string
	Loader SchemaLoader
}

// AvroSchema representation of the PersistorRecord struct.
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

// WriteToAvro Writes a set of Messages to Avro to a given buffer, in a specified schema.
func WriteToAvro(msgs []streamproc.Message, buffer *bytes.Buffer, schema string, loader SchemaLoader) (errorCode int, err error) {
	// Prepare the OFC writer.
	encoder, err := ocf.NewEncoder(schema, buffer)
	if err != nil {
		return AvroSchemaInvalidError, &FatalError{errors.Errorf("invalid avro schema: %s", err.Error())}
	}

	// Write the messages to the given writer.
	for _, msg := range msgs {
		record := loader(msg)

		err = encoder.Encode(record)
		if err != nil {
			// Log...
			// Since we have been given the Message interface, the only reason
			// this operation would fail is if something is wrong with the defined
			// schema.
			return AvroSchemaMismatchError, &FatalError{errors.Errorf("incompatible avro schema: %s", err.Error())}
		}
	}

	// Flush the encoder.
	err = encoder.Flush()
	if err != nil {
		return WriterError, fmt.Errorf("writing to avro: %w", err)
	}

	return 0, nil
}
