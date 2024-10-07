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

package avro

import (
	"bytes"
	"io"

	"github.com/hamba/avro/ocf"
	"github.com/pkg/errors"

	"github.com/dataphos/persistor-resubmitter-api/lib/persistor"
	"github.com/dataphos/persistor-resubmitter-api/lib/serializer"
)

var schema = persistor.AvroSchema

type Serializer struct{}

func New() serializer.SerializationService {
	return Serializer{}
}

func (Serializer) Serialize(records []persistor.Record) ([]byte, error) {
	var buffer bytes.Buffer

	err := fillBuffer(&buffer, records)
	if err != nil {
		return nil, errors.Wrap(err, "can't fill buffer")
	}

	return buffer.Bytes(), nil
}

func fillBuffer(writer io.Writer, records []persistor.Record) error {
	encoder, err := ocf.NewEncoder(schema, writer)
	if err != nil {
		return errors.Wrap(err, "can't create encoder")
	}

	for _, record := range records {
		err = encoder.Encode(record)
		if err != nil {
			return errors.Wrapf(err, "can't encode record with id: %s", record.ID)
		}
	}

	err = encoder.Flush()
	if err != nil {
		return errors.Wrap(err, "flushing encoder failed")
	}

	return nil
}

func (Serializer) Deserialize(blob []byte) ([]persistor.Record, error) {
	dec, err := ocf.NewDecoder(bytes.NewReader(blob))
	if err != nil {
		return nil, errors.Wrap(err, "can't create decoder")
	}

	var records []persistor.Record

	for dec.HasNext() {
		var record persistor.Record

		err = dec.Decode(&record)
		if err != nil {
			return nil, errors.Wrap(err, "can't decode into struct")
		}

		records = append(records, record)
	}

	return records, nil
}
