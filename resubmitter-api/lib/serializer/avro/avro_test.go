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
	"reflect"
	"testing"

	"github.com/dataphos/persistor-resubmitter-api/lib/persistor"
)

func TestSerializer_Serialize(t *testing.T) {
	serializer := New()

	metadata := map[string]string{
		"key1":                      "value1",
		"key2":                      "value2",
		persistor.BusinessObjectKey: "business object key",
		persistor.BusinessSourceKey: "business source key",
	}
	expected := []persistor.Record{
		{
			ID:       "id1",
			Data:     []byte("data1"),
			Metadata: metadata,
		},
		{
			ID:       "id2",
			Data:     []byte("data2"),
			Metadata: metadata,
		},
	}

	serialized, err := serializer.Serialize(expected)
	if err != nil {
		t.Fatal(err)
	}
	deserialized, err := serializer.Deserialize(serialized)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(deserialized, expected) {
		t.Fatal("result doesn't match expected")
	}
}
