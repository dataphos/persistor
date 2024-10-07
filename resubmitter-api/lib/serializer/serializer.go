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

package serializer

import (
	"github.com/dataphos/persistor-resubmitter-api/lib/persistor"
)

type Serializer interface {
	Serialize([]persistor.Record) ([]byte, error)
}

type Deserializer interface {
	Deserialize([]byte) ([]persistor.Record, error)
}

type SerializationService interface {
	Serializer
	Deserializer
}
