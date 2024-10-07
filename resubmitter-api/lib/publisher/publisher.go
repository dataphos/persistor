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

package publisher

import (
	"context"

	"github.com/dataphos/persistor-resubmitter-api/lib/persistor"
)

type Publisher interface {
	Topic(string) (Topic, error)
}

type Topic interface {
	Publish(context.Context, *persistor.Record) error
}

func CreateAttributes(record *persistor.Record) map[string]interface{} {
	attributes := make(map[string]interface{})

	for key, value := range record.Metadata {
		attributes[key] = value
	}

	return attributes
}

func GetOrderingKeyFromAttributes(attributes map[string]interface{}) string {
	orderingKeyValue, isKeyPresent := attributes["ordering_key"]

	orderingKeyString, isCastOK := orderingKeyValue.(string)
	if !isKeyPresent || !isCastOK {
		orderingKeyString = ""
	}

	return orderingKeyString
}
