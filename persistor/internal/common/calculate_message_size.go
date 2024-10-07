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

// CalculateMessageSize determines the size of a message in bytes
// for determining the size of attributes an approximation is used
// the approximation considers only those fields whose values are of type []bytes or string since most brokers use that value standard
// other value types are not considered in the size calculation.
func CalculateMessageSize(data []byte, attributes map[string]interface{}) int {
	messageSize := len(data)

	for key, value := range attributes {
		// Add the size of the key.
		messageSize += len(key)

		// Add the size of the value.
		switch v := value.(type) {
		case []byte:
			messageSize += len(v)
		case string:
			messageSize += len(v)
		}
	}

	return messageSize
}
