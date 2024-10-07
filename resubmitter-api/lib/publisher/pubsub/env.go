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

package pubsub

const (
	projectIDEnvKey = "PUBSUB_PROJECT_ID"
)

const (
	delayThresholdEnvKey                = "PUBLISH_DELAY_THRESHOLD"
	countThresholdEnvKey                = "PUBLISH_COUNT_THRESHOLD"
	byteThresholdEnvKey                 = "PUBLISH_BYTE_THRESHOLD"
	numPublishGoroutinesEnvKey          = "NUM_PUBLISH_GOROUTINES"
	timeoutEnvKey                       = "PUBLISH_TIMEOUT"
	maxPublishOutstandingMessagesEnvKey = "MAX_PUBLISH_OUTSTANDING_MESSAGES"
	maxPublishOutstandingBytesEnvKey    = "MAX_PUBLISH_OUTSTANDING_BYTES"
	publishEnableMessageOrderingEnvKey  = "PUBLISH_ENABLE_MESSAGE_ORDERING"
)
