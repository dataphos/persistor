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

package kafka

const (
	brokerAddrsEnvKey        = "KAFKA_BROKERS"
	tlsEnabledEnvKey         = "KAFKA_USE_TLS"
	saslEnabledEnvKey        = "KAFKA_USE_SASL"
	skipVerifyEnvKey         = "KAFKA_SKIP_VERIFY"
	disableCompressionEnvKey = "KAFKA_DISABLE_COMPRESSION"
)

const (
	publisherBatchSize = "KAFKA_BATCH_SIZE"
	batchBytesEnvKey   = "KAFKA_BATCH_BYTES"
	lingerEnvKey       = "KAFKA_BATCH_TIMEOUT"
)

const (
	enableKerberosKey   = "ENABLE_KERBEROS"
	krbConfigPathEnvKey = "KRB_CONFIG_PATH"
	krbRealmEnvKey      = "KRB_REALM"
	krbServiceEnvKey    = "KRB_SERVICE_NAME"
	krbKeyTabEnvKey     = "KRB_KEY_TAB"
	krbUsernameEnvKey   = "KRB_USERNAME"
)
