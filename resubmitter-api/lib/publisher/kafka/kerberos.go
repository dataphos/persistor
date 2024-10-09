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

import (
	"os"

	"github.com/dataphos/lib-brokers/pkg/broker/kafka"
	"github.com/dataphos/persistor-resubmitter-api/common/log"
)

// LoadKerberosConfigFromEnv loads broker.KerberosConfig from the expected environment variables.
//
// Returns an error if any environment variable is not defined or if they're misconfigured.
func LoadKerberosConfigFromEnv() (*kafka.KerberosConfig, error) {
	krbConfigPath := os.Getenv(krbConfigPathEnvKey)
	if krbConfigPath == "" {
		return nil, log.EnvVariableNotDefined(krbConfigPathEnvKey)
	}

	krbKeyTabPath := os.Getenv(krbKeyTabEnvKey)
	if krbKeyTabPath == "" {
		return nil, log.EnvVariableNotDefined(krbKeyTabEnvKey)
	}

	krbRealm := os.Getenv(krbRealmEnvKey)
	if krbRealm == "" {
		return nil, log.EnvVariableNotDefined(krbRealmEnvKey)
	}

	krbService := os.Getenv(krbServiceEnvKey)
	if krbService == "" {
		return nil, log.EnvVariableNotDefined(krbServiceEnvKey)
	}

	krbUsername := os.Getenv(krbUsernameEnvKey)
	if krbUsername == "" {
		return nil, log.EnvVariableNotDefined(krbUsernameEnvKey)
	}

	return &kafka.KerberosConfig{
		KeyTabPath: krbKeyTabPath,
		ConfigPath: krbConfigPath,
		Realm:      krbRealm,
		Service:    krbService,
		Username:   krbUsername,
	}, nil
}
