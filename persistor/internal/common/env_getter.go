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

// package common contains functionality needed by other modules.

import (
	"errors"
	"fmt"
	"os"
)

// Define static errors
var (
	ErrEnvVarNotSet      = errors.New("environment variable is not set")
	ErrEnvVarValueNotSet = errors.New("value for environment variable was not found")
)

// GetEnvVariable represents a helper function which receives an environment variable
// name and attempts to extract its value.
// The function returns an extracted value and an error message if an error occurs.
func GetEnvVariable(name string) (string, error) {
	value, ok := os.LookupEnv(name)
	if !ok {
		// Wrap the static error with the environment variable name
		return "", fmt.Errorf("%w: '%s'", ErrEnvVarNotSet, name)
	}

	if value == "" {
		// Wrap the static error with the environment variable name
		return "", fmt.Errorf("%w: '%s'", ErrEnvVarValueNotSet, name)
	}

	return value, nil
}
