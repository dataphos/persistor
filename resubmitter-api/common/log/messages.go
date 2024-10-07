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

package log

import "fmt"

const (
	usingDefaultParameterValueTemplate      = "parameter %s not provided, using default value %v instead"
	positiveIntegerEnvVariableErrorTemplate = "error during environment variable loading: parameter %s is not a positive integer: %w"
	parsingEnvVariableFailedTemplate        = "parsing env variable %s failed"
	envVariableNotDefinedTemplate           = "environment variable %s not defined"
)

func UsingDefaultParameterValue(parameterName string, defaultValue interface{}) string {
	return fmt.Sprintf(usingDefaultParameterValueTemplate, parameterName, defaultValue)
}

func PositiveIntEnvVariableError(parameterName string, err error) error {
	return fmt.Errorf(positiveIntegerEnvVariableErrorTemplate, parameterName, err)
}

func ParsingEnvVariableFailed(parameterName string) string {
	return fmt.Sprintf(parsingEnvVariableFailedTemplate, parameterName)
}

func EnvVariableNotDefined(parameterName string) error {
	return fmt.Errorf(envVariableNotDefinedTemplate, parameterName)
}
