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
	malformedUrlQueryParameterTemplate = "error in parsing %s query parameter: %s"
	usingDefaultParameterValueTemplate = "parameter %s not provided, using default value %s instead"
	parsingEnvVariableFailedTemplate   = "parsing env variable %s failed"
)

func MalformedQueryParameter(parameterName, additionalInformation string) string {
	return fmt.Sprintf(malformedUrlQueryParameterTemplate, parameterName, additionalInformation)
}

func UsingDefaultParameterValue(parameterName, defaultValue string) string {
	return fmt.Sprintf(usingDefaultParameterValueTemplate, parameterName, defaultValue)
}

func ParsingEnvVariableFailed(parameterName string) string {
	return fmt.Sprintf(parsingEnvVariableFailedTemplate, parameterName)
}
