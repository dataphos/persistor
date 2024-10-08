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

import (
	"fmt"
)

// Templates to be used with fmt.Sprintf to ensure consistency in log formation.
const (
	configErrorMessageTemplate      = "Error in %s configuration!"
	defaultValueUsedMessageTemplate = "Warning in %s configuration: value for '%s' is not set! Using %s as a default."
	invalidMask                     = "value %s is not allowed because it was not found in custom values, check mask. " +
		"custom value should be defined as key:value pairs divided with a comma, e.g. key1:value1,key2:value2. Format of the mask is required to be (order of values is user definable): .../key1/key2/year/day/hour"
	emptyMaskMember     = "undefined mask format %s: found empty member in position %d. Add the value or remove the extra slash."
	emptyMaskVersionKey = "undefined mask format %s: Found empty {} parentheses in position %d. They need to contain message attribute keys with at least one character."
)

// Common error messages -- used regardless of specific connector.
const (
	PullAPIRequestErrorMessage           = "Invalid request to initiate a Persistor pull!"
	GeneralInitializationErrorMessage    = "Failed to initialize the Persistor!"
	GeneralInitializationErrorMessageIdx = "Failed to initialize the Indexer!"
	GeneralValidationErrorMessage        = "Failed to validate the Persistor!"
	GeneralValidationErrorMessageIdx     = "Failed to validate the Indexer!"
	GeneralPullError                     = "An error occurred during the pull!"
	ReaderClosingErrorMessage            = "Failed to close the reader connector!"
)

// GetConfigErrorMessage generates a string regarding an error in configuration from the template.
func GetConfigErrorMessage(configName string) string {
	return fmt.Sprintf(configErrorMessageTemplate, configName)
}

// GetUsingDefaultWarningMessage generates a warning about a missing value in the configuration and
// using a default instead.
func GetUsingDefaultWarningMessage(configName string, settingName string, defaultValue string) string {
	return fmt.Sprintf(defaultValueUsedMessageTemplate, configName, settingName, defaultValue)
}

func GetInvalidMaskError(paramValue string) string {
	return fmt.Sprintf(invalidMask, paramValue)
}

func GetEmptyMaskMemberError(mask string, memberPos int) string {
	return fmt.Sprintf(emptyMaskMember, mask, memberPos)
}

func GetEmptyMaskAttributeError(mask string, memberPos int) string {
	return fmt.Sprintf(emptyMaskVersionKey, mask, memberPos)
}
