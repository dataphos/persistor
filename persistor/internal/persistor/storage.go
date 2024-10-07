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

package persistor

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dataphos/lib-streamproc/pkg/streamproc"
	"github.com/dataphos/persistor/internal/common"
	"github.com/dataphos/persistor/internal/common/log"
	"github.com/dataphos/persistor/internal/config"
)

const (
	gcsBucketProtocol = "gs://"
	absBucketProtocol = "https://"
)

const (
	// LeftAttributeSignal and RightAttributeSignal stand around message version keys in storage mask, for example: day/{country_origin}/hour.
	LeftAttributeSignal  = '{'
	RightAttributeSignal = '}'
	// MinMaskAttributeKeyLen minimum length of mask member that can be interpreted as version key (at least one character between curly brackets).
	MinMaskAttributeKeyLen = 3
)

const UnknownVersion = "unknown"

// MaskMember is a single element of the storage mask, separated from other keys by / slashes.
type MaskMember struct {
	// Key is the string via which the actual value is looked up.
	Key string
	// FromMessageAttributes determines whether the value should be extracted from the timestamp or looked up in the attributes of messages.
	FromMessageAttributes bool
}

// StorageProperties Some shared requirements between all Blob Storage Writers.
// (Ensures consistency in the output blob format!)
// This could be expanded by topic/subscription name, if we want
// those included in the path.
type StorageProperties struct {
	Config          config.StorageConfig
	Mask            []MaskMember
	CustomValuesMap map[string]string
	VersionKeys     []string
	OptionalPrefix  string
}

// NewStorageProperties initializes an StorageProperties from the config.
func NewStorageProperties(storageConfig config.StorageConfig) (StorageProperties, error) {
	var (
		err             error
		customValuesMap map[string]string
	)

	storage := StorageProperties{
		Config: storageConfig,
	}

	if customValuesMap, err = ValidateCustomValues(storageConfig.CustomValues); err != nil {
		return storage, err
	}

	storage.CustomValuesMap = customValuesMap

	if storageConfig.Mask == "" {
		storageConfig.Mask = "year/month/day/hour"
	}

	var maskError error
	if storage.Mask, storage.VersionKeys, maskError = GenerateMaskValuesFromMaskString(storageConfig.Mask); maskError != nil {
		return storage, maskError
	}

	storage.OptionalPrefix, err = storage.GenerateAndValidateOptionalPrefixFromMask()
	if err != nil {
		return storage, err
	}

	return storage, nil
}

// GenerateBlobName Construct the blob name.
// Optional prefix contains fields from mask that user has defined
// (bucketID, subID, topicID, custom values) without year, month, day, hour.
// Year, month, day, hour are concatenated to objectName also based on mask in this function.
// If mask isn't defined default storage name will be used, and it is: year/month/day/hour.
func (storage *StorageProperties) GenerateBlobName(msg streamproc.Message) string {
	currentTime := time.Now().UTC()
	objectName := storage.OptionalPrefix

	if len(storage.Mask) != 0 {
		for _, maskParam := range storage.Mask {
			objectName = fmt.Sprintf("%s%s",
				objectName, GenerateValueFromMaskParam(msg, maskParam, currentTime))
		}

		objectName = fmt.Sprintf("%s%s-%s.%s", objectName, storage.Config.Prefix, msg.ID, storage.Config.MsgExtension)
	} else {
		formattedTime := fmt.Sprintf("%02d/%02d/%02d/%02d",
			currentTime.Year(), currentTime.Month(), currentTime.Day(), currentTime.Hour())
		objectName = fmt.Sprintf("%s/%s-%s.%s",
			formattedTime, storage.Config.Prefix, msg.ID, storage.Config.MsgExtension)
	}

	return objectName
}

// GenerateMaskValuesFromMaskString splits mask on / and interprets keys inside {} brackets as message attribute keys.
func GenerateMaskValuesFromMaskString(mask string) ([]MaskMember, []string, error) { //nolint:gocritic // not naming these returns for now
	maskParams := strings.Split(mask, "/")
	maskMembers := make([]MaskMember, 0, len(maskParams))

	var messageVersions []string

	for paramNum, param := range maskParams {
		paramLen := len(param)
		// if param is empty then the user forgot something separated by or added an unneeded / character.
		if paramLen == 0 {
			return nil, nil, errors.New(log.GetEmptyMaskMemberError(mask, paramNum+1)) //nolint:goerr113 // unnecessary here
		}

		member := MaskMember{}

		lastCharPos := paramLen - 1

		if param[0] == LeftAttributeSignal && param[lastCharPos] == RightAttributeSignal {
			if paramLen >= MinMaskAttributeKeyLen {
				member.Key = param[1:lastCharPos]
				member.FromMessageAttributes = true

				messageVersions = append(messageVersions, member.Key)
				maskMembers = append(maskMembers, member)

				continue
			} else {
				// found just an empty {} but can't use that as version key.
				return nil, nil, errors.New(log.GetEmptyMaskAttributeError(mask, paramNum+1)) //nolint:goerr113 // unnecessary here
			}
		}

		member.Key = param
		member.FromMessageAttributes = false
		maskMembers = append(maskMembers, member)
	}

	return maskMembers, messageVersions, nil
}

// GenerateValueFromMaskParam Returns value for currentTime.
// Because user can define order of year, month, day and hour
// this function returns values in same order how it is defined.
func GenerateValueFromMaskParam(msg streamproc.Message, maskParam MaskMember, currentTime time.Time) string {
	if maskParam.FromMessageAttributes {
		if version, exists := msg.Attributes[maskParam.Key]; exists {
			return fmt.Sprintf("%s/", version)
		}

		return fmt.Sprintf("%s/", UnknownVersion)
	}

	switch maskParam.Key {
	case "year":
		return fmt.Sprintf("%02d/", currentTime.Year())
	case "month":
		return fmt.Sprintf("%02d/", currentTime.Month())
	case "day":
		return fmt.Sprintf("%02d/", currentTime.Day())
	case "hour":
		return fmt.Sprintf("%02d/", currentTime.Hour())
	default:
		return ""
	}
}

// GenerateAndValidateOptionalPrefixFromMask Returns optional prefix based on user-defined mask.
func (storage *StorageProperties) GenerateAndValidateOptionalPrefixFromMask() (optionalPrefix string, err error) {
	for _, maskParam := range storage.Mask {
		if maskParam.FromMessageAttributes {
			continue
		}

		lowerMaskParam := strings.ToLower(maskParam.Key)
		switch lowerMaskParam {
		case "year":
			err = nil
		case "month":
			err = nil
		case "day":
			err = nil
		case "hour":
			err = nil
		default:
			value, ok := storage.CustomValuesMap[lowerMaskParam]
			if !ok {
				errMsg := log.GetInvalidMaskError(maskParam.Key)
				// additionally check if maybe the user misspelled the version format.
				if strings.Contains(lowerMaskParam, string(LeftAttributeSignal)) || strings.Contains(lowerMaskParam, string(RightAttributeSignal)) {
					errMsg += ". Value contains } or { characters. Did you mean to apply versioning in a {myattributekey} format?"
				}

				return optionalPrefix, fmt.Errorf("storage config: %s", errMsg) //nolint:goerr113 // unnecessary here
			}

			optionalPrefix = fmt.Sprintf("%s%s/", optionalPrefix, value)
		}
	}

	return optionalPrefix, nil
}

var ErrReadingCustomValues = errors.New("reading custom values. Format of custom values should be -> key1:value1,key2:value2 ")

func ValidateCustomValues(customValues string) (map[string]string, error) {
	customValuesMap := make(map[string]string)
	if customValues == "" {
		return customValuesMap, nil
	}

	customValuesSplit := strings.Split(customValues, ",")

	for _, param := range customValuesSplit {
		temp := strings.Split(param, ":")
		if len(temp) != 2 || temp[0] == "" || temp[1] == "" {
			return customValuesMap, ErrReadingCustomValues
		}

		customValuesMap[strings.ToLower(strings.TrimSpace(temp[0]))] = strings.TrimSpace(temp[1])
	}

	return customValuesMap, nil
}

var ErrStorageTypeNotSupported = errors.New("storage type not supported")

// GetCompletePath generates location metadata parameter which will be set in blob
// objectPath is calculated with GenerateBlob() function and sent as input parameter to this function.
func (storage *StorageProperties) GetCompletePath(objectPath string) (string, error) {
	switch storage.Config.Type {
	case config.WriterABS:
		return fmt.Sprintf("%s%s.blob.core.windows.net/%s/%s", absBucketProtocol, storage.Config.StorageAccountID, storage.Config.Destination, objectPath), nil

	case config.WriterGCS:
		return fmt.Sprintf("%s%s/%s", gcsBucketProtocol, storage.Config.Destination, objectPath), nil
	}

	// config error which should be caught at initialization. Handling this properly anyway, just in case.
	return "", &common.FatalError{ErrStorageTypeNotSupported}
}

// ContainsVersionKey checks if versioning is active.
func (storage *StorageProperties) ContainsVersionKey() bool {
	return len(storage.VersionKeys) > 0
}
