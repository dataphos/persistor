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

package config

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	azureStorage "github.com/Azure/azure-sdk-for-go/storage"
)

func ValidateStorage(storage *StorageConfig, errorList *[]string) {
	if storage.Destination == "" {
		*errorList = append(*errorList, ErrorEmptyString("Storage.Destination"))

		return
	}

	switch storage.Type {
	case WriterGCS:
		ValidateGCSBucket(storage.Destination, errorList)
	case WriterABS:
		if storage.StorageAccountID == "" {
			*errorList = append(*errorList, ErrorEmptyString("StorageAccountID"))
		} else {
			ValidateABSContainer(storage.StorageAccountID, storage.Destination, errorList)
		}
	case "":
		*errorList = append(*errorList, ErrorEmptyString("STORAGE_TYPE"))
	default:
		*errorList = append(*errorList, fmt.Sprintf("Storage type %s not recognized", storage.Type))
	}
}

// ValidateGCSBucket checks does the bucket exist.
func ValidateGCSBucket(bucketID string, errorList *[]string) {
	ctx := context.Background()

	helperClient, err := storage.NewClient(ctx)
	if err != nil {
		*errorList = append(*errorList, fmt.Errorf("failed to get GCS client: %w", err).Error())

		return
	}

	defer func() {
		if helperClient != nil {
			err = helperClient.Close()
		}
	}()

	bucket := helperClient.Bucket(bucketID)

	_, err = bucket.Attrs(ctx)
	if err != nil {
		*errorList = append(*errorList, fmt.Sprintf("bucket %s does not exist", bucketID))
	}
}

func ValidateABSContainer(storageAccountID, containerID string, errorList *[]string) {
	valid := azureStorage.IsValidStorageAccount(storageAccountID)

	if !valid {
		*errorList = append(*errorList, fmt.Sprintf("azure storage account %s isn't valid", storageAccountID))

		return
	}

	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		*errorList = append(*errorList, fmt.Sprintf("Unable to create default azure credentials: %s", err.Error()))
	}

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", storageAccountID)

	client, err := azblob.NewClient(serviceURL, credential, nil)
	if err != nil {
		*errorList = append(*errorList, fmt.Sprintf("Failed to create Azure storage client for storage account %s", storageAccountID))
	}

	_, err = client.ServiceClient().NewContainerClient(containerID).GetProperties(context.Background(), nil)

	if err != nil {
		*errorList = append(*errorList, fmt.Sprintf("Storage container %s does not exist", containerID))
	}
}
