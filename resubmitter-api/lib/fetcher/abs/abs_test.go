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

package abs

import (
	"fmt"
	"testing"
)

func TestExtractContainerAndBlobFromLocationKey(t *testing.T) {
	const expectedContainer = "this-is-the-container-name"
	const expectedBlob = "this-is//the_file'name/.avro"

	const storageURL = "https://storagename.blob.core.windows.net"
	actualContainer, actualBlob := extractContainerAndBlobFromLocationKey(fmt.Sprintf("%s/%s/%s",
		storageURL, expectedContainer, expectedBlob))
	if expectedContainer != actualContainer {
		t.Fatalf("Expected bucket not the same as actual (%s != %s)", expectedContainer, actualContainer)
	}
	if expectedBlob != actualBlob {
		t.Fatalf("Expected object not the same as actual (%s != %s)", expectedBlob, actualBlob)
	}
}
