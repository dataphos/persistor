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

package main

import (
	"fmt"
	"os"

	"github.com/dataphos/persistor-resubmitter-api/common/log"
	"github.com/dataphos/persistor-resubmitter-api/lib/fetcher"
	"github.com/dataphos/persistor-resubmitter-api/lib/fetcher/abs"
	"github.com/dataphos/persistor-resubmitter-api/lib/fetcher/gs"
	"github.com/dataphos/persistor-resubmitter-api/lib/fetcher/ms"
	"github.com/dataphos/persistor-resubmitter-api/lib/indexer"
	"github.com/dataphos/persistor-resubmitter-api/lib/publisher"
	"github.com/dataphos/persistor-resubmitter-api/lib/publisher/kafka"
	"github.com/dataphos/persistor-resubmitter-api/lib/publisher/pubsub"
	"github.com/dataphos/persistor-resubmitter-api/lib/publisher/servicebus"
	"github.com/dataphos/persistor-resubmitter-api/lib/resubmitter"
	"github.com/dataphos/persistor-resubmitter-api/lib/serializer/avro"
)

var handler *resubmitter.Handler

func initializeStorage(storageType storageConnectorType) (fetcher.BlobStorage, error) {
	switch storageType {
	case nonDefinedStorageConnector:
		return nil, fmt.Errorf("storage runtime type not specified in '%s' environment variable", storageTypeEnv)
	case googleStorageConnector:
		return gs.FromEnv()
	case azureBlobStorageConnector:
		return abs.FromEnv()
	case mongoStorageConnector:
		return ms.FromEnv()
	default:
		return nil, fmt.Errorf("invalid storage runtime type given in initialization")
	}
}

func initializePublisher(publisherType publisherConnectorType) (publisher.Publisher, error) {
	switch publisherType {
	case nonDefinedPublisherConnector:
		return nil, fmt.Errorf("publisher runtime type not specified in '%s' environment variable", publisherTypeEnv)
	case googlePubSubConnector:
		return pubsub.FromEnv()
	case azureServiceBusConnector:
		return servicebus.FromEnv()
	case kafkaConnector:
		return kafka.FromEnv()
	default:
		return nil, fmt.Errorf("invalid publisher runtime type given in initialization")
	}
}

func main() {

	storage, err := initializeStorage(storageConnectorType(os.Getenv(storageTypeEnv)))
	if err != nil {
		log.Error(err.Error(), 0)
		return
	}

	pub, err := initializePublisher(publisherConnectorType(os.Getenv(publisherTypeEnv)))
	if err != nil {
		log.Error(err.Error(), 0)
		return
	}

	rsb, err := resubmitter.FromEnv(
		indexer.FromEnv(),
		storage,
		avro.New(),
		pub,
	)

	if err != nil {
		log.Error(fmt.Errorf("error during resubmitter creation: %w", err).Error(), 0)
		return
	}

	handler = resubmitter.NewHandler(rsb)

	resubmitter.ServeWithOptionsFromEnv(handler)
}
