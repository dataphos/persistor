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

package mongowriter

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/dataphos/persistor/internal/common"
	"github.com/dataphos/persistor/internal/common/log"
)

// MongoBatchWriter The Mongo batch writer.
// Not a BlobWriter
type MongoBatchWriter struct {
	collection *mongo.Collection
}

// Write writes records to mongo. Returns a slice of failed indices, or a nil slice if they all failed.
func (writer *MongoBatchWriter) Write(ctx context.Context, records []interface{}) ([]int, error) {
	if len(records) == 0 {
		return nil, nil
	}

	// Send the batch to Mongo.
	_, errWrite := writer.collection.InsertMany(ctx, records, options.InsertMany().SetBypassDocumentValidation(true),
		options.InsertMany().SetOrdered(false))
	if errWrite == nil {
		// everything okay
		return nil, nil
	}

	var errMongo mongo.BulkWriteException
	// Check if the error is a BulkWriteException.
	// If not, assume the entire writing process failed.
	if ok := errors.As(errWrite, &errMongo); !ok {
		return nil, fmt.Errorf("mongo writer: %w", errWrite)
	}

	// iterate over the write error and return the indices of records that failed to write
	failedIndices := make([]int, len(errMongo.WriteErrors))
	for i, bulkErr := range errMongo.WriteErrors {
		failedIndices[i] = bulkErr.Index
	}

	return failedIndices, fmt.Errorf("mongo writer: %w", errMongo)
}

// NewMongoWriter Constructor.
func NewMongoWriter(ctx context.Context, config MongoConfig) (*MongoBatchWriter, error) {
	var err error

	// Set the URI used to connect to Mongo.
	mongoClientOpts := options.Client().ApplyURI(config.ConnectionString)

	// If additional authentication options have been set, attempt to load them into
	// the client configuration.
	if config.AuthOptionsSet() {
		// The minimum (as far as I can tell) is generally username, password and AuthMechanism.
		// If left empty, AuthMechanism defaults to SCRAM.
		credentials := options.Credential{
			AuthMechanism: config.AuthMechanism,
			AuthSource:    config.AuthSource,
			Username:      config.Username,
			Password:      config.Password,
		}

		// Optionally set session token.
		// An example would be: "AWS_SESSION_TOKEN" as SessionTokenName, with the value being the token itself.
		// (In that case, the AuthMechanism needs to be set to "MONGODB-AWS".)
		if config.SessionTokenName != "" {
			credentials.AuthMechanismProperties = map[string]string{config.SessionTokenName: config.SessionTokenVal}
		}

		// Apply the authentication.
		mongoClientOpts.SetAuth(credentials)
	}

	// Connect to Mongo.
	client, err := mongo.Connect(ctx, mongoClientOpts)
	if err != nil {
		log.Debugw("Error while connecting to Mongo!", common.StorageInvalidConfigError, log.F{log.ErrorFieldKey: err.Error()})

		return nil, fmt.Errorf("connecting to mongo: %w", err)
	}

	// Try and ping the client.
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Debugw("Error while pinging the Mongo database!", common.StorageInitializationError, log.F{log.ErrorFieldKey: err.Error()})

		return nil, fmt.Errorf("pinging mongo: %w", err)
	}

	// Get the collection.
	mongoCollection := client.Database(config.Database).Collection(config.Collection)

	// Return the configured batch writer.
	return &MongoBatchWriter{
		collection: mongoCollection,
	}, nil
}

// AuthOptionsSet Checks if additional authentication options have been set.
func (mongoConfig *MongoConfig) AuthOptionsSet() bool {
	return mongoConfig.AuthMechanism != "" || mongoConfig.Username != ""
}
