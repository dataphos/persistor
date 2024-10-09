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

// package mongowriter provides methods to write data to mongo.
import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoConfig struct {
	ConnectionString string
	Database         string
	Collection       string
	AuthMechanism    string
	AuthSource       string
	Username         string
	Password         string
	SessionTokenName string
	SessionTokenVal  string
}

var (
	ErrMongoConnectionStringInvalid = errors.New("error validating mongo connection string")
	ErrMongoConnectionFailed        = errors.New("error connecting to mongo")
	ErrMongoPingFailed              = errors.New("can't ping mongo database")
)

func (mongoConfig *MongoConfig) Validate() error {
	ctx := context.Background()

	mongoClientOpts := options.Client().ApplyURI(mongoConfig.ConnectionString)

	err := mongoClientOpts.Validate()
	if err != nil {
		return fmt.Errorf("%w: '%s'", ErrMongoConnectionStringInvalid, mongoConfig.ConnectionString)
	}

	client, err := mongo.Connect(ctx, mongoClientOpts)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrMongoConnectionFailed, err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return fmt.Errorf("%w", ErrMongoPingFailed)
	}

	return nil
}
