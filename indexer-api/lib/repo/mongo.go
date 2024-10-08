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

package repo

import (
	"context"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	ObjectID             = "_id"
	UniqueID             = "unique_id"
	BrokerMessageID      = "broker_msg_id"
	LocationKey          = "location_key"
	LocationPosition     = "location_position"
	PublishTime          = "publish_time"
	BrokerID             = "broker_id"
	BusinessSourceKey    = "business_source_key"
	BusinessObjectKey    = "business_object_key"
	IndexSourceKey       = "index_source_key"
	OrderingKey          = "ordering_key"
	AdditionalMetadata   = "additional_metadata"
	IngestionTime        = "ingestion_time"
	IndexerIngestionTime = "indexer_ingestion_time"
	ConfirmationFlag     = "confirmation_flag"
)

type repo struct {
	database *mongo.Database
}

type config struct {
	connectionString string
	databaseName     string
}

func FromEnv() (Repository, error) {
	return newRepo(loadConfigFromEnv())
}

const (
	ConnectionStringEnv = "CONN"
	DatabaseNameEnv     = "DATABASE"
)

func loadConfigFromEnv() config {
	return config{
		connectionString: os.Getenv(ConnectionStringEnv),
		databaseName:     os.Getenv(DatabaseNameEnv),
	}
}

// newRepo constructs a new repo.
func newRepo(config config) (*repo, error) {
	ctx := context.Background()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(config.connectionString))
	if err != nil {
		return nil, err
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &repo{
		database: client.Database(config.databaseName),
	}, nil
}

// Get gets the metadata with given unique id.
func (repo *repo) Get(ctx context.Context, mongoCollection, id string, attributesList []string) ([]Message, error) { //nolint:varnamelen // fine length
	projection := bson.D{
		{Key: ObjectID, Value: 0},
	}
	for _, attribute := range attributesList {
		projection = append(projection, bson.E{Key: attribute, Value: 1})
	}

	filter := bson.M{
		UniqueID: id,
	}

	cursor, err := repo.database.Collection(mongoCollection).Find(
		ctx,
		filter,
		options.Find().SetProjection(projection),
	)
	if err != nil {
		return nil, err
	}

	var messages []Message

	err = cursor.All(ctx, &messages)
	if err != nil {
		return nil, err
	}

	return messages, nil
}

func (repo *repo) GetAll(ctx context.Context, mongoCollection string, ids, attributesList []string) ([]Message, error) {
	projection := bson.D{
		{Key: ObjectID, Value: 0},
	}
	for _, attribute := range attributesList {
		projection = append(projection, bson.E{Key: attribute, Value: 1})
	}

	filter := bson.M{
		UniqueID: bson.M{
			"$in": ids,
		},
	}

	cursor, err := repo.database.Collection(mongoCollection).Find(
		ctx,
		filter,
		options.Find().SetProjection(projection),
	)
	if err != nil {
		return nil, err
	}

	var messages []Message

	err = cursor.All(ctx, &messages)
	if err != nil {
		return nil, err
	}

	return messages, nil
}

// GetAllInInterval returns a collection of a subset of metadata for the specific time interval and broker_id.
// The metadata subset that is returned consists of message_id, message count, and location (both path and position).
func (repo *repo) GetAllInInterval(ctx context.Context, mongoCollection string, to, from time.Time, brokerID string, limit, offset int, attributesList []string) ([]Message, error) { //nolint:varnamelen // fine length
	projection := bson.D{
		{Key: ObjectID, Value: 0},
	}
	for _, attribute := range attributesList {
		projection = append(projection, bson.E{Key: attribute, Value: 1})
	}

	filter := bson.M{
		PublishTime: bson.M{
			"$gte": to,
			"$lt":  from,
		},
		BrokerID: brokerID,
	}

	cursor, err := repo.database.Collection(mongoCollection).Find(
		ctx,
		filter,
		options.Find().SetProjection(projection),
		options.Find().SetLimit(int64(limit)),
		options.Find().SetSkip(int64(offset)),
	)
	if err != nil {
		return nil, err
	}

	var messages []Message

	err = cursor.All(ctx, &messages)
	if err != nil {
		return nil, err
	}

	return messages, nil
}

// GetAllInIntervalDocumentCount returns the count of documents that would be returned by the GetAllInInterval.
func (repo *repo) GetAllInIntervalDocumentCount(ctx context.Context, mongoCollection string, to, from time.Time, brokerID string) (int64, error) { //nolint:varnamelen // fine length
	filter := bson.M{
		PublishTime: bson.M{
			"$gte": to,
			"$lt":  from,
		},
		BrokerID: brokerID,
	}

	count, err := repo.database.Collection(mongoCollection).CountDocuments(ctx, filter)
	if err != nil {
		return -1, err
	}

	return count, err
}

// GetQueried returns a collection of metadata that satisfy the given restrictions.
func (repo *repo) GetQueried(ctx context.Context, queryInfo QueryInformation) ([]Message, error) {
	projection := bson.D{
		{Key: ObjectID, Value: 0},
	}
	for _, attribute := range queryInfo.AttributesList {
		projection = append(projection, bson.E{Key: attribute, Value: 1})
	}

	cursor, err := repo.database.Collection(queryInfo.MongoCollection).Find(
		ctx,
		bson.M{"$or": queryInfo.Filters},
		options.Find().SetProjection(projection),
		options.Find().SetLimit(int64(queryInfo.Limit)),
		options.Find().SetSkip(int64(queryInfo.Offset)),
	)
	if err != nil {
		return nil, err
	}

	var messages []Message

	err = cursor.All(ctx, &messages)
	if err != nil {
		return nil, err
	}

	return messages, nil
}

// GetQueriedDocumentCount returns the count of documents that would be returned by GetQueried.
func (repo *repo) GetQueriedDocumentCount(ctx context.Context, mongoCollection string, filters []map[string]interface{}) (int64, error) {
	count, err := repo.database.Collection(mongoCollection).CountDocuments(ctx, bson.M{"$or": filters})
	if err != nil {
		return -1, err
	}

	return count, nil
}
