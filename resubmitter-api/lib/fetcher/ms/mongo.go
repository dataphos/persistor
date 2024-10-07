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

package ms

import (
	"context"
	"io"
	"os"
	"strings"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/dataphos/persistor-resubmitter-api/common/errcodes"
	"github.com/dataphos/persistor-resubmitter-api/common/log"
	"github.com/dataphos/persistor-resubmitter-api/lib/fetcher"
)

const (
	connectionStringEnv    = "MONGO_CONNECTION_STRING"
	storageDatabaseNameEnv = "MONGO_STORAGE_DATABASE"

	idField = "id"
)

type MongoRecord struct {
	id       string `bson:"id"` //nolint:unused // mongo decoder might use this field
	data     []byte `bson:"data"`
	metadata bson.M `bson:"metadata"` //nolint:unused // mongo decoder might use this field
}

type Storage struct {
	client *mongo.Database
}

func FromEnv() (fetcher.BlobStorage, error) {
	return New(
		os.Getenv(storageDatabaseNameEnv),
	)
}

func New(databaseName string) (fetcher.BlobStorage, error) {
	client, err := newClient(databaseName, context.Background())
	if err != nil {
		return nil, errors.Wrap(err, "couldn't create the mongo database client")
	}

	return &Storage{
		client: client,
	}, nil

}

func newClient(databaseName string, ctx context.Context) (*mongo.Database, error) {
	connectionString := os.Getenv(connectionStringEnv)

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connectionString))
	if err != nil {
		return nil, errors.Wrap(err, "error while connecting to the mongo client")
	}

	database := client.Database(databaseName)

	return database, nil
}

func (ms *Storage) Fetch(ctx context.Context, object string) ([]byte, error) {
	reader, err := ms.Reader(ctx, object)
	if err != nil {
		return nil, errors.Wrap(err, "can't acquire reader")
	}

	data, err := fetcher.Read(reader)
	if err != nil {
		return nil, errors.Wrap(err, "reader failed")
	}

	err = reader.Close()
	if err != nil {
		log.Debug(errors.Wrap(err, "can't close reader object").Error(), errcodes.Fetcher)
	}

	return data, nil
}

func (ms *Storage) Reader(ctx context.Context, object string) (io.ReadCloser, error) {
	collection, msgID, err := extractCollectionAndMsgIDFromLocationKey(object)
	if err != nil {
		return nil, err
	}

	document := ms.client.Collection(collection).FindOne(
		ctx,
		bson.M{
			idField: msgID,
		},
	)

	return &mongoReadCloser{singleResult: document}, nil
}

type mongoReadCloser struct {
	singleResult *mongo.SingleResult
	data         []byte
	pos          int
}

func (rc *mongoReadCloser) Read(p []byte) (int, error) {
	if rc.data == nil {
		var document MongoRecord
		err := rc.singleResult.Decode(&document)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				return 0, errors.Wrap(err, "mongo document not found")
			}
			return 0, errors.Wrap(err, "error during mongo document decoding")
		}

		rc.data = document.data
	}

	if rc.pos >= len(rc.data) {
		return 0, io.EOF
	}

	n := copy(p, rc.data[rc.pos:])
	rc.pos += n
	return n, nil
}

func (rc *mongoReadCloser) Close() error {
	rc.data = nil
	rc.pos = 0
	return nil
}

func extractCollectionAndMsgIDFromLocationKey(locationKey string) (string, string, error) {
	parameters := strings.Split(locationKey, "/")
	if len(parameters) != 2 {
		return "", "", errors.New("invalid location key: the location must be of format %collection%/%msg_id%")
	}
	return parameters[0], parameters[1], nil
}
