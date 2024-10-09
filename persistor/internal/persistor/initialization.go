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

// Package persistor contains the core Persistor logic.
package persistor

import (
	"context"

	"github.com/dataphos/persistor/internal/common"
	"github.com/dataphos/persistor/internal/config"
	"github.com/dataphos/persistor/internal/sender"
	"github.com/dataphos/persistor/internal/writer/azure"
	"github.com/dataphos/persistor/internal/writer/gcp"
)

func InitializePersistor(persistorConfig *config.PersistorConfig) (persistorHandler *Persistor, err error) {
	persistorHandler = &Persistor{}
	persistorHandler.HandlerCtx, persistorHandler.Cancel = context.WithCancel(context.Background())

	storage, err := NewStorageProperties(persistorConfig.Storage)
	if err != nil {
		return
	}

	persistorHandler.Storage = storage

	persistorHandler.IndexerEnabled, persistorHandler.DeadLetterActive = persistorConfig.IndexerEnabled, persistorConfig.DeadLetterEnabled

	if persistorConfig.IndexerEnabled {
		if persistorHandler.IndexerTopic, _, err = sender.NewTopic(persistorHandler.HandlerCtx, persistorConfig.Sender, persistorConfig.Sender.TopicID,
			persistorConfig.BatchSettings.BatchSize, persistorConfig.BatchSettings.BatchMemory); err != nil {
			return
		}
	}

	if persistorConfig.DeadLetterEnabled {
		if persistorHandler.DeadLetterTopic, persistorHandler.tolerateDeadLetterErrors, err = sender.NewTopic(persistorHandler.HandlerCtx, persistorConfig.Sender, persistorConfig.Sender.DeadLetterTopic,
			persistorConfig.BatchSettings.BatchSize, persistorConfig.BatchSettings.BatchMemory); err != nil {
			return
		}
	}

	persistorHandler.DeadLetterActive = persistorConfig.DeadLetterEnabled

	switch persistorConfig.Storage.Type {
	case config.WriterGCS:
		if persistorHandler.Writer, err = gcp.NewGCSAvroBatchWriter(common.AvroSchema, common.NewPersistorRecord); err != nil {
			return
		}
	case config.WriterABS:
		if persistorHandler.Writer, err = azure.NewABSAvroBatchWriter(persistorConfig.Storage.StorageAccountID, common.AvroSchema, common.NewPersistorRecord); err != nil {
			return
		}
	}

	return persistorHandler, err
}
