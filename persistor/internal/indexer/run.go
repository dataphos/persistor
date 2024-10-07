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

package indexer

import (
	"context"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/kkyr/fig"
	"github.com/pkg/errors"

	"github.com/dataphos/lib-shutdown/pkg/graceful"
	"github.com/dataphos/persistor/internal/common"
	"github.com/dataphos/persistor/internal/common/log"
	"github.com/dataphos/persistor/internal/config"
	"github.com/dataphos/persistor/internal/persistor"
	"github.com/dataphos/persistor/internal/sender"
	mongowriter "github.com/dataphos/persistor/internal/writer/mongo"
)

const indexerConfigFileName = "config/indexer.toml"

// Run receives persistor message metadata and writes it into mongo
func Run() {
	indexerConfig := &config.IndexerConfig{}
	tomlError := fig.Load(indexerConfig, fig.File(indexerConfigFileName), fig.UseEnv(""))

	if tomlError != nil {
		log.Errorw("Error reading configuration from file!", common.ConfigurationError, log.F{log.ErrorFieldKey: tomlError.Error()})

		return
	}

	indexerConfig.Log()
	errorList := indexerConfig.Validate()

	if len(errorList) > 0 {
		log.Error("Error validating indexer config: "+strings.Join(errorList, "|"), common.ConfigurationError)

		return
	}
	ctx := context.Background()
	handler, err := InitializeIndexer(ctx, indexerConfig)

	defer handler.cancel()

	if err != nil {
		log.Error(err.Error(), common.InitializationError)

		return
	}
	srv := common.RunMetricsServer("indexer")

	runCtx := graceful.WithSignalShutdown(context.Background()) // context for streamproc executor, will be canceled when a signal is sent

	go func() {
		// when the context is canceled, wait for handler to clean up any messages still left inside
		<-runCtx.Done()
		handler.End()
	}()

	log.Info("Indexer starting...")

	switch indexerConfig.Reader.Type {
	case config.TypePubSub:
		persistor.RunBatchMessageReceiverHandler(runCtx, handler, indexerConfig.Reader.PubSub, indexerConfig.BatchSettings)
	case config.TypeServiceBus:
		persistor.RunBatchMessageReceiverHandler(runCtx, handler, indexerConfig.Reader.ServiceBus, indexerConfig.BatchSettings)
	case config.TypeKafka:
		persistor.RunBatchRecordIteratorHandler(runCtx, handler, indexerConfig.Reader.Kafka, indexerConfig.BatchSettings)
	}

	handler.cancel() // to return from the goroutine calling End

	// context used to stop metrics server
	ctx, cancel := context.WithTimeout(context.Background(), common.ServerShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Error(errors.Wrap(err, "metrics server shutdown failed").Error(), common.MetricsServerError)
	}

	log.Info("Indexer exiting")
}

func InitializeIndexer(ctx context.Context, indexerConfig *config.IndexerConfig) (indexer *Indexer, err error) {
	indexer = &Indexer{
		validate: validator.New(),
	}
	indexer.handlerCtx, indexer.cancel = context.WithCancel(context.Background())

	writer, err := mongowriter.NewMongoWriter(ctx, indexerConfig.Mongo)
	if err != nil {
		return
	}

	indexer.mongoWriter = writer

	if indexerConfig.DeadLetterEnabled {
		indexer.deadLetterTopic, indexer.tolerateDeadLetterErrors, err = sender.NewTopic(ctx, indexerConfig.Sender, indexerConfig.Sender.DeadLetterTopic,
			indexerConfig.BatchSettings.BatchSize, indexerConfig.BatchSettings.BatchMemory)
	}

	return
}
