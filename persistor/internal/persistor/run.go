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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kkyr/fig"
	"github.com/pkg/errors"

	"github.com/dataphos/lib-brokers/pkg/broker"
	"github.com/dataphos/lib-shutdown/pkg/graceful"
	"github.com/dataphos/lib-streamproc/pkg/streamproc"
	"github.com/dataphos/persistor/internal/common"
	"github.com/dataphos/persistor/internal/common/log"
	"github.com/dataphos/persistor/internal/config"
)

const persistorConfigFileName = "config/persistor.toml"

func Run() {
	persistorConfig := &config.PersistorConfig{}
	tomlError := fig.Load(persistorConfig, fig.File(persistorConfigFileName), fig.UseEnv(""))

	if tomlError != nil {
		log.Errorw("Error reading configuration from file!", common.ConfigurationError, log.F{log.ErrorFieldKey: tomlError.Error()})

		return
	}

	persistorConfig.Log()
	errorList := persistorConfig.Validate()

	if len(errorList) > 0 {
		log.Error("Error validating persistor config: "+strings.Join(errorList, "|"), common.ConfigurationError)

		return
	}

	handler, err := InitializePersistor(persistorConfig)
	defer handler.Cancel()

	if err != nil {
		log.Errorw("Error initializing Persistor", common.InitializationError, log.F{log.ErrorFieldKey: err.Error()})

		return
	}

	srv := common.RunMetricsServer("persistor")

	runCtx := graceful.WithSignalShutdown(context.Background()) // context for streamproc executor, will be canceled when a signal is sent

	go func() {
		// when the context is canceled, wait for handler to clean up any messages still left inside
		<-runCtx.Done()
		handler.End()
	}()

	log.Info("Persistor starting...")

	switch persistorConfig.Reader.Type {
	case config.TypePubSub:
		RunBatchMessageReceiverHandler(runCtx, handler, persistorConfig.Reader.PubSub, persistorConfig.BatchSettings)
	case config.TypeServiceBus:
		RunBatchMessageReceiverHandler(runCtx, handler, persistorConfig.Reader.ServiceBus, persistorConfig.BatchSettings)
	case config.TypeKafka:
		RunBatchRecordIteratorHandler(runCtx, handler, persistorConfig.Reader.Kafka, persistorConfig.BatchSettings)
	}

	handler.Cancel() // to return from the goroutine calling End

	// context used to stop metrics server
	ctx, cancel := context.WithTimeout(context.Background(), common.ServerShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Error(errors.Wrap(err, "metrics server shutdown failed").Error(), common.MetricsServerError)
	}

	log.Info("Persistor exiting")
}

func RunBatchMessageReceiverHandler(runCtx context.Context, handler streamproc.BatchHandler, receiverConfig interface{}, batchSettings config.BatchSettings) {
	var (
		err      error
		receiver broker.BatchedReceiver
	)

	switch specificReceiverConfig := receiverConfig.(type) {
	case config.PubSubReceiverConfig:
		receiver, err = NewPubSubReceiver(runCtx, specificReceiverConfig, batchSettings)
	case config.ServiceBusReceiverConfig:
		receiver, err = NewServiceBusReceiver(specificReceiverConfig, batchSettings)
	default:
		log.Error("Receiver type unknown", common.BrokerInvalidConfigError)

		return
	}

	if err != nil {
		log.Fatal(err.Error(), common.InitializationError)
	}

	runOptions := DefineRunOptions()
	executor := streamproc.NewBatchedReceiverExecutor(handler)

	err = executor.Run(runCtx, receiver, runOptions...)
	if err != nil {
		log.Error(err.Error(), common.ProcessingError)
	}
}

func RunBatchRecordIteratorHandler(runCtx context.Context, handler streamproc.BatchHandler, receiverConfig interface{}, batchSettings config.BatchSettings) {
	var (
		iterator broker.BatchedIterator
		err      error
	)

	switch specificReceiverConfig := receiverConfig.(type) {
	case config.KafkaIteratorConfig:
		iterator, err = NewKafkaIterator(runCtx, specificReceiverConfig, batchSettings)
	default:
		log.Error("Receiver type unknown", common.BrokerInvalidConfigError)

		return
	}

	if err != nil {
		log.Fatal(err.Error(), common.InitializationError)
	}

	runOptions := DefineRunOptions()

	executor := streamproc.NewBatchExecutor(handler)

	err = executor.Run(runCtx, iterator, runOptions...)
	if err != nil {
		log.Error(err.Error(), common.ProcessingError)
	}
}

// DefineRunOptions specifies what happens for each error type and whether the run should continue (rough draft)
// this will maybe depend on the broker being used
func DefineRunOptions() []streamproc.RunOption {
	runOptions := []streamproc.RunOption{
		streamproc.WithErrThreshold(50),
		streamproc.WithErrInterval(1 * time.Minute),
		streamproc.WithNumRetires(0),
		streamproc.OnPullErr(func(err error) streamproc.FlowControl {
			log.Info("Error while pulling messages: " + err.Error())

			return streamproc.FlowControlStop
		}),
		streamproc.OnProcessErr(func(err error) streamproc.FlowControl {
			log.Info(fmt.Sprintf("Error occurred during processing: %v\n", err.Error()))

			return streamproc.FlowControlContinue
		}),
		streamproc.OnUnrecoverable(func(err error) streamproc.FlowControl {
			log.Errorw("Unrecoverable error encountered", common.ProcessingError, log.F{"error": err})

			return streamproc.FlowControlStop
		}),
		streamproc.OnThresholdReached(func(err error, count, threshold int64) streamproc.FlowControl {
			log.Info(fmt.Sprintf("Error threshold reached (%d >= %d)", count, threshold))

			return streamproc.FlowControlStop
		}),
	}

	return runOptions
}
