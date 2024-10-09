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

package common

const (
	ConfigurationError  = 100
	InitializationError = 101
	ProcessingError     = 103
	/*
		Connectors error codes.
	*/

	// WriterError Generic error that occurs when the writer attempts to write the messages to storage.
	WriterError = 110

	// StorageMissingConfigError
	// Error that occurs when one of the mandatory variables is not set in
	// the configuration variables for the storage destination.
	StorageMissingConfigError = 550

	// StorageInvalidConfigError
	// Error that occurs when the given configuration is invalid; tossed if it's
	// found that the bucket doesn't exist during validation.
	StorageInvalidConfigError = 551

	// StorageInitializationError
	// Occurs when the storage fails to initialize.
	StorageInitializationError = 700

	// BrokerMissingConfigError
	// Error that occurs when one of the mandatory variables is not
	// set in the configuration variables for the Broker.
	BrokerMissingConfigError = 500

	// BrokerInvalidConfigError
	// Error that occurs when one of the given values for the broker configuration
	// is invalid (IE, value that should be a number is passed as a word/other string).
	BrokerInvalidConfigError = 501

	// BrokerInitializationError
	// Occurs when the broker fails to initialize.
	BrokerInitializationError = 300

	/*
		MessageProcessor error codes.
	*/

	// DefaultMessageProcessorError
	// Default message processor error code, used to bind the
	// default error processor.
	DefaultMessageProcessorError = 0

	// AvroSchemaInvalidError
	// Error that occurs for any writers utilizing the Avro format.
	// Is caused when attempting to use a completely random string as
	// an Avro schema (IE, when the string is not a valid Avro schema).
	AvroSchemaInvalidError = 951

	// AvroSchemaMismatchError
	// Error that occurs when the passed valid schema does not
	// correspond to the data structure passed to be written to
	// the chosen destination.
	AvroSchemaMismatchError = 952

	// DataLoadError
	// Error that occurs when a Message fails to be loaded into its
	// proper structure/record/document. Generally used when parsing
	// the message data to be written to Mongo.
	DataLoadError = 960 // Fails to load the messages into their proper structs.

	// MessageInvalidVersionError
	// Error that occurs when Message has wrong version.
	MessageInvalidVersionError = 307

	SenderInvalidMessage = 903

	SenderPublishError = 302

	SenderDeadLetterError = 303

	/*
		Core configuration errors.
	*/

	// HandlerMissingConfigError
	// Error that occurs when the Batch Handler's mandatory configuration
	// fields are not filled.
	HandlerMissingConfigError = 900

	// HandlerInvalidConfigError
	// Error that occurs when a Batch Handler's field is invalid (IE, when
	// a random string is passed to a configuration field that was supposed
	// to be numeric.)
	HandlerInvalidConfigError = 901

	// MetricsConfigError configuration error.
	MetricsConfigError = 980

	MetricsServerError = 981

	/*
		Generic error codes.

		These are usually used at-runtime, when a component whose root
		error cause is buried behind layers of abstraction (but logged).

		Ultimately used when triggering PANICS.
	*/

	// GeneralInitializationError General component initialization error.
	GeneralInitializationError = 905

	// PullInvalidConfigError General pull errors (used at runtime panic).
	PullInvalidConfigError = 907

	// PullInvalidRequestError Periodic Pull errors.
	PullInvalidRequestError = 430

	// ProcessError General process error.
	ProcessError = 910
	/*

		Docker-specific errors.

	*/

	// InvalidPersistorModeError
	// Occurs when the Persistor is initialized
	// in a mode that isn't supported.
	InvalidPersistorModeError = 540

	// InvalidPersistorConfigError
	// Occurs when the Dockerized version of the Persistor
	// gets invalid configuration.
	InvalidPersistorConfigError = 541

	// InvalidConnectorClosing
	// Occurs when the Dockerized version of the Persistor
	// cannot close the reader connector.
	InvalidConnectorClosing = 542

	/*

		HTTP server-specific errors.

	*/

	// HTTPRequestError
	// Occurs when the received HTTP request isn't valid.
	HTTPRequestError = 500

	// HTTPServerError
	// Occurs when the HTTP server is started or closed.
	HTTPServerError = 501

	// HTTPServerGracefulShutdownError
	// Occurs when the HTTP server is gracefully shutdown.
	HTTPServerGracefulShutdownError = 502
)
