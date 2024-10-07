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

// Package log contains the wrapper used for interacting with lib-logger,
// allowing for easier integration with the existing codebase.
//
// All that needs to be done is:
//
//	import "github.com/dataphos/persistor/internal/common/log"
//
// And then to log one can just call the methods from the 'log' module directly.
// For example:
//
//	log.Info(...)
package log

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"os"

	"github.com/dataphos/lib-logger/logger"
	"github.com/dataphos/lib-logger/standardlogger"
)

// Same across all products.
const (
	license      = "Enterprise"
	clientPrefix = "PERSISTOR_"
)

const (
	maxClientID = 100
)

// accepted environment values for log level configuration.
const (
	LevelSpecifier = "MINIMUM_LOG_LEVEL"
	InfoLevel      = "INFO"
	WarnLevel      = "WARN"
	ErrorLevel     = "ERROR"
	DefaultLevel   = InfoLevel
)

type (
	NoErrorCode             func(string)
	WithErrorCode           func(string, uint64)
	NoErrorCodeWithFields   func(string, logger.F)
	WithErrorCodeWithFields func(string, uint64, logger.F)
)

//nolint:gochecknoglobals // functions as global variables for easier calling and shorter code.
var (
	Info  NoErrorCode
	Warn  NoErrorCode
	Error WithErrorCode
	Fatal WithErrorCode
	Panic WithErrorCode
)

//nolint:gochecknoglobals // functions as global variables for easier calling and shorter code.
var (
	Infow  NoErrorCodeWithFields
	Warnw  NoErrorCodeWithFields
	Errorw WithErrorCodeWithFields
	Fatalw WithErrorCodeWithFields
	Panicw WithErrorCodeWithFields
)

//nolint:gochecknoglobals // functions as global variables for easier calling and shorter code.
var (
	Debug  WithErrorCode
	Debugw WithErrorCodeWithFields
)

//nolint:gochecknoglobals // functions as global variables for easier calling and shorter code.
var (
	Close       func()
	Flush       func()
	PanicLogger func()
)

type F = logger.F

// Initialization of logger.
//
//nolint:gochecknoinits // since the log package is only imported by Persistor and Indexer, this init has no external side effects.
func init() {
	var Log logger.Log //nolint:revive // capitalized to not be confused with package

	// load product name (e.g. PERSISTOR or INDEXER).
	productName, exists := os.LookupEnv("PRODUCT_NAME")
	if !exists {
		productName = "Undefined"
	}
	// Initialize labels (these will be present in all logs.)
	numClientID, err := rand.Int(rand.Reader, big.NewInt(maxClientID))
	if err != nil {
		Log.Warn("Failed to generate clientID for logger")
	}

	labels := logger.L{
		"product":  productName,
		"clientId": fmt.Sprintf("%s%d", clientPrefix, numClientID),
		"license":  license,
	}

	minimumLogLevel, logConfigWarnings := getLogConfig()

	// Create the logger.
	Log = standardlogger.New(labels, standardlogger.WithLogLevel(minimumLogLevel))

	// Make the logger's functions functions of the package itself.
	// This allows us to import functionality from the github.com/dataphos/lib-logger module and instead of doing:
	// log.Log.Info(...)
	// we can just use
	// log.Info(...)
	Info = Log.Info
	Warn = Log.Warn
	Error = Log.Error
	Fatal = Log.Fatal
	Panic = Log.Panic
	// Debug logs error using info, but appends the error code to the end of the error message.
	Debug = func(msg string, code uint64) {
		Info(msg + fmt.Sprintf(" [code %d]", code))
	}

	// Versions with fields.
	Infow = Log.Infow
	Warnw = Log.Warnw
	Errorw = Log.Errorw
	Fatalw = Log.Fatalw
	Panicw = Log.Panicw
	// Debugw is like Debug, but with fields.
	Debugw = func(msg string, code uint64, fields logger.F) {
		Infow(msg+fmt.Sprintf(" [code %d]", code), fields)
	}

	// Additional options.
	Close = Log.Close
	Flush = Log.Flush
	PanicLogger = Log.PanicLogger

	for _, w := range logConfigWarnings {
		Warn(w)
	}
}

// getLogConfig returns minimum log level based on environment variable.
// Possible levels are info, warn, and error. Defaults to error.
func getLogConfig() (level logger.Level, warnings []string) {
	levels := map[string]logger.Level{
		InfoLevel:  logger.LevelInfo,
		WarnLevel:  logger.LevelWarn,
		ErrorLevel: logger.LevelError,
	}

	levelString, exists := os.LookupEnv(LevelSpecifier)
	if !exists {
		warnings = append(warnings,
			GetUsingDefaultWarningMessage("log", LevelSpecifier, DefaultLevel))

		return levels[DefaultLevel], warnings
	}

	level, supported := levels[levelString]
	if supported {
		return level, warnings
	}

	warnings = append(warnings,
		fmt.Sprintf("Value %v for %v is not supported, using %v", levelString, LevelSpecifier, DefaultLevel))

	return levels[DefaultLevel], warnings
}
