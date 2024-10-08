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

// Package log implements the logging logic for Indexer-API.
package log

import (
	"fmt"
	"os"

	"github.com/dataphos/lib-logger/logger"
	"github.com/dataphos/lib-logger/standardlogger"
)

const (
	productName = "Indexer API"

	minimumLogLevelEnv = "MINIMUM_LOG_LEVEL"

	defaultMinimumLogLevel = infoLevel

	infoLevel  = "INFO"
	warnLevel  = "WARN"
	errorLevel = "ERROR"
)

var logLevels = map[string]logger.Level{
	infoLevel:  logger.LevelInfo,
	warnLevel:  logger.LevelWarn,
	errorLevel: logger.LevelError,
}

type (
	F = logger.F
	L = logger.L
)

type (
	NoErrorCodeWithoutFields   func(string)
	NoErrorCodeWithFields      func(string, F)
	WithErrorCodeWithoutFields func(string, uint64)
	WithErrorCodeWithFields    func(string, uint64, F)
)

var (
	Close func()
	Flush func()
)

var log logger.Log

var (
	Info  NoErrorCodeWithoutFields
	Warn  NoErrorCodeWithoutFields
	Error WithErrorCodeWithoutFields
	Fatal WithErrorCodeWithoutFields
	Panic WithErrorCodeWithoutFields
	Debug WithErrorCodeWithoutFields
)

var (
	Infow  NoErrorCodeWithFields
	Warnw  NoErrorCodeWithFields
	Errorw WithErrorCodeWithFields
	Fatalw WithErrorCodeWithFields
	Panicw WithErrorCodeWithFields
	Debugw WithErrorCodeWithFields
)

var PanicLogger func()

func init() {
	labels := logger.Labels{"product": productName}

	minimumLogLevel, initWarnMsg := getMinimumLogLevel()

	log = standardlogger.New(labels, standardlogger.WithLogLevel(minimumLogLevel))

	Info = log.Info
	Infow = log.Infow
	Warn = log.Warn
	Warnw = log.Warnw
	Error = log.Error
	Errorw = log.Errorw
	Fatal = log.Fatal
	Fatalw = log.Fatalw
	Panic = log.Panic
	Panicw = log.Panicw
	Debug = func(msg string, code uint64) {
		Info(msg + fmt.Sprintf(" [code %d]", code))
	}
	Debugw = func(msg string, code uint64, fields logger.F) {
		Infow(msg+fmt.Sprintf(" [code %d]", code), fields)
	}

	Close = log.Close
	Flush = log.Flush

	PanicLogger = log.PanicLogger

	if initWarnMsg != nil {
		Warn(*initWarnMsg)
	}
}

func getMinimumLogLevel() (logger.Level, *string) {
	var initWarnMsg string

	levelString, exists := os.LookupEnv(minimumLogLevelEnv)
	if !exists {
		initWarnMsg = UsingDefaultParameterValue(minimumLogLevelEnv, defaultMinimumLogLevel)

		return logLevels[defaultMinimumLogLevel], &initWarnMsg
	}

	level, supported := logLevels[levelString]
	if !supported {
		initWarnMsg = fmt.Sprintf("Value %v for %v is not supported, using %v",
			levelString, minimumLogLevelEnv, defaultMinimumLogLevel)

		return logLevels[defaultMinimumLogLevel], &initWarnMsg
	}

	return level, nil
}
