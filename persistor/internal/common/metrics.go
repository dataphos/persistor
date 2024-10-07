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

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dataphos/lib-streamproc/pkg/streamproc"
	"github.com/dataphos/persistor/internal/common/log"
)

const (
	MaxSummaryAge           = 5 * time.Minute
	ServerShutdownTimeout   = 5 * time.Second
	ServerReadHeaderTimeout = 5 * time.Second
)

type trackedPrometheusMetrics struct {
	processedCountProm  prometheus.Counter
	bytesProcessedProm  prometheus.Counter
	processingTimesProm prometheus.Summary

	failedCountProm           prometheus.Counter
	failedBytesProcessedProm  prometheus.Counter
	failedProcessingTimesProm prometheus.Summary
}

// trackedMetrics holds all the metrics that we need to update
//
//nolint:gochecknoglobals // accessed by various functions in this file, so it's easier for it to be global.
var trackedMetrics trackedPrometheusMetrics

// metricsInitialized is used in tests to avoid panic when metrics server is not run
//
//nolint:gochecknoglobals // same as above
var metricsInitialized bool

func initMetrics(productName string) {
	//nolint:gomnd // magic numbers hardcoded in this map are easier to read than 6 float constants.
	timeSummaryObjectives := map[float64]float64{ // processing time quantiles and their absolute errors.
		0.5:  0.05,
		0.9:  0.01,
		0.99: 0.001,
	}

	trackedMetrics.processedCountProm = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: productName,
		Name:      "processed_messages_total",
		Help:      "The total number of processed messages",
	})
	trackedMetrics.bytesProcessedProm = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: productName,
		Name:      "processed_bytes_total",
		Help:      "The total number of processed bytes",
	})
	trackedMetrics.processingTimesProm = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:  productName,
		Name:       "processing_times_milliseconds",
		Help:       "Processing times of published messages in milliseconds",
		MaxAge:     MaxSummaryAge,
		Objectives: timeSummaryObjectives,
	})

	trackedMetrics.failedCountProm = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: productName,
		Name:      "failed_messages_total",
		Help:      "The total number of failed messages",
	})
	trackedMetrics.failedBytesProcessedProm = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: productName,
		Name:      "failed_processed_bytes_total",
		Help:      "The total number of failed processed bytes",
	})
	trackedMetrics.failedProcessingTimesProm = promauto.NewSummary(prometheus.SummaryOpts{
		Namespace:  productName,
		Name:       "failed_processing_times_milliseconds",
		Help:       "Processing times of failed messages in milliseconds",
		MaxAge:     MaxSummaryAge,
		Objectives: timeSummaryObjectives,
	})
	metricsInitialized = true
}

// UpdateSuccessMetrics updates Prometheus metrics: processedCountProm, bytesProcessedProm, and processingTimesProm.
func UpdateSuccessMetrics(messages ...streamproc.Message) {
	if !metricsInitialized {
		return
	}

	trackedMetrics.processedCountProm.Add(float64(len(messages)))

	tNow := time.Now()
	payloadLengthSum := 0

	for _, message := range messages {
		messageProcessingTime := tNow.Sub(message.IngestionTime).Milliseconds()
		trackedMetrics.processingTimesProm.Observe(float64(messageProcessingTime))

		payloadLengthSum += len(message.Data)
	}

	trackedMetrics.bytesProcessedProm.Add(float64(payloadLengthSum))
}

// UpdateFailureMetrics updates Prometheus metrics: nackCountProm, nackBytesProcessedProm, and nackProcessingTimesProm.
func UpdateFailureMetrics(messages ...streamproc.Message) {
	if !metricsInitialized {
		return
	}

	trackedMetrics.failedCountProm.Add(float64(len(messages)))

	tNow := time.Now()
	payloadLengthSum := 0

	for _, message := range messages {
		msgNackProcessingTime := tNow.Sub(message.IngestionTime).Milliseconds()
		trackedMetrics.failedProcessingTimesProm.Observe(float64(msgNackProcessingTime))

		payloadLengthSum += len(message.Data)
	}

	trackedMetrics.failedBytesProcessedProm.Add(float64(payloadLengthSum))
}

// RunMetricsServer runs an http server on which Prometheus metrics are being exposed.
// All metrics that are registered to default Prometheus Registry are displayed at:
// "localhost:2112/metrics" endpoint.
func RunMetricsServer(productName string) *http.Server {
	initMetrics(productName)

	http.Handle("/metrics", promhttp.Handler())

	port := ":2112"

	srv := &http.Server{
		Addr:              port,
		ReadHeaderTimeout: ServerReadHeaderTimeout, // Set an appropriate timeout value.
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("Error serving metrics: "+err.Error(), MetricsServerError)
		}
	}()

	log.Info(fmt.Sprintf("exposed metrics at port %s", port))

	return srv
}
