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
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"

	"github.com/dataphos/persistor-indexer-api/common/log"
)

type ServerConfig struct {
	Addr              string
	TLS               bool
	ReadHeaderTimeout time.Duration
}

var DefaultServerConfig = ServerConfig{
	Addr:              ":8080",
	TLS:               false,
	ReadHeaderTimeout: 2 * time.Second,
}

type ServerOption func(*ServerConfig)

func WithAddr(addr string) ServerOption {
	return func(config *ServerConfig) {
		config.Addr = addr
	}
}

func WithTLS(tls bool) ServerOption {
	return func(config *ServerConfig) {
		config.TLS = tls
	}
}

func WithReadHeaderTimeout(timeout time.Duration) ServerOption {
	return func(config *ServerConfig) {
		config.ReadHeaderTimeout = timeout
	}
}

const (
	CertsEnv = "CERTS"

	serverAddressEnv     = "IDX_API_SERVER_ADDRESS"
	tlsEnv               = "IDX_API_USE_TLS"
	readHeaderTimeoutEnv = "IDX_API_SERVER_TIMEOUT"
)

func ServeWithOptionsFromEnv(handler *Handler) {
	serverOptions, err := loadServerOptionsFromEnv()
	if err != nil {
		log.Fatal("error while reading server options from env: "+err.Error(), 500)
	}

	Serve(handler, serverOptions...)
}

func loadServerOptionsFromEnv() ([]ServerOption, error) {
	var serverOptions []ServerOption

	if serverAddress := os.Getenv(serverAddressEnv); serverAddress != "" {
		serverOptions = append(serverOptions, WithAddr(serverAddress))
	}

	if tlsStr := os.Getenv(tlsEnv); tlsStr != "" {
		tls, err := strconv.ParseBool(tlsStr)
		if err != nil {
			return nil, errors.Wrap(err, log.ParsingEnvVariableFailed(tlsEnv))
		}

		serverOptions = append(serverOptions, WithTLS(tls))
	}

	if timeoutStr := os.Getenv(readHeaderTimeoutEnv); timeoutStr != "" {
		timeout, err := time.ParseDuration(timeoutStr)
		if err != nil {
			return nil, errors.Wrap(err, log.ParsingEnvVariableFailed(readHeaderTimeoutEnv))
		}

		serverOptions = append(serverOptions, WithReadHeaderTimeout(timeout))
	}

	return serverOptions, nil
}

// Serve constructs the router and runs the API.
func Serve(handler *Handler, opts ...ServerOption) {
	config := DefaultServerConfig
	for _, opt := range opts {
		opt(&config)
	}

	router := gin.Default()

	var waitGroup sync.WaitGroup

	registerIndexerEndpoints(handler, router, &waitGroup)

	srv := &http.Server{
		Addr:              config.Addr,
		Handler:           router,
		ReadHeaderTimeout: config.ReadHeaderTimeout,
	}

	go startServer(srv, config)

	// Wait for interrupt signal to gracefully shut down the server.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	initiateGracefulShutdown(srv, &waitGroup)
}

func registerIndexerEndpoints(handler *Handler, router *gin.Engine, waitGroup *sync.WaitGroup) {
	router.GET("/exact/:mongo_collection/:id", func(context *gin.Context) {
		waitGroup.Add(1)
		handler.GetUnique(context)
		waitGroup.Done()
	})

	router.POST("/all/:mongo_collection", func(context *gin.Context) {
		waitGroup.Add(1)
		handler.GetAll(context)
		waitGroup.Done()
	})

	router.GET("/range/:mongo_collection/:id", func(context *gin.Context) {
		waitGroup.Add(1)
		handler.GetAllInInterval(context)
		waitGroup.Done()
	})

	router.POST("/query/:mongo_collection", func(context *gin.Context) {
		waitGroup.Add(1)
		handler.GetQueried(context)
		waitGroup.Done()
	})
}

func startServer(srv *http.Server, config ServerConfig) {
	func() {
		if !config.TLS {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatal(err.Error(), 500)
			}
		} else {
			certsPath := os.Getenv(CertsEnv)
			if err := srv.ListenAndServeTLS(certsPath+"/server.crt", certsPath+"/server.key"); err != nil && err != http.ErrServerClosed {
				log.Fatal(err.Error(), 500)
			}
		}
	}()
}

func initiateGracefulShutdown(srv *http.Server, waitGroup *sync.WaitGroup) {
	log.Info("graceful shut down initiated, waiting for all goroutines to finish")

	// Waiting for all jobs to be done.
	waitGroup.Wait()

	log.Info("all goroutines finished, ready for shutdown")

	// New context which gives the server 5 seconds to shut down.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	if err := srv.Shutdown(ctx); err != nil {
		cancel()
		log.Fatal("error during server shutdown: "+err.Error(), 500)
	}

	log.Info("server exiting")
	cancel()
}
