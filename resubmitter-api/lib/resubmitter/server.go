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

package resubmitter

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

	"github.com/dataphos/persistor-resubmitter-api/common/errcodes"
	"github.com/dataphos/persistor-resubmitter-api/common/log"
)

type ServerConfig struct {
	Addr              string
	TLS               bool
	ReadHeaderTimeout time.Duration
}

var DefaultServerConfig = ServerConfig{
	Addr:              ":8081",
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

	serverAddressEnv     = "RSB_SERVER_ADDRESS"
	tlsEnv               = "RSB_USE_TLS"
	readHeaderTimeoutEnv = "RSB_SERVER_TIMEOUT"
)

func ServeWithOptionsFromEnv(handler *Handler) {
	serverOptions, err := loadServerOptionsFromEnv()
	if err != nil {
		log.Fatal("error while reading server options from env: "+err.Error(), errcodes.Server)
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

func Serve(handler *Handler, opts ...ServerOption) {
	config := DefaultServerConfig
	for _, opt := range opts {
		opt(&config)
	}

	router := gin.Default()
	var wg sync.WaitGroup
	registerResubmitterEndpoints(handler, router, &wg)

	srv := &http.Server{
		Addr:              config.Addr,
		Handler:           router,
		ReadHeaderTimeout: config.ReadHeaderTimeout,
	}

	go startServer(srv, config)

	log.Info("resubmitter server started")

	// Wait for interrupt signal to gracefully shut down the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	initiateGracefulShutdown(srv, &wg)
}

func startServer(srv *http.Server, config ServerConfig) {
	func() {
		if !config.TLS {
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatal(err.Error(), errcodes.Server)
			}
		} else {
			certsPath := os.Getenv(CertsEnv)
			if err := srv.ListenAndServeTLS(certsPath+"/server.crt", certsPath+"/server.key"); err != nil && err != http.ErrServerClosed {
				log.Fatal(err.Error(), errcodes.Server)
			}
		}
	}()
}

func initiateGracefulShutdown(srv *http.Server, wg *sync.WaitGroup) {
	log.Info("graceful shut down initiated, waiting for all goroutines to finish")

	// Waiting for all jobs to be done
	wg.Wait()

	log.Info("all goroutines finished, ready for shutdown")

	// New context which gives the server 5 seconds to shut down
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	if err := srv.Shutdown(ctx); err != nil {
		cancel()
		log.Fatal("error during server shutdown: "+err.Error(), errcodes.Server)
	}

	log.Info("server exiting")
	cancel()
}

func registerResubmitterEndpoints(handler *Handler, router *gin.Engine, wg *sync.WaitGroup) {
	router.POST("/resubmit/:mongo_collection", func(c *gin.Context) {
		wg.Add(1)
		handler.ResubmitIds(c)
		wg.Done()
	})

	router.POST("/range/:mongo_collection", func(c *gin.Context) {
		wg.Add(1)
		handler.ResubmitInterval(c)
		wg.Done()
	})

	router.POST("/query/:mongo_collection", func(c *gin.Context) {
		wg.Add(1)
		handler.ResubmitQueried(c)
		wg.Done()
	})
}
