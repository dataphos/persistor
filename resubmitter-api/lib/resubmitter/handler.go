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
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/dataphos/persistor-resubmitter-api/common/log"
	"github.com/dataphos/persistor-resubmitter-api/common/util"
	"github.com/dataphos/persistor-resubmitter-api/lib/indexer"
)

type Handler struct {
	resubmitterJob *resubmitterJob
	envTopicID     string
}

const (
	OnSuccessMessage        = "resubmission successful"
	OnPartialContentMessage = "partial resubmission"
	OnBadRequestMessage     = "resubmission failed due to bad request"
	OnFailureMessage        = "resubmission failed"
	OnNoTopicMessage        = "topic was not defined as a query parameter nor as an environment variable"

	TopicIDEnv = "TOPIC_ID"
)

func NewHandler(resubmitter *Resubmitter) *Handler {
	topicID := os.Getenv(TopicIDEnv)
	if topicID == "" {
		log.Warn(fmt.Sprintf("environment variable %s is not defined", TopicIDEnv))
	}

	return &Handler{
		resubmitterJob: &resubmitterJob{resubmitter: *resubmitter},
		envTopicID:     topicID,
	}
}

type request struct {
	Ids []string `json:"ids" binding:"required"`
}

type response struct {
	Status  int             `json:"status"`
	Msg     string          `json:"msg"`
	Summary *resubmitterJob `json:"summary"`
	Errors  *ResubmitResult `json:"errors,omitempty"`
}

func (handler *Handler) ResubmitIds(context *gin.Context) {
	topicID := handler.getTopicIDFromQueryOrEnv(context)
	if topicID == "" {
		context.JSON(http.StatusBadRequest, gin.H{
			"msg": OnNoTopicMessage,
		})

		return
	}

	mongoCollection := context.Param("mongo_collection")

	var body request

	err := context.BindJSON(&body)
	if err != nil {
		context.JSON(http.StatusBadRequest, gin.H{
			"msg":   OnBadRequestMessage,
			"error": err.Error(),
		})
		return
	}

	handler.resubmitterJob.resetCounters()

	results := handler.resubmitterJob.Resubmit(topicID, mongoCollection, body.Ids)

	statusCode := chooseHTTPStatusCode(handler.resubmitterJob, results)
	context.JSON(statusCode, &response{
		Status:  statusCode,
		Msg:     chooseResponseMessage(statusCode),
		Summary: handler.resubmitterJob,
		Errors:  &results,
	})
}

func (handler *Handler) getTopicIDFromQueryOrEnv(context *gin.Context) string {
	topicID := context.Query(topicParam)
	if topicID == "" {
		topicID = handler.envTopicID
	}

	return topicID
}

func chooseHTTPStatusCode(job *resubmitterJob, results ResubmitResult) int {
	switch {
	case len(results.IndexerErrors) == 0 && len(results.PipelineErrors) == 0:
		return http.StatusOK
	case job.PublishedCounter == 0:
		return http.StatusBadRequest
	default:
		return http.StatusPartialContent
	}
}

func chooseResponseMessage(statusCode int) string {
	switch statusCode {
	case http.StatusOK:
		return OnSuccessMessage
	case http.StatusPartialContent:
		return OnPartialContentMessage
	case http.StatusBadRequest:
		return OnBadRequestMessage
	case http.StatusInternalServerError:
		return OnFailureMessage
	}

	return "no response message for this status code"
}

type intervalRequest struct {
	BrokerID   string     `json:"broker_id" binding:"required"`
	LowerBound *time.Time `json:"lb,omitempty"`
	UpperBound *time.Time `json:"ub,omitempty"`
}

func (handler *Handler) ResubmitInterval(context *gin.Context) {
	topicID := handler.getTopicIDFromQueryOrEnv(context)
	if topicID == "" {
		context.JSON(http.StatusBadRequest, gin.H{
			"msg": OnNoTopicMessage,
		})

		return
	}

	mongoCollection := context.Param("mongo_collection")

	var body intervalRequest

	err := context.BindJSON(&body)
	if err != nil {
		err = fmt.Errorf("error occurred during binding JSON to request body: %w", err)
		context.JSON(http.StatusBadRequest, gin.H{
			"msg":   OnBadRequestMessage,
			"error": err.Error(),
		})

		return
	}

	checkIfParamsMissing(&body)

	handler.resubmitterJob.resetCounters()

	results := handler.resubmitterJob.ResubmitInterval(topicID, mongoCollection, body.BrokerID, *body.LowerBound, *body.UpperBound)

	statusCode := chooseHTTPStatusCode(handler.resubmitterJob, results)
	context.JSON(statusCode, &response{
		Status:  statusCode,
		Msg:     chooseResponseMessage(statusCode),
		Summary: handler.resubmitterJob,
		Errors:  &results,
	})
}

func checkIfParamsMissing(body *intervalRequest) {
	if body.LowerBound == nil {
		lowerBound := time.Time{}
		body.LowerBound = &lowerBound
		log.Info(log.UsingDefaultParameterValue(indexer.From, lowerBound.Format(indexer.DateFormat)))
	}

	if body.UpperBound == nil {
		upperBound := time.Now()
		body.UpperBound = &upperBound
		log.Info(log.UsingDefaultParameterValue(indexer.To, upperBound.Format(indexer.DateFormat)))
	}
}

func (handler *Handler) ResubmitQueried(context *gin.Context) {
	topicID := handler.getTopicIDFromQueryOrEnv(context)
	if topicID == "" {
		context.JSON(http.StatusBadRequest, gin.H{
			"msg": OnNoTopicMessage,
		})

		return
	}

	mongoCollection := context.Param("mongo_collection")

	var body util.QueryRequestBody

	err := context.BindJSON(&body)
	if err != nil {
		err = fmt.Errorf("error occurred during binding JSON to request body: %w", err)

		context.JSON(http.StatusBadRequest, gin.H{
			"msg":   OnBadRequestMessage,
			"error": err.Error(),
		})

		return
	}

	handler.resubmitterJob.resetCounters()

	results := handler.resubmitterJob.ResubmitQuery(topicID, mongoCollection, body)

	statusCode := chooseHTTPStatusCode(handler.resubmitterJob, results)

	context.JSON(statusCode, &response{
		Status:  statusCode,
		Msg:     chooseResponseMessage(statusCode),
		Summary: handler.resubmitterJob,
		Errors:  &results,
	})
}
