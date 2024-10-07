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
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/dataphos/persistor-indexer-api/lib/repo"
)

type Handler struct {
	indexer *Indexer
}

// NewHandler constructs a new Handler.
func NewHandler(indexer *Indexer) *Handler {
	return &Handler{indexer: indexer}
}

// GetUnique gets the metadata with the unique id supplied in the URL
func (handler *Handler) GetUnique(c *gin.Context) {
	id := c.Param("id")
	mongoCollection := c.Param("mongo_collection")

	metadata, err := handler.indexer.Get(mongoCollection, id, uniqueRequestAttributes)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": OnBadRequestMessage,
			"error":   err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, &metadata)
}

type GetAllRequestBody struct {
	Ids []string `json:"ids" binding:"required"`
}

// GetAll gets the metadata with the unique ids supplied in the request body
func (handler *Handler) GetAll(c *gin.Context) {
	var body GetAllRequestBody
	mongoCollection := c.Param("mongo_collection")

	err := c.BindJSON(&body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": OnBadRequestMessage,
			"error":   err.Error(),
		})
		return
	}

	metadata, err := handler.indexer.GetAll(mongoCollection, body.Ids, intervalRequest)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": OnFailureMessage,
			"error":   err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, &metadata)
}

// GetAllInInterval gets the message with the partial id supplied in the URL.
// Because this operation can produce a multitude of messages, pagination is also implemented through URL params.
func (handler *Handler) GetAllInInterval(c *gin.Context) {
	brokerId := c.Param("id")
	mongoCollection := c.Param("mongo_collection")

	intervalParams, err := extractIntervalQueryParams(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": OnBadRequestMessage,
			"error":   err.Error(),
		})
		return
	}
	paginationParams := extractPaginationQueryParams(c)

	metadata, err := handler.indexer.GetAllInInterval(mongoCollection, brokerId, intervalParams.to, intervalParams.from, paginationParams.limit, paginationParams.offset, intervalRequest)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"message": OnFailureMessage,
			"error":   err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, &metadata)
}

type GetQueriedRequestBody struct {
	Filters []map[string]interface{} `json:"filters" binding:"required"`
}

// GetQueried gets all messages which contain metadata values given as query parameters.
// Because this operation can produce a multitude of messages, pagination is also implemented through URL params.
func (handler *Handler) GetQueried(c *gin.Context) {
	mongoCollection := c.Param("mongo_collection")

	var body GetQueriedRequestBody
	err := c.BindJSON(&body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": OnBadRequestMessage,
			"error":   err.Error(),
		})
		return
	}

	err = checkForInvalidQueryKeys(body.Filters)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": OnBadRequestMessage,
			"error":   err.Error(),
		})
		return
	}

	err = convertTimestamps(body.Filters)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": OnBadRequestMessage,
			"error":   err.Error(),
		})
		return
	}

	paginationParams := extractPaginationQueryParams(c)
	queryInfo := repo.QueryInformation{
		MongoCollection: mongoCollection,
		Filters:         body.Filters,
		Limit:           paginationParams.limit,
		Offset:          paginationParams.offset,
		AttributesList:  queryRequest,
	}

	metadata, err := handler.indexer.GetQueried(queryInfo)

	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"message": OnBadRequestMessage,
			"error":   err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, &metadata)
}
