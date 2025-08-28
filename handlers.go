package main

// handlers module
//
// Copyright (c) 2025 - Valentin Kuznetsov <vkuznet@gmail.com>
//
import (
	"errors"
	"fmt"
	"net/http"

	srvConfig "github.com/CHESSComputing/golib/config"
	"github.com/CHESSComputing/golib/services"
	"github.com/gin-gonic/gin"
)

// RequestHandler provides access to /request end-point
// return all specific request record (GET method) from /request/<request>
// accept new request record (POST method)
// delete given request (DELETE method)
func RequestHandler(c *gin.Context) {
	resp := services.ServiceResponse{}
	spec := make(map[string]any)
	if c.Request.Method == "GET" {
		requestID := c.Param("request")
		resp.ServiceQuery.Query = fmt.Sprintf("/request/%s", requestID)
		spec["UUID"] = requestID
		records := metaDB.Get(
			srvConfig.Config.Sync.MongoDB.DBName,
			srvConfig.Config.Sync.MongoDB.DBColl,
			spec, 0, 1)
		// TODO: implement logic to fetch request from internal DB
		resp.ServiceQuery.Query = fmt.Sprintf("/request/%s", requestID)
		resp.Results.Records = records
	} else if c.Request.Method == "DELETE" {
		requestID := c.Param("request")
		resp.ServiceQuery.Query = fmt.Sprintf("/request/%s", requestID)
		// TODO: implement logic to delete request from internal DB
	} else if c.Request.Method == "PUT" {
		requestID := c.Param("request")
		resp.ServiceQuery.Query = fmt.Sprintf("/request/%s", requestID)
		// TODO: implement logic to update request in internal DB
	} else if c.Request.Method == "POST" {
		var payload RequestData
		if err := c.ShouldBindJSON(&payload); err != nil {
			resp = services.Response("SyncService", http.StatusBadRequest, IncompleteRequest, err)
			c.JSON(http.StatusBadRequest, resp)
			return
		}
		// check if payload data contains everything we need to accept request
		if payload.SourceURL == "" || payload.TargetURL == "" ||
			payload.SourceToken == "" || payload.TargetToken == "" {
			err := errors.New("sync request does not provide all required attributes")
			resp = services.Response("SyncService", http.StatusBadRequest, IncompleteRequest, err)
			c.JSON(http.StatusBadRequest, resp)
			return
		}

		// insert request record into underlying sync database
		requestRecord := payload.RequestRecord()
		err := metaDB.InsertRecord(srvConfig.Config.Sync.MongoDB.DBName, "archive", requestRecord)
		if err != nil {
			resp = services.Response("SyncService", http.StatusBadRequest, InsertError, err)
			c.JSON(http.StatusBadRequest, resp)
			return
		}
		c.JSON(http.StatusOK, requestRecord)
		return
	} else {
		err := errors.New("unsupported HTTP method")
		resp = services.Response("SyncService", http.StatusBadRequest, UnsupportedMethod, err)
		c.JSON(http.StatusBadRequest, resp)
		return
	}
	resp = services.Response("SyncService", http.StatusOK, Accepted, nil)
	c.JSON(http.StatusOK, resp)
}
