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
	if c.Request.Method == "GET" {
		GetHandler(c)
	} else if c.Request.Method == "DELETE" {
		DeleteHandler(c)
	} else if c.Request.Method == "PUT" {
		PutHandler(c)
	} else if c.Request.Method == "POST" {
		PostHandler(c)
	} else {
		err := errors.New("unsupported HTTP method")
		resp := services.Response("SyncService", http.StatusBadRequest, UnsupportedMethod, err)
		c.JSON(http.StatusBadRequest, resp)
		return
	}
}

func GetHandler(c *gin.Context) {
	resp := services.ServiceResponse{}
	spec := make(map[string]any)
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
	resp = services.Response("SyncService", http.StatusOK, Accepted, nil)
	c.JSON(http.StatusOK, resp)
}

func PostHandler(c *gin.Context) {
	resp := services.ServiceResponse{}
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
	resp = services.Response("SyncService", http.StatusOK, Accepted, nil)
	c.JSON(http.StatusOK, resp)
}

func PutHandler(c *gin.Context) {
	resp := services.ServiceResponse{}
	spec := make(map[string]any)
	requestID := c.Param("request")
	spec["UUID"] = requestID
	resp.ServiceQuery.Query = fmt.Sprintf("/request/%s", requestID)
	// TODO: implement logic to update request in internal DB
	resp = services.Response("SyncService", http.StatusOK, Updated, nil)
	c.JSON(http.StatusOK, resp)
}

func DeleteHandler(c *gin.Context) {
	resp := services.ServiceResponse{}
	spec := make(map[string]any)
	requestID := c.Param("request")
	spec["UUID"] = requestID
	resp.ServiceQuery.Query = fmt.Sprintf("/request/%s", requestID)
	// TODO: implement logic to delete request from internal DB
	resp = services.Response("SyncService", http.StatusOK, Deleted, nil)
	c.JSON(http.StatusOK, resp)
}
