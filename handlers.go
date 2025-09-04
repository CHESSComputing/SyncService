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
	switch c.Request.Method {
	case "GET":
		getHandler(c)
	case "POST":
		postHandler(c)
	case "PUT":
		putHandler(c)
	case "DELETE":
		deleteHandler(c)
	default:
		err := errors.New("unsupported HTTP method")
		resp := services.Response("SyncService", http.StatusBadRequest, UnsupportedMethod, err)
		c.JSON(http.StatusBadRequest, resp)
		return
	}
}

// getHandler handles GET HTTP requests
func getHandler(c *gin.Context) {
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

// postHandler handles POST HTTP requests
func postHandler(c *gin.Context) {
	syncHandler(c, "post")
}

// putHandler handles PUT HTTP requests
func putHandler(c *gin.Context) {
	syncHandler(c, "put")
}

// syncHandler handles POST/PUT HTTP requests
func syncHandler(c *gin.Context, method string) {
	var payload RequestData
	if err := c.ShouldBindJSON(&payload); err != nil {
		resp := services.Response("SyncService", http.StatusBadRequest, IncompleteRequest, err)
		c.JSON(http.StatusBadRequest, resp)
		return
	}
	// check if payload data contains everything we need to accept request
	if payload.SourceURL == "" || payload.TargetURL == "" ||
		payload.SourceToken == "" || payload.TargetToken == "" {
		err := errors.New("sync request does not provide all required attributes")
		resp := services.Response("SyncService", http.StatusBadRequest, IncompleteRequest, err)
		c.JSON(http.StatusBadRequest, resp)
		return
	}

	// insert request record into underlying sync database
	requestRecord := payload.RequestRecord()
	dbName := srvConfig.Config.Sync.MongoDB.DBName
	dbColl := srvConfig.Config.Sync.MongoDB.DBColl
	var err error
	switch method {
	case "POST":
		err = metaDB.InsertRecord(dbName, dbColl, requestRecord)
	case "PUT":
		spec := make(map[string]any)
		if uuid := payload.UUID; uuid != "" {
			spec["UUID"] = uuid
			err = metaDB.Update(dbName, dbColl, spec, requestRecord)
		} else {
			err = errors.New(fmt.Sprintf("update request %+v does not provide UUID", payload))
		}
	default:
		err = errors.New(fmt.Sprintf("unsupported method %s", method))
	}
	if err != nil {
		resp := services.Response("SyncService", http.StatusBadRequest, InsertError, err)
		c.JSON(http.StatusBadRequest, resp)
		return
	}
	resp := services.Response("SyncService", http.StatusOK, Accepted, nil)
	c.JSON(http.StatusOK, resp)
}

// deleteHandler handles DELETE HTTP requests
func deleteHandler(c *gin.Context) {
	resp := services.ServiceResponse{}
	spec := make(map[string]any)
	requestID := c.Param("request")
	spec["UUID"] = requestID
	resp.ServiceQuery.Query = fmt.Sprintf("/request/%s", requestID)
	// TODO: implement logic to delete request from internal DB
	resp = services.Response("SyncService", http.StatusOK, Deleted, nil)
	c.JSON(http.StatusOK, resp)
}
