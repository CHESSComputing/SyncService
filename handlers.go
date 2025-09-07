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

// getHandler handles GET HTTP requests
func getHandler(c *gin.Context) {
	spec := make(map[string]any)
	spec["uuid"] = c.Param("uuid")
	records := metaDB.Get(
		srvConfig.Config.Sync.MongoDB.DBName,
		srvConfig.Config.Sync.MongoDB.DBColl,
		spec, 0, 1)
	c.JSON(http.StatusOK, records)
}

// recordsHandler handles requests to get set of records for provided meta parametes
func recordsHandler(c *gin.Context) {
	spec := make(map[string]any)
	records := metaDB.Get(
		srvConfig.Config.Sync.DBName,
		srvConfig.Config.Sync.DBColl,
		spec, 0, -1)
	c.JSON(http.StatusOK, records)
}

// postHandler handles POST HTTP requests
func postHandler(c *gin.Context) {
	syncHandler(c, "POST")
}

// putHandler handles PUT HTTP requests
func putHandler(c *gin.Context) {
	syncHandler(c, "PUT")
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
			spec["uuid"] = uuid
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
	spec := make(map[string]any)
	spec["uuid"] = c.Param("uuid")
	err := metaDB.Remove(
		srvConfig.Config.Sync.DBName,
		srvConfig.Config.Sync.DBColl,
		spec)
	if err != nil {
		c.JSON(http.StatusBadRequest, err)
		return
	}
	c.JSON(http.StatusOK, nil)
}
