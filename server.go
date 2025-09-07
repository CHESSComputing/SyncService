package main

// server module
//
// Copyright (c) 2025 - Valentin Kuznetsov <vkuznet@gmail.com>
//
import (
	"log"

	srvConfig "github.com/CHESSComputing/golib/config"
	docdb "github.com/CHESSComputing/golib/docdb"
	server "github.com/CHESSComputing/golib/server"
	"github.com/CHESSComputing/golib/services"
	"github.com/gin-gonic/gin"
)

// global variables
var _header, _footer string
var _httpReadRequest, _httpWriteRequest *services.HttpRequest
var metaDB docdb.DocDB
var Verbose int

// helper function to setup our router
func setupRouter() *gin.Engine {
	routes := []server.Route{
		{Method: "GET", Path: "/records", Handler: recordsHandler, Authorized: true, Scope: "read"},
		{Method: "GET", Path: "/record/:uuid", Handler: getHandler, Authorized: true, Scope: "read"},
		{Method: "PUT", Path: "/record/:uuid", Handler: putHandler, Authorized: true, Scope: "write"},
		{Method: "POST", Path: "/record", Handler: postHandler, Authorized: true, Scope: "write"},
		{Method: "DELETE", Path: "/record/:uuid", Handler: deleteHandler, Authorized: true, Scope: "delete"},
	}
	r := server.Router(routes, nil, "static", srvConfig.Config.Sync.WebServer)
	return r
}

// Server defines our HTTP server
func Server() {
	var err error
	// initialize http request
	_httpReadRequest = services.NewHttpRequest("read", 0)
	_httpWriteRequest = services.NewHttpRequest("write", 0)

	// init docdb
	metaDB, err = docdb.InitializeDocDB(srvConfig.Config.Sync.MongoDB.DBUri)
	if err != nil {
		log.Fatal(err)
	}

	go syncDaemon(srvConfig.Config.Sync.SleepInterval)

	// setup web router and start the service
	r := setupRouter()
	webServer := srvConfig.Config.Sync.WebServer
	Verbose = srvConfig.Config.Sync.WebServer.Verbose
	server.StartServer(r, webServer)
}
