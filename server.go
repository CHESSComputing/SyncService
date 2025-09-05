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
var _httpReadRequest *services.HttpRequest
var metaDB docdb.DocDB

// helper function to setup our router
func setupRouter() *gin.Engine {
	routes := []server.Route{
		{Method: "GET", Path: "/request/*request", Handler: RequestHandler, Authorized: false},
		{Method: "PUT", Path: "/request/*request", Handler: RequestHandler, Authorized: false},
		{Method: "POST", Path: "/request", Handler: RequestHandler, Authorized: false},
		{Method: "DELETE", Path: "/request/*request", Handler: RequestHandler, Authorized: false},
	}
	r := server.Router(routes, nil, "static", srvConfig.Config.Sync.WebServer)
	return r
}

// Server defines our HTTP server
func Server() {
	var err error
	// initialize http request
	_httpReadRequest = services.NewHttpRequest("read", 0)

	// init docdb
	metaDB, err = docdb.InitializeDocDB(srvConfig.Config.Sync.MongoDB.DBUri)
	if err != nil {
		log.Fatal(err)
	}

	go syncDaemon()

	// setup web router and start the service
	r := setupRouter()
	webServer := srvConfig.Config.Sync.WebServer
	server.StartServer(r, webServer)
}
