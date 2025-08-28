package main

// handlers module
//
// Copyright (c) 2025 - Valentin Kuznetsov <vkuznet@gmail.com>
//
import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

// MainHandler provides access to / end-point and either
// return all request records (GET method) or
// accept new request record (POST method)
func MainHandler(c *gin.Context) {
	rec := make(map[string]any)
	if c.Request.Method == "GET" {
		requestID := c.Param("request")
		log.Println("### request id %s", requestID)
	} else if c.Request.Method == "POST" {
		var payload RequestData
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	} else {
		c.JSON(http.StatusBadRequest, nil)
	}
	c.JSON(http.StatusOK, rec)
}

// RequestHandler provides access to GET /request/123 end-point
func RequestHandler(c *gin.Context) {
	rec := make(map[string]any)
	if c.Request.Method == "GET" {
	} else if c.Request.Method == "DELETE" {
	} else {
		c.JSON(http.StatusBadRequest, nil)
	}
	c.JSON(http.StatusOK, rec)
}
