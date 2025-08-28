package main

// handlers module
//
// Copyright (c) 2025 - Valentin Kuznetsov <vkuznet@gmail.com>
//
import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// MainHandler provides access to / end-point and either
// return all request records (GET method) or
func MainHandler(c *gin.Context) {
	rec := make(map[string]any)
	if c.Request.Method != "GET" {
		c.JSON(http.StatusBadRequest, nil)
	}
	c.JSON(http.StatusOK, rec)
}

// RequestHandler provides access to /request end-point
// return all specific request record (GET method) from /request/<request>
// accept new request record (POST method)
// delete given request (DELETE method)
func RequestHandler(c *gin.Context) {
	rec := make(map[string]any)
	if c.Request.Method == "GET" {
		requestID := c.Param("request")
		rec["requestID"] = requestID
	} else if c.Request.Method == "DELETE" {
		rec["method"] = "delete"
	} else if c.Request.Method == "POST" {
		var payload RequestData
		if err := c.ShouldBindJSON(&payload); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, payload)
		return
	} else {
		c.JSON(http.StatusBadRequest, nil)
	}
	c.JSON(http.StatusOK, rec)
}
