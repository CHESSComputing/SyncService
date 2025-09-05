package main

import (
	"time"

	"github.com/google/uuid"
)

const (
	DoesNotExist      = iota + 100 // 100 does not exit
	IncompleteRequest              // 101 incomplete request
	UnsupportedMethod              // 102 unsupported method
	InsertError                    // 103 insert error
	Accepted                       // 104 request accepted
	Updated                        // 105 requested updated
	Deleted                        // 106 request deleted
	InProgress                     // 107 request in progress
	SyncMetadata                   // 108 synching metadata
	SyncProvenance                 // 109 synching provenance records
	Completed                      // 110 request completed
)

// RequestData represents SyncService request data payload
type RequestData struct {
	UUID        string `json:"uuid"`
	SourceURL   string `json:"source_url"`
	SourceToken string `json:"source_token"`
	TargetURL   string `json:"target_url"`
	TargetToken string `json:"target_token"`
	Status      string `json:"status"`
	StatusCode  int    `json:"status_code"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

// RequestRecord creates new request record for database out of payload
func (p *RequestData) RequestRecord() map[string]any {
	rec := make(map[string]any)
	rec["source_url"] = p.SourceURL
	rec["target_url"] = p.TargetURL
	rec["source_token"] = p.SourceToken
	rec["target_token"] = p.TargetToken
	if p.UUID != "" {
		rec["uuid"] = p.UUID
	} else {
		uuid, _ := uuid.NewRandom()
		rec["uuid"] = uuid.String()
	}
	rec["status"] = "sync request is accepted"
	rec["status_code"] = Accepted
	rec["created_at"] = time.Now().Format(time.RFC3339)
	return rec
}
