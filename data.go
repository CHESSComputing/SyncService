package main

const (
	DoesNotExist = iota
	Accepted
	InProgress
	SyncMetadata
	SyncProvenance
	Completed
)

type RequestData struct {
	UID        string `json:"uuid"`
	SourceURL  string `json:"source_url"`
	TargetURL  string `json:"target_url"`
	Status     string `json:"status"`
	StatusCode int    `json:"status_code"`
}
