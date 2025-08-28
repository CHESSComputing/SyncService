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
	SourceURI  string `json:"source_uri"`
	TargetURI  string `json:"target_uri"`
	Status     string `json:"status"`
	StatusCode int    `json:"status_code"`
}
