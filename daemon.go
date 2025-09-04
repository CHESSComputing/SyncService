package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	srvConfig "github.com/CHESSComputing/golib/config"
	"github.com/CHESSComputing/golib/services"
)

// syncDaemon function provide async functionality of
// FOXDEN sync process
func syncDaemon() {
	dbname := srvConfig.Config.Sync.MongoDB.DBName
	dbcoll := srvConfig.Config.Sync.MongoDB.DBColl
	idx := 0
	limit := 0
	spec := make(map[string]any)
	for {
		records := metaDB.Get(dbname, dbcoll, spec, idx, limit)
		var wg sync.WaitGroup
		for _, rec := range records {
			wg.Add(1)
			log.Printf("processing %+v", rec)
			syncWorker(rec)
		}
		wg.Wait()
	}
}

// syncWorker function provide async functionality of
// FOXDEN sync process which is based on the following logic
// - obtain all FOXDEN metadata records from source FOXDEN instance
// - send PUT request to target FOXDEN isntance with all metadata records
// - update sync record with SyncMetadata status code
// - obtain all FOXDEN provenance records from source FOXDEN instance
// - send PUT request to target FOXDEN instance with all provenance records
// - update sync record with SyncProvenance status code
// - perform verification step
// - update sync status code with Completed or Aborted
func syncWorker(syncRecord map[string]any) error {
	dbname := srvConfig.Config.Sync.MongoDB.DBName
	dbcoll := srvConfig.Config.Sync.MongoDB.DBColl
	var suuid string
	if val, ok := syncRecord["uuid"]; ok {
		suuid = fmt.Sprintf("%s", val)
	} else {
		return errors.New(fmt.Sprintf("Unable to obtain UUID of sync record %+v", syncRecord))
	}
	if err := updateMetadataRecords(syncRecord); err != nil {
		return err
	}

	// prepare sync query record
	spec := make(map[string]any)
	spec["uuid"] = suuid

	// update sync record
	spec["status_code"] = SyncMetadata
	spec["updated_at"] = time.Now().Format(time.RFC3339)
	if err := metaDB.Update(dbname, dbcoll, spec, syncRecord); err != nil {
		return err
	}

	// update provenance records
	if err := updateProvenanceRecords(syncRecord); err != nil {
		return err
	}
	// update sync record
	spec["status_code"] = SyncProvenance
	spec["updated_at"] = time.Now().Format(time.RFC3339)
	if err := metaDB.Update(dbname, dbcoll, spec, syncRecord); err != nil {
		return err
	}

	return nil
}

func updateProvenanceRecords(syncRecord map[string]any) error {
	// obtain all FOXDEN provenance records from source FOXDEN instance
	// send PUT request to target FOXDEN instance with all provenance records
	if surl, ok := syncRecord["source_url"]; ok {
		spec := make(map[string]any)
		rurl := fmt.Sprintf("%s/provenance", surl)
		rec := services.ServiceRequest{
			Client:       "foxden-sync",
			ServiceQuery: services.ServiceQuery{Spec: spec},
		}
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		resp, err := _httpReadRequest.Post(rurl, "application/json", bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		data, err = io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		var records []map[string]any
		err = json.Unmarshal(data, &records)
		if err != nil {
			return err
		}
	}
	return nil
}

func updateMetadataRecords(syncRecord map[string]any) error {
	// obtain all FOXDEN metadata records from source FOXDEN instance
	if surl, ok := syncRecord["source_url"]; ok {
		spec := make(map[string]any)
		rurl := fmt.Sprintf("%s/search", surl)
		rec := services.ServiceRequest{
			Client:       "foxden-sync",
			ServiceQuery: services.ServiceQuery{Spec: spec},
		}
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		resp, err := _httpReadRequest.Post(rurl, "application/json", bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		data, err = io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		var records []map[string]any
		err = json.Unmarshal(data, &records)
		if err != nil {
			return err
		}
	}
	// send PUT request to target FOXDEN isntance with all metadata records
	// update sync record with SyncMetadata status code
	return nil
}
