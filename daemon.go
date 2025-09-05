package main

import (
	"bytes"
	"encoding/json"
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
func syncDaemon(sleep int) {
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
		// if sleep interval is negative we quite the daemon cycle
		if sleep < 0 {
			break
		}
		// otherwise we'll sleep for provided duration
		time.Sleep(time.Duration(sleep))
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
		return fmt.Errorf("Unable to obtain UUID of sync record %+v", syncRecord)
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

// helper function to update provenance records
func updateProvenanceRecords(syncRecord map[string]any) error {
	return updateRecords("provenance", syncRecord)
}

// helper function to update metadata records
func updateMetadataRecords(syncRecord map[string]any) error {
	return updateRecords("metadata", syncRecord)
}

// helper function to update records in FOXDEN
func updateRecords(srv string, syncRecord map[string]any) error {
	var records []map[string]any
	var err error
	// obtain all FOXDEN records from source FOXDEN instance
	if val, ok := syncRecord["source_url"]; ok {
		surl := fmt.Sprintf("%s", val)
		rurl := fmt.Sprintf("%s/dids", surl)
		didRecords, err := getRecords(rurl)
		if err != nil {
			return err
		}
		var dids, urls []string
		for _, rec := range didRecords {
			did := fmt.Sprintf("%s", rec["did"])
			dids = append(dids, did)
			rurl = fmt.Sprintf("%s/record?did=%s", did)
			if srv == "provenance" {
				rurl = fmt.Sprintf("%s/provenance?did=%s", did)
			}
			urls = append(urls, rurl)
		}
		// now we'll get either metadata or provenance records
		records = getFoxdenRecords(urls)
	}
	// send PUT request to target FOXDEN isntance with all metadata records
	if val, ok := syncRecord["target_url"]; ok {
		turl := fmt.Sprintf("%s", val)
		err = pushRecords(srv, turl, records)
		if err != nil {
			return err
		}
	}
	return nil
}

// helper function to get records for given set of urls
func getFoxdenRecords(urls []string) []map[string]any {
	var records []map[string]any
	return records
}

// helper function to push FOXDEN records to upstream target url
func pushRecords(srv, turl string, records []map[string]any) error {
	log.Println(srv, turl, records)
	return nil
}

// helper function to get records from given FOXDEN instance
func getRecords(rurl string) ([]map[string]any, error) {
	var records []map[string]any
	spec := make(map[string]any)
	rec := services.ServiceRequest{
		Client:       "foxden-sync",
		ServiceQuery: services.ServiceQuery{Spec: spec},
	}
	data, err := json.Marshal(rec)
	if err != nil {
		return records, err
	}
	resp, err := _httpReadRequest.Post(rurl, "application/json", bytes.NewBuffer(data))
	if err != nil {
		return records, err
	}
	defer resp.Body.Close()
	data, err = io.ReadAll(resp.Body)
	if err != nil {
		return records, err
	}
	err = json.Unmarshal(data, &records)
	if err != nil {
		return records, err
	}
	return records, nil
}
