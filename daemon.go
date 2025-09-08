package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	srvConfig "github.com/CHESSComputing/golib/config"
	"github.com/CHESSComputing/golib/services"
	"github.com/CHESSComputing/golib/utils"
)

// syncDaemon function provide async functionality of
// FOXDEN sync process
func syncDaemon(sleep int) {
	dbname := srvConfig.Config.Sync.MongoDB.DBName
	dbcoll := srvConfig.Config.Sync.MongoDB.DBColl
	idx := 0
	limit := 0
	spec := make(map[string]any)
	// initially we sleep for couple of seconds to allow other services to wake up
	time.Sleep(time.Duration(2 * int(time.Second)))
	for {
		records := metaDB.Get(dbname, dbcoll, spec, idx, limit)
		var wg sync.WaitGroup
		for _, rec := range records {
			wg.Add(1)
			if Verbose > 1 {
				log.Printf("processing %+v", rec)
			}
			syncWorker(rec)
		}
		wg.Wait()
		// if sleep interval is negative we quite the daemon cycle
		if sleep < 0 {
			break
		}
		// otherwise we'll sleep for provided duration
		time.Sleep(time.Duration(sleep * int(time.Second)))
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

	// extract sync record uuid from sync record
	var suuid string
	if val, ok := syncRecord["uuid"]; ok {
		suuid = fmt.Sprintf("%s", val)
	} else {
		return fmt.Errorf("Unable to obtain UUID of sync record %+v", syncRecord)
	}

	// update metadata records
	if err := updateSyncRecordStatus(suuid, "in progress", InProgress); err != nil {
		return err
	}
	if err := updateMetadataRecords(syncRecord); err != nil {
		return err
	}
	if err := updateSyncRecordStatus(suuid, "metadata records are synched", SyncMetadata); err != nil {
		return err
	}

	// update provenance records
	if err := updateSyncRecordStatus(suuid, "in progress", InProgress); err != nil {
		return err
	}
	if err := updateProvenanceRecords(syncRecord); err != nil {
		return err
	}
	if err := updateSyncRecordStatus(suuid, "provenance records are synched", SyncProvenance); err != nil {
		return err
	}

	// final update
	if err := updateSyncRecordStatus(suuid, "sync is completed", Completed); err != nil {
		return err
	}
	return nil
}

// helper function to update sync record status
func updateSyncRecordStatus(suuid, status string, statusCode int) error {
	dbname := srvConfig.Config.Sync.MongoDB.DBName
	dbcoll := srvConfig.Config.Sync.MongoDB.DBColl
	spec := make(map[string]any)
	spec["uuid"] = suuid
	rec := make(map[string]any)
	newRecord := make(map[string]any)
	// update sync record
	rec["status_code"] = statusCode
	rec["status"] = status
	rec["updated_at"] = time.Now().Format(time.RFC3339)
	newRecord["$set"] = rec
	if err := metaDB.Update(dbname, dbcoll, spec, newRecord); err != nil {
		return err
	}
	printSyncRecordStatus(suuid)
	return nil
}

// helper function to print record status
func printSyncRecordStatus(suuid string) {
	dbname := srvConfig.Config.Sync.MongoDB.DBName
	dbcoll := srvConfig.Config.Sync.MongoDB.DBColl
	spec := make(map[string]any)
	spec["uuid"] = suuid
	records := metaDB.Get(dbname, dbcoll, spec, 0, 1)
	for _, rec := range records {
		if status, ok := rec["status"]; ok {
			log.Printf("INFO: sync record %s status %s", suuid, status)
		}
	}
}

// helper function to update provenance records
func updateProvenanceRecords(syncRecord map[string]any) error {
	return updateRecords("provenance", syncRecord)
}

// helper function to update metadata records
func updateMetadataRecords(syncRecord map[string]any) error {
	return updateRecords("metadata", syncRecord)
}

// helper function to get dids for given FOXDEN url and access token
func getDIDs(rurl, token string) ([]string, error) {
	var dids []string
	var err error
	didRecords, err := getRecords(rurl, token)
	if err != nil {
		log.Printf("ERROR: unable to get records from url=%s token=%s error=%v", rurl, token, err)
		return dids, err
	}
	for _, rec := range didRecords {
		did := fmt.Sprintf("%s", rec["did"])
		dids = append(dids, did)
	}
	return dids, nil
}

// helper function to update records in FOXDEN
func updateRecords(srv string, syncRecord map[string]any) error {
	var records []map[string]any
	var err error
	var surl, sourceUrl, turl, targetUrl string
	if val, ok := syncRecord["source_url"]; ok {
		surl = fmt.Sprintf("%s", val)
		sourceUrl = fmt.Sprintf("%s/dids", surl)
	}
	if val, ok := syncRecord["target_url"]; ok {
		turl = fmt.Sprintf("%s", val)
		targetUrl = fmt.Sprintf("%s/dids", turl)
	}
	var sourceToken, targetToken string
	if val, ok := syncRecord["source_token"]; ok {
		sourceToken = fmt.Sprintf("%s", val)
	}
	if val, ok := syncRecord["target_token"]; ok {
		targetToken = fmt.Sprintf("%s", val)
	}

	// obtain all FOXDEN records from source FOXDEN instance
	sourceDIDs, err := getDIDs(sourceUrl, sourceToken)
	if err != nil {
		return err
	}
	targetDIDs, err := getDIDs(targetUrl, targetToken)
	if err != nil {
		return err
	}
	// construct unique list of dids to fetch from source FOXDEN instance
	var dids []string
	for _, did := range sourceDIDs {
		if !utils.InList[string](did, targetDIDs) {
			dids = append(dids, did)
		}
	}
	if Verbose > 0 {
		log.Printf("Source dids=%d, target dids=%d, sync dids=%d", len(sourceDIDs), len(targetDIDs), len(dids))
	}

	// now we'll get either metadata or provenance records
	if srvConfig.Config.Sync.NWorkers != 0 {
		nWorkers := srvConfig.Config.Sync.NWorkers
		records = getDidRecordsConcurrent(srv, surl, dids, sourceToken, nWorkers)
	} else {
		records = getDidRecords(srv, surl, dids, sourceToken)
	}
	if Verbose > 0 {
		log.Printf("Fetched %d records from source FOXDEN instance %s", len(records), surl)
	}

	// push records to target FOXDEN instance
	err = pushRecords(srv, turl, targetToken, records)
	return err
}

// helper function to get proper FOXDEN url for given service, url and did
func getFoxdenUrl(srv, surl, did string) string {
	rurl := fmt.Sprintf("%s/record?did=%s", surl, did)
	if srv == "provenance" {
		rurl = fmt.Sprintf("%s/provenance?did=%s", surl, did)
	}
	return rurl
}

// helper function to get records for given set of urls
func getDidRecords(srv, surl string, dids []string, token string) []map[string]any {
	var records []map[string]any
	// fetch records sequentially
	for _, did := range dids {
		rurl := getFoxdenUrl(srv, surl, did)
		recs, err := getRecords(rurl, token)
		if err == nil {
			records = append(records, recs...)
		} else {
			log.Println("ERROR: unable to get records for url", rurl, err)
		}
	}
	return records
}

// concurrent version of getDidRecords with worker pool
func getDidRecordsConcurrent(srv, surl string, dids []string, token string, maxWorkers int) []map[string]any {
	var (
		records []map[string]any
		mu      sync.Mutex
		wg      sync.WaitGroup
		sem     = make(chan struct{}, maxWorkers) // semaphore channel
	)

	for _, did := range dids {
		rurl := getFoxdenUrl(srv, surl, did)
		wg.Add(1)

		// acquire slot
		sem <- struct{}{}

		go func(u string) {
			defer wg.Done()
			defer func() { <-sem }() // release slot

			recs, err := getRecords(u, token)
			if err != nil {
				log.Println("ERROR: unable to get records for url", u, err)
				return
			}

			// safely append results
			mu.Lock()
			records = append(records, recs...)
			mu.Unlock()
		}(rurl)
	}

	// wait for all workers
	wg.Wait()

	return records
}

// helper function to get records from given FOXDEN instance
func getRecords(rurl, token string) ([]map[string]any, error) {
	log.Printf("INFO: get records for %s", rurl)
	var records []map[string]any
	_httpReadRequest.SetToken(token)
	resp, err := _httpReadRequest.Get(rurl)
	if err != nil {
		return records, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return records, err
	}
	err = json.Unmarshal(data, &records)
	if err != nil {
		// try out service response record
		var srec services.ServiceResponse
		if err = json.Unmarshal(data, &srec); err == nil {
			return srec.Results.Records, nil
		}
		return records, err
	}
	return records, nil
}

type MetaRecord struct {
	Schema string
	Record map[string]any
}

// helper function to get records from given FOXDEN instance
func pushRecord(rurl, token string, rec map[string]any) error {
	if did, ok := rec["did"]; ok {
		log.Printf("INFO: push record did=%s to %s", did, rurl)
	} else {
		log.Printf("INFO: push record to %s", rurl)
	}
	var data []byte
	var err error
	if strings.Contains(rurl, "provenance") {
		// we submit provenance record
		data, err = json.Marshal(rec)
	} else {
		// we submit MetaRecord
		mrec := MetaRecord{Record: rec}
		if val, ok := rec["schema"]; ok {
			mrec.Schema = fmt.Sprintf("%s", val)
		} else {
			msg := fmt.Sprintf("non complaint FOXDEN metadata record %+v", rec)
			return errors.New(msg)
		}
		data, err = json.Marshal(mrec)
	}
	if Verbose > 1 {
		log.Println("pushRecord", rurl, token, rec)
	}
	if err != nil {
		return err
	}
	_httpWriteRequest.SetToken(token)
	resp, err := _httpWriteRequest.Post(rurl, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("ERROR: unable to push record to target FOXDEN URL %s, error %v", rurl, err)
		return err
	}
	defer resp.Body.Close()
	data, err = io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		log.Printf("ERROR: reponse from target FOXDEN %+v", resp.Status)
		return errors.New(resp.Status)
	}
	return nil
}

// helper function to push FOXDEN records to upstream target url
func pushRecords(srv, turl, token string, records []map[string]any) error {
	rurl := fmt.Sprintf("%s/record", turl)
	if srv == "provenance" {
		rurl = fmt.Sprintf("%s/record", turl)
	}
	if Verbose > 0 {
		log.Printf("Push %d records to target FOXDEN instance %s", len(records), rurl)
	}
	// TODO: optimize push request via concurrency pool
	for _, rec := range records {
		err := pushRecord(rurl, token, rec)
		if err != nil {
			log.Printf("ERROR: unable to push record to target URL %s, error=%v", turl, err)
			return err
		}
	}
	return nil
}
