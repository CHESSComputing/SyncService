// daemon_test.go
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	srvConfig "github.com/CHESSComputing/golib/config"
	"github.com/CHESSComputing/golib/services"
)

func init() {
	config := os.Getenv("FOXDEN_CONFIG")
	if cobj, err := srvConfig.ParseConfig(config); err == nil {
		srvConfig.Config = &cobj
	}
}

// -------------------- Mocks --------------------

// mockHTTPRead implements services.HTTPClient
type mockHTTPRead struct {
	responses map[string]string
}

func (m *mockHTTPRead) Get(rurl string) (*http.Response, error) {
	if body, ok := m.responses[rurl]; ok {
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	}
	return &http.Response{
		StatusCode: 404,
		Body:       io.NopCloser(strings.NewReader("not found")),
	}, nil
}

// implement stubs for other methods required by the interface
func (m *mockHTTPRead) Post(rurl, contentType string, buffer *bytes.Buffer) (*http.Response, error) {
	return nil, fmt.Errorf("not implemented in mock")
}
func (m *mockHTTPRead) Put(rurl, contentType string, buffer *bytes.Buffer) (*http.Response, error) {
	return nil, fmt.Errorf("not implemented in mock")
}
func (m *mockHTTPRead) Delete(rurl, contentType string, buffer *bytes.Buffer) (*http.Response, error) {
	return nil, fmt.Errorf("not implemented in mock")
}
func (m *mockHTTPRead) Request(method, rurl, contentType string, buffer *bytes.Buffer) (*http.Response, error) {
	return nil, fmt.Errorf("not implemented in mock")
}
func (m *mockHTTPRead) GetToken() {
}
func (m *mockHTTPRead) SetToken(token string) {
}
func (m *mockHTTPRead) PostForm(rurl string, formData url.Values) (*http.Response, error) {
	return nil, fmt.Errorf("not implemented in mock")
}

// mockHTTPWrite implements services.HTTPClient
type mockHTTPWrite struct {
	Token    string
	lastReq  map[string][]byte // url -> body bytes (for inspection)
	errs     map[string]error  // url -> error
	statusOK map[string]bool   // url -> return 200 or non-200
	mu       sync.Mutex
}

func (m *mockHTTPWrite) Post(url, contentType string, body *bytes.Buffer) (*http.Response, error) {
	if err, ok := m.errs[url]; ok && err != nil {
		return nil, err
	}

	b := body.Bytes() // safe since body is *bytes.Buffer

	m.mu.Lock()
	if m.lastReq == nil {
		m.lastReq = map[string][]byte{}
	}
	m.lastReq[url] = b
	m.mu.Unlock()

	status := 200
	if ok, found := m.statusOK[url]; found && !ok {
		status = 500
	}

	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(bytes.NewBufferString(`{"ok":true}`)),
		Header:     make(http.Header),
	}, nil
}

// implement stubs for other methods required by the interface
func (m *mockHTTPWrite) Get(rurl string) (*http.Response, error) {
	return nil, fmt.Errorf("not implemented in mock")
}
func (m *mockHTTPWrite) Put(rurl, contentType string, buffer *bytes.Buffer) (*http.Response, error) {
	return nil, fmt.Errorf("not implemented in mock")
}
func (m *mockHTTPWrite) Delete(rurl, contentType string, buffer *bytes.Buffer) (*http.Response, error) {
	return nil, fmt.Errorf("not implemented in mock")
}
func (m *mockHTTPWrite) Request(method, rurl, contentType string, buffer *bytes.Buffer) (*http.Response, error) {
	return nil, fmt.Errorf("not implemented in mock")
}
func (m *mockHTTPWrite) GetToken() {
}
func (m *mockHTTPWrite) SetToken(token string) {
}
func (m *mockHTTPWrite) PostForm(rurl string, formData url.Values) (*http.Response, error) {
	return nil, fmt.Errorf("not implemented in mock")
}

// mockMetaDB implements minimal methods used by daemon.go:
// Get(db, coll string, spec map[string]any, idx, limit int) []map[string]any
// Update(db, coll string, spec map[string]any, newRecord map[string]any) error
type mockMetaDB struct {
	gets    map[string][]map[string]any
	updates []struct {
		db, coll string
		spec     map[string]any
		newRec   map[string]any
	}
	mu sync.Mutex
	// optionally, an Update hook
	UpdateHook func(db, coll string, spec map[string]any, newRecord map[string]any) error
}

func (m *mockMetaDB) Count(_, _ string, query map[string]interface{}) int {
	// return whatever fits your test
	return 0
}

func (m *mockMetaDB) InitDB(uri string) {
	// not implemented in mock
}
func (m *mockMetaDB) Insert(dbname, collname string, records []map[string]any) {
}
func (m *mockMetaDB) Upsert(dbname, collname, attr string, records []map[string]any) error {
	return nil
}
func (m *mockMetaDB) GetProjection(dbname, collname string, spec map[string]any, projection map[string]int, idx, limit int) []map[string]any {
	var records []map[string]any
	return records
}
func (m *mockMetaDB) Remove(dbname, collname string, spec map[string]any) error {
	return nil
}
func (m *mockMetaDB) Distinct(dbname, collname, field string) ([]any, error) {
	var out []any
	return out, nil
}
func (m *mockMetaDB) InsertRecord(dbname, collname string, rec map[string]any) error {
	return nil
}
func (m *mockMetaDB) GetSorted(dbname, collname string, spec map[string]any, skeys []string, sortOrder, idx, limit int) []map[string]any {
	var records []map[string]any
	return records
}

func (m *mockMetaDB) Get(db, coll string, spec map[string]any, idx, limit int) []map[string]any {
	// key by uuid if present, otherwise use special key ""
	key := ""
	if spec != nil {
		if v, ok := spec["uuid"]; ok {
			key = fmt.Sprintf("%v", v)
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if recs, ok := m.gets[key]; ok {
		// respect idx/limit (simple slice)
		start := idx
		end := len(recs)
		if limit > 0 && start+limit < end {
			end = start + limit
		}
		if start >= len(recs) {
			return []map[string]any{}
		}
		return append([]map[string]any(nil), recs[start:end]...)
	}
	return []map[string]any{}
}

func (m *mockMetaDB) Update(db, coll string, spec map[string]any, newRecord map[string]any) error {
	m.mu.Lock()
	m.updates = append(m.updates, struct {
		db, coll string
		spec     map[string]any
		newRec   map[string]any
	}{db, coll, spec, newRecord})
	m.mu.Unlock()
	if m.UpdateHook != nil {
		return m.UpdateHook(db, coll, spec, newRecord)
	}
	return nil
}

// -------------------- Helpers for tests --------------------

func withReplacedGlobals(t *testing.T, replace func(), restore func()) {
	replace()
	t.Cleanup(restore)
}

// small helper to create a service-like JSON wrapper used in getRecords fallback
func serviceResponseJSON(records []map[string]any) string {
	sr := services.ServiceResponse{
		Results: services.ServiceResults{
			Records: records,
		},
	}
	b, _ := jsonMarshal(sr)
	return string(b)
}

// tiny json marshal wrapper to avoid importing testing-specific tags
func jsonMarshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// -------------------- Tests --------------------

func Test_getFoxdenUrl(t *testing.T) {
	// metadata
	u := getFoxdenUrl("metadata", "http://host", "did:1")
	if u != "http://host/record?did=did:1" {
		t.Fatalf("unexpected metadata url: %s", u)
	}
	// provenance
	u = getFoxdenUrl("provenance", "http://host", "did:1")
	if u != "http://host/provenance?did=did:1" {
		t.Fatalf("unexpected provenance url: %s", u)
	}
}

func Test_getRecords_jsonArray_and_serviceResponse(t *testing.T) {
	// set up mock read request
	mr := &mockHTTPRead{
		responses: map[string]string{
			"http://a/array": `[{"did":"d1","x":1},{"did":"d2","x":2}]`,
			"http://a/wrap":  `{"results": {"records": [{"did":"d3","x":3}]}}`,
		},
	}
	origRead := _httpReadRequest
	origVerbose := Verbose
	withReplacedGlobals(t, func() {
		_httpReadRequest = mr
		Verbose = 0
	}, func() {
		_httpReadRequest = origRead
		Verbose = origVerbose
	})

	// array case
	recs, err := getRecords("http://a/array", "tok")
	if err != nil {
		t.Fatalf("getRecords array returned err: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("expected 2 records, got %d", len(recs))
	}
	if fmt.Sprintf("%v", recs[0]["did"]) != "d1" {
		t.Fatalf("unexpected record[0].did: %v", recs[0]["did"])
	}

	// service response wrapper case
	recs2, err := getRecords("http://a/wrap", "tok")
	if err != nil {
		t.Fatalf("getRecords wrap returned err: %v", err)
	}
	if len(recs2) != 1 || fmt.Sprintf("%v", recs2[0]["did"]) != "d3" {
		t.Fatalf("unexpected wrapped results: %+v", recs2)
	}
}

func Test_pushRecord_metadata_and_provenance(t *testing.T) {
	mw := &mockHTTPWrite{
		statusOK: map[string]bool{},
		errs:     map[string]error{},
		lastReq:  map[string][]byte{},
	}
	orig := _httpWriteRequest
	withReplacedGlobals(t, func() {
		_httpWriteRequest = mw
	}, func() {
		_httpWriteRequest = orig
	})

	// provenance record (no schema required)
	provRec := map[string]any{"did": "p1", "who": "x"}
	err := pushRecord("http://target/provenance", "token", provRec)
	if err != nil {
		t.Fatalf("pushRecord provenance failed: %v", err)
	}
	// should have recorded last request body
	mw.mu.Lock()
	_, ok := mw.lastReq["http://target/provenance"]
	mw.mu.Unlock()
	if !ok {
		t.Fatalf("expected request to provenance url to be saved")
	}

	// metadata record requires schema
	metaRec := map[string]any{"did": "m1", "schema": "s1", "field": "v"}
	err = pushRecord("http://target/record", "token", metaRec)
	if err != nil {
		t.Fatalf("pushRecord metadata failed: %v", err)
	}

	// missing schema should return error
	badMeta := map[string]any{"did": "m2", "field": "v"}
	err = pushRecord("http://target/record", "token", badMeta)
	if err == nil {
		t.Fatalf("pushRecord should have failed due missing schema")
	}
}

func Test_pushRecords_errorPropagation(t *testing.T) {
	mw := &mockHTTPWrite{
		errs:    map[string]error{"http://target/record": errors.New("boom")},
		lastReq: map[string][]byte{},
	}
	orig := _httpWriteRequest
	withReplacedGlobals(t, func() {
		_httpWriteRequest = mw
	}, func() {
		_httpWriteRequest = orig
	})

	recs := []map[string]any{{"did": "x", "schema": "s"}}
	err := pushRecords("metadata", "http://target", "tok", recs)
	if err == nil {
		t.Fatalf("expected pushRecords to return error when pushRecord fails")
	}
}

func Test_getDidRecords_and_concurrent(t *testing.T) {
	// setup read mock returning different payloads per did URL
	respMap := map[string]string{}
	dids := []string{"d1", "d2", "d3", "d4"}
	for _, d := range dids {
		// create URL that getFoxdenUrl will produce
		rmeta := getFoxdenUrl("metadata", "http://src", d)
		respMap[rmeta] = fmt.Sprintf(`[{"did":"%s","x":%s}]`, d, `"val"`)
	}
	mr := &mockHTTPRead{responses: respMap}
	orig := _httpReadRequest
	withReplacedGlobals(t, func() {
		_httpReadRequest = mr
	}, func() {
		_httpReadRequest = orig
	})

	// sequential
	seq := getDidRecords("metadata", "http://src", dids, "tok")
	if len(seq) != len(dids) {
		t.Fatalf("expected %d records, got %d (seq)", len(dids), len(seq))
	}

	// concurrent with worker pool
	con := getDidRecordsConcurrent("metadata", "http://src", dids, "tok", 2)
	if len(con) != len(dids) {
		t.Fatalf("expected %d records, got %d (concurrent)", len(dids), len(con))
	}
}

func Test_getDIDs_and_updateRecords_flow_with_workers(t *testing.T) {

	// This integrates:
	// - getDIDs which uses getRecords internally.
	// - updateRecords which uses getDIDs, getDidRecordsConcurrent, pushRecords.
	// We'll set source to have 3 dids, target to have 1 did so 2 diffs to sync.
	srcBase := "http://src"
	tgtBase := "http://tgt"

	// build responses
	resp := map[string]string{}
	// source /dids endpoints
	resp[fmt.Sprintf("%s/dids", srcBase)] = `[{"did":"a"},{"did":"b"},{"did":"c"}]`
	// target /dids endpoint
	resp[fmt.Sprintf("%s/dids", tgtBase)] = `[{"did":"c"}]`
	// for each did, source record endpoint
	for _, d := range []string{"a", "b"} {
		// metadata record endpoints
		resp[getFoxdenUrl("metadata", srcBase, d)] = fmt.Sprintf(`[{"did":"%s","schema":"S","x":1}]`, d)
		// provenance endpoints too (updateRecords calls getDIDs for provenance or metadata separately)
		resp[getFoxdenUrl("provenance", srcBase, d)] = fmt.Sprintf(`[{"did":"%s","schema":"S","actor":"me"}]`, d)
	}
	// mock readers and writers
	mr := &mockHTTPRead{responses: resp}
	mw := &mockHTTPWrite{
		statusOK: map[string]bool{},
		errs:     map[string]error{},
		lastReq:  map[string][]byte{},
	}

	// metaDB: need to track Update calls made by updateSyncRecordStatus invoked by syncWorker
	mdb := &mockMetaDB{
		gets: map[string][]map[string]any{
			// for printSyncRecordStatus called with uuid "u1", return a record with status set later by Update
			"u1": {{"uuid": "u1", "status": "initial"}},
		},
	}

	// set config: use NWorkers to pick concurrent code path
	prevWorkers := srvConfig.Config.Sync.NWorkers
	origRead := _httpReadRequest
	origWrite := _httpWriteRequest
	origMeta := metaDB

	withReplacedGlobals(t, func() {
		_httpReadRequest = mr
		_httpWriteRequest = mw
		metaDB = mdb
		srvConfig.Config.Sync.NWorkers = 2
	}, func() {
		_httpReadRequest = origRead
		_httpWriteRequest = origWrite
		metaDB = origMeta
		srvConfig.Config.Sync.NWorkers = prevWorkers
	})

	// create a syncRecord to pass to updateRecords
	syncRec := map[string]any{
		"source_url":   srcBase,
		"target_url":   tgtBase,
		"source_token": "stok",
		"target_token": "ttok",
	}

	// First call updateMetadataRecords (calls updateRecords with "metadata")
	if err := updateMetadataRecords(syncRec); err != nil {
		t.Fatalf("updateMetadataRecords failed: %v", err)
	}
	// Then provenance
	if err := updateProvenanceRecords(syncRec); err != nil {
		t.Fatalf("updateProvenanceRecords failed: %v", err)
	}
	// verify that at least some push requests happened
	mw.mu.Lock()
	if len(mw.lastReq) == 0 {
		mw.mu.Unlock()
		t.Fatalf("expected some push requests to be made")
	}
	mw.mu.Unlock()
}

func Test_updateSyncRecordStatus_and_print(t *testing.T) {
	// prepare metaDB mock to capture Update and return Get result
	mdb := &mockMetaDB{
		gets: map[string][]map[string]any{
			"uu": {{"uuid": "uu", "status": "abc"}},
		},
	}
	origMeta := metaDB
	withReplacedGlobals(t, func() {
		metaDB = mdb
	}, func() {
		metaDB = origMeta
	})

	// call updateSyncRecordStatus
	err := updateSyncRecordStatus("uu", "testing", 123)
	if err != nil {
		t.Fatalf("updateSyncRecordStatus returned err: %v", err)
	}
	// ensure update was recorded
	if len(mdb.updates) == 0 {
		t.Fatalf("expected metaDB.Update to be called")
	}
	// ensure printSyncRecordStatus didn't panic (it calls Get)
	printSyncRecordStatus("uu")
}

func Test_syncWorker_fullFlow(t *testing.T) {
	// Test the end-to-end flow of syncWorker with mocked get/push/update to confirm order
	srcBase := "http://src2"
	tgtBase := "http://tgt2"

	// make source list of dids with one DID for simplicity
	srcDidsJSON := `[{"did":"z1"}]`
	// records returned for metadata and provenance
	metaRecJSON := `[{"did":"z1","schema":"S","field":"v"}]`
	provRecJSON := `[{"did":"z1","schema":"S","actor":"A"}]`

	// responses for both metadata and provenance /dids and record endpoints
	resp := map[string]string{
		fmt.Sprintf("%s/dids", srcBase):           srcDidsJSON,
		fmt.Sprintf("%s/dids", tgtBase):           `[]`,
		getFoxdenUrl("metadata", srcBase, "z1"):   metaRecJSON,
		getFoxdenUrl("provenance", srcBase, "z1"): provRecJSON,
		fmt.Sprintf("%s/dids", srcBase):           srcDidsJSON,
		fmt.Sprintf("%s/dids", tgtBase):           `[]`,
	}

	// http read and write mocks
	mr := &mockHTTPRead{responses: resp}
	mw := &mockHTTPWrite{
		lastReq: map[string][]byte{},
	}

	// metaDB: watch Update calls and supply Get for print
	mdb := &mockMetaDB{
		gets: map[string][]map[string]any{
			"suuid": {{"uuid": "suuid", "status": "initial"}},
		},
	}
	// capture update status codes in order
	var statusCodes []int
	mdb.UpdateHook = func(db, coll string, spec map[string]any, newRecord map[string]any) error {
		if sc, ok := newRecord["$set"].(map[string]any)["status_code"]; ok {
			// status_code is an int in the daemon code
			if v, ok2 := sc.(int); ok2 {
				statusCodes = append(statusCodes, v)
			} else if vf, ok3 := sc.(float64); ok3 {
				// json numbers become float64 sometimes; accept both
				statusCodes = append(statusCodes, int(vf))
			}
		}
		return nil
	}

	// set config and globals
	prevWorkers := srvConfig.Config.Sync.NWorkers
	origRead := _httpReadRequest
	origWrite := _httpWriteRequest
	origMeta := metaDB
	withReplacedGlobals(t, func() {
		_httpReadRequest = mr
		_httpWriteRequest = mw
		metaDB = mdb
		srvConfig.Config.Sync.NWorkers = 2
	}, func() {
		_httpReadRequest = origRead
		_httpWriteRequest = origWrite
		metaDB = origMeta
		srvConfig.Config.Sync.NWorkers = prevWorkers
	})

	// build syncRecord used by syncWorker
	syncRec := map[string]any{
		"uuid":         "suuid",
		"source_url":   srcBase,
		"target_url":   tgtBase,
		"source_token": "stok",
		"target_token": "ttok",
	}

	// run syncWorker
	if err := syncWorker(syncRec); err != nil {
		t.Fatalf("syncWorker failed: %v", err)
	}

	// Expect multiple status updates (InProgress, SyncMetadata, InProgress, SyncProvenance, Completed)
	if len(statusCodes) < 3 {
		t.Fatalf("expected at least 3 status updates, got %d", len(statusCodes))
	}
}
