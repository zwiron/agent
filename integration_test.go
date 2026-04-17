package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowmem "github.com/apache/arrow-go/v18/arrow/memory"

	"fmt"

	"github.com/zwiron/connector"
	"github.com/zwiron/engine/runner"
	"github.com/zwiron/pkg/logger"
	agentv1 "github.com/zwiron/proto/gen/go/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ====================================================================
// Mock gRPC stream — captures events the agent sends to Atlas.

type mockStream struct {
	grpc.ClientStream
	mu     sync.Mutex
	events []*agentv1.ConnectRequest
}

func (m *mockStream) Send(req *agentv1.ConnectRequest) error {
	m.mu.Lock()
	m.events = append(m.events, req)
	m.mu.Unlock()
	return nil
}

func (m *mockStream) Recv() (*agentv1.ConnectResponse, error) {
	// Block forever — tests call handlers directly.
	select {}
}

func (m *mockStream) Header() (metadata.MD, error)  { return nil, nil }
func (m *mockStream) Trailer() metadata.MD          { return nil }
func (m *mockStream) CloseSend() error              { return nil }
func (m *mockStream) Context() context.Context      { return context.Background() }
func (m *mockStream) SendMsg(msg interface{}) error { return nil }
func (m *mockStream) RecvMsg(msg interface{}) error { return nil }

func (m *mockStream) completedEvents() []*agentv1.JobCompleted {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*agentv1.JobCompleted
	for _, e := range m.events {
		if jc := e.GetJobCompleted(); jc != nil {
			out = append(out, jc)
		}
	}
	return out
}

func (m *mockStream) failedEvents() []*agentv1.JobFailed {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*agentv1.JobFailed
	for _, e := range m.events {
		if jf := e.GetJobFailed(); jf != nil {
			out = append(out, jf)
		}
	}
	return out
}

func (m *mockStream) discoverResults() []*agentv1.DiscoverSchemaResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*agentv1.DiscoverSchemaResult
	for _, e := range m.events {
		if dr := e.GetDiscoverSchemaResult(); dr != nil {
			out = append(out, dr)
		}
	}
	return out
}

func (m *mockStream) testResults() []*agentv1.TestConnectionResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*agentv1.TestConnectionResult
	for _, e := range m.events {
		if tr := e.GetTestResult(); tr != nil {
			out = append(out, tr)
		}
	}
	return out
}

func (m *mockStream) validationEvents() []*agentv1.JobValidation {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*agentv1.JobValidation
	for _, e := range m.events {
		if jv := e.GetJobValidation(); jv != nil {
			out = append(out, jv)
		}
	}
	return out
}

// ====================================================================
// Mock connector — implements Source + Destination + Connection.

type testConnector struct {
	connectErr  error
	checkErr    error
	discoverErr error
	readErr     error
	writeErr    error

	catalog   *connector.Catalog
	readResp  connector.RecordStream
	writeResp *connector.WriteResult
	rowCount  int64

	connected bool
	closed    bool
}

func (c *testConnector) Check(_ context.Context, _ connector.Config) error { return c.checkErr }

func (c *testConnector) Connect(_ context.Context, _ connector.Config) error {
	if c.connectErr != nil {
		return c.connectErr
	}
	c.connected = true
	return nil
}

func (c *testConnector) Ping(_ context.Context) error { return nil }

func (c *testConnector) Close(_ context.Context) error {
	c.closed = true
	return nil
}

func (c *testConnector) EnsureTable(_ context.Context, _ *arrow.Schema, _, _ string, _ []string) error {
	return nil
}

func (c *testConnector) SetReadOnly(_ context.Context) error { return nil }

func (c *testConnector) Info() connector.ConnectionInfo {
	return connector.ConnectionInfo{ConnectorType: "test-integ"}
}

func (c *testConnector) Spec() connector.ConnectorSpec {
	return connector.ConnectorSpec{Name: "test-integ"}
}

func (c *testConnector) Discover(_ context.Context, _ connector.Config) (*connector.Catalog, error) {
	if c.discoverErr != nil {
		return nil, c.discoverErr
	}
	return c.catalog, nil
}

func (c *testConnector) Read(_ context.Context, _ connector.Config, _ *connector.ConfiguredCatalog, _ *connector.State) (connector.RecordStream, error) {
	if c.readErr != nil {
		return nil, c.readErr
	}
	return c.readResp, nil
}

func (c *testConnector) Write(_ context.Context, _ connector.Config, _ *connector.ConfiguredCatalog, stream connector.RecordStream) (*connector.WriteResult, error) {
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	var total int64
	for stream.Next() {
		total += stream.Record().NumRows()
	}
	if err := stream.Err(); err != nil {
		return nil, err
	}
	return &connector.WriteResult{RecordsWritten: total}, nil
}

func (c *testConnector) RowCount(_ context.Context, _ string) (int64, error) {
	return c.rowCount, nil
}

// ====================================================================
// Helpers

var integSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "name", Type: arrow.BinaryTypes.String},
}, nil)

func makeIntegRecord(n int) arrow.Record {
	pool := arrowmem.NewGoAllocator()
	b := array.NewRecordBuilder(pool, integSchema)
	defer b.Release()
	for i := 0; i < n; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i + 1))
		b.Field(1).(*array.StringBuilder).Append("row")
	}
	return b.NewRecord()
}

func integCatalog(namespace string, tables ...string) *connector.Catalog {
	streams := make([]connector.Stream, len(tables))
	for i, t := range tables {
		streams[i] = connector.Stream{
			Namespace:  namespace,
			Name:       t,
			Schema:     integSchema,
			PrimaryKey: []string{"id"},
			SyncModes:  []connector.SyncMode{connector.SyncModeFullRefresh},
		}
	}
	return &connector.Catalog{Streams: streams}
}

// encryptConfig encrypts a connector config for the given public key.
func encryptConfig(t *testing.T, pub *rsa.PublicKey, cfg map[string]any) []byte {
	t.Helper()
	plain, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	ct, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, plain, nil)
	if err != nil {
		t.Fatalf("encrypt config: %v", err)
	}
	return ct
}

// newTestAgent creates a minimal Agent wired with a mock stream. The agent
// has a real RSA key pair and a real runner but no actual gRPC connection.
func newTestAgent(t *testing.T) (*Agent, *mockStream) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	dataDir := t.TempDir()
	log := logger.New(logger.Config{}) // silent logger

	ms := &mockStream{}

	a := &Agent{
		log:     log,
		token:   "test-token",
		dataDir: dataDir,
		keys:    &KeyPair{Private: priv},
		stream:  ms,
	}

	a.reporter = newGRPCReporter(a)
	a.jobRunner = runner.New(runner.Config{
		Log:              log,
		Reporter:         a.reporter,
		CDCStore:         &fileCDCStore{agent: a},
		CheckpointDir:    dataDir,
		ProgressInterval: 100 * time.Millisecond,
	})

	return a, ms
}

// ====================================================================
// Integration tests

const (
	integSrcType connector.ConnectorType = "integ_test_src"
	integDstType connector.ConnectorType = "integ_test_dst"
)

// TestJobEndToEnd verifies the full job execution path: agent receives
// a StartJob → decrypts configs → resolves connectors → runs pipeline →
// reports completed with correct row count.
func TestJobEndToEnd(t *testing.T) {
	a, ms := newTestAgent(t)

	rec := makeIntegRecord(25)
	src := &testConnector{
		catalog:  integCatalog("public", "users"),
		readResp: connector.NewSliceRecordStream(integSchema, []arrow.Record{rec}),
	}
	dst := &testConnector{}

	connector.RegisterSource(integSrcType, func() connector.Source { return src })
	connector.RegisterDestination(integDstType, func() connector.Destination { return dst })

	cfg := map[string]any{"host": "localhost", "database": "testdb"}
	encCfg := encryptConfig(t, &a.keys.Private.PublicKey, cfg)

	cmd := &agentv1.StartJob{
		JobId:   "job-e2e-1",
		JobName: "test-e2e",
		RunId:   "run-1",
		Source: &agentv1.EncryptedConnection{
			ConnectionId:    "conn-src-1",
			ConnectorType:   string(integSrcType),
			EncryptedConfig: encCfg,
		},
		Dest: &agentv1.EncryptedConnection{
			ConnectionId:    "conn-dst-1",
			ConnectorType:   string(integDstType),
			EncryptedConfig: encCfg,
		},
		Workers: 1,
	}

	a.handleStartJob(context.Background(), cmd)

	completed := ms.completedEvents()
	if len(completed) != 1 {
		failed := ms.failedEvents()
		if len(failed) > 0 {
			t.Fatalf("job failed: %s", failed[0].GetErrorMessage())
		}
		t.Fatalf("expected 1 completed event, got %d", len(completed))
	}
	if completed[0].GetTotalRows() != 25 {
		t.Errorf("TotalRows = %d, want 25", completed[0].GetTotalRows())
	}
	if completed[0].GetJobId() != "job-e2e-1" {
		t.Errorf("JobId = %q, want job-e2e-1", completed[0].GetJobId())
	}
	if completed[0].GetRunId() != "run-1" {
		t.Errorf("RunId = %q, want run-1", completed[0].GetRunId())
	}
}

// TestJobWithQualifiedTableNames verifies that namespace-qualified table
// names (e.g. "source.customers") in the StartJob.tables field correctly
// match discovered streams with matching namespace+name. This is the
// regression test for the 0-rows bug.
func TestJobWithQualifiedTableNames(t *testing.T) {
	a, ms := newTestAgent(t)

	const srcType connector.ConnectorType = "integ_qualified_src"
	const dstType connector.ConnectorType = "integ_qualified_dst"

	rec := makeIntegRecord(50)
	src := &testConnector{
		catalog:  integCatalog("source", "customers", "orders"),
		readResp: connector.NewSliceRecordStream(integSchema, []arrow.Record{rec}),
	}
	dst := &testConnector{}

	connector.RegisterSource(srcType, func() connector.Source { return src })
	connector.RegisterDestination(dstType, func() connector.Destination { return dst })

	cfg := map[string]any{"host": "localhost", "database": "source"}
	encCfg := encryptConfig(t, &a.keys.Private.PublicKey, cfg)

	cmd := &agentv1.StartJob{
		JobId:   "job-qualified-1",
		JobName: "test-qualified",
		RunId:   "run-q-1",
		Source: &agentv1.EncryptedConnection{
			ConnectionId:    "conn-src",
			ConnectorType:   string(srcType),
			EncryptedConfig: encCfg,
		},
		Dest: &agentv1.EncryptedConnection{
			ConnectionId:    "conn-dst",
			ConnectorType:   string(dstType),
			EncryptedConfig: encCfg,
		},
		Tables:  []string{"source.customers", "source.orders"},
		Workers: 1,
	}

	a.handleStartJob(context.Background(), cmd)

	completed := ms.completedEvents()
	if len(completed) != 1 {
		failed := ms.failedEvents()
		if len(failed) > 0 {
			t.Fatalf("job failed: %s", failed[0].GetErrorMessage())
		}
		t.Fatalf("expected 1 completed event, got %d", len(completed))
	}
	if completed[0].GetTotalRows() == 0 {
		t.Fatal("TotalRows = 0, expected rows to be synced (regression: namespace-qualified table filter)")
	}
}

// TestJobDecryptFailure verifies that a bad encrypted config results in a
// JobFailed event with the correct error message.
func TestJobDecryptFailure(t *testing.T) {
	a, ms := newTestAgent(t)

	cmd := &agentv1.StartJob{
		JobId:   "job-decrypt-fail",
		JobName: "bad-config",
		RunId:   "run-df-1",
		Source: &agentv1.EncryptedConnection{
			ConnectionId:    "conn-src",
			ConnectorType:   "mysql",
			EncryptedConfig: []byte("not-encrypted"),
		},
		Dest: &agentv1.EncryptedConnection{
			ConnectionId:    "conn-dst",
			ConnectorType:   "snowflake",
			EncryptedConfig: []byte("not-encrypted"),
		},
		Workers: 1,
	}

	a.handleStartJob(context.Background(), cmd)

	failed := ms.failedEvents()
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed event, got %d", len(failed))
	}
	if failed[0].GetJobId() != "job-decrypt-fail" {
		t.Errorf("JobId = %q", failed[0].GetJobId())
	}
}

// TestJobUnknownConnector verifies that an unknown connector type results
// in a JobFailed event.
func TestJobUnknownConnector(t *testing.T) {
	a, ms := newTestAgent(t)

	cfg := map[string]any{"host": "localhost"}
	encCfg := encryptConfig(t, &a.keys.Private.PublicKey, cfg)

	cmd := &agentv1.StartJob{
		JobId:   "job-unknown-conn",
		JobName: "unknown-type",
		RunId:   "run-uc-1",
		Source: &agentv1.EncryptedConnection{
			ConnectionId:    "conn-src",
			ConnectorType:   "nonexistent_db_type",
			EncryptedConfig: encCfg,
		},
		Dest: &agentv1.EncryptedConnection{
			ConnectionId:    "conn-dst",
			ConnectorType:   "another_nonexistent",
			EncryptedConfig: encCfg,
		},
		Workers: 1,
	}

	a.handleStartJob(context.Background(), cmd)

	failed := ms.failedEvents()
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed event, got %d", len(failed))
	}
}

// TestJobConcurrencyLimit verifies that jobs are rejected when the
// concurrency limit is reached.
func TestJobConcurrencyLimit(t *testing.T) {
	a, ms := newTestAgent(t)
	a.maxJobs = 1
	a.jobSem = make(chan struct{}, 1)

	const srcType connector.ConnectorType = "integ_conc_src"
	const dstType connector.ConnectorType = "integ_conc_dst"

	// Register a connector that blocks until cancelled.
	blockCh := make(chan struct{})
	readCalled := make(chan struct{})
	src := &testConnector{
		catalog: integCatalog("public", "users"),
		readResp: &blockingRecordStream{
			schema:  integSchema,
			blockCh: blockCh,
			ready:   readCalled,
		},
	}
	dst := &testConnector{}

	connector.RegisterSource(srcType, func() connector.Source { return src })
	connector.RegisterDestination(dstType, func() connector.Destination { return dst })

	cfg := map[string]any{"host": "localhost", "database": "testdb"}
	encCfg := encryptConfig(t, &a.keys.Private.PublicKey, cfg)

	makeCmd := func(id, runID string) *agentv1.StartJob {
		return &agentv1.StartJob{
			JobId:   id,
			JobName: "conc-test",
			RunId:   runID,
			Source: &agentv1.EncryptedConnection{
				ConnectionId:    "conn-src",
				ConnectorType:   string(srcType),
				EncryptedConfig: encCfg,
			},
			Dest: &agentv1.EncryptedConnection{
				ConnectionId:    "conn-dst",
				ConnectorType:   string(dstType),
				EncryptedConfig: encCfg,
			},
			Workers: 1,
		}
	}

	// Start first job in background.
	go a.handleStartJob(context.Background(), makeCmd("job-conc-1", "run-1"))

	// Wait for the first job to actually start reading (so it holds the semaphore).
	<-readCalled

	// Second job should be rejected immediately.
	a.handleStartJob(context.Background(), makeCmd("job-conc-2", "run-2"))

	failed := ms.failedEvents()
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed event for rejected job, got %d", len(failed))
	}
	if failed[0].GetJobId() != "job-conc-2" {
		t.Errorf("rejected job_id = %q, want job-conc-2", failed[0].GetJobId())
	}

	// Unblock the first job.
	close(blockCh)
}

// TestDiscoverSchema verifies the discover flow: agent decrypts config,
// resolves connector, calls Discover, and sends tables back.
func TestDiscoverSchema(t *testing.T) {
	a, ms := newTestAgent(t)

	const srcType connector.ConnectorType = "integ_discover_src"

	src := &testConnector{
		catalog: integCatalog("mydb", "users", "orders", "products"),
	}
	connector.RegisterSource(srcType, func() connector.Source { return src })

	cfg := map[string]any{"host": "localhost", "database": "mydb"}
	encCfg := encryptConfig(t, &a.keys.Private.PublicKey, cfg)

	cmd := &agentv1.DiscoverSchema{
		RequestId:       "req-discover-1",
		ConnectorType:   string(srcType),
		EncryptedConfig: encCfg,
	}

	a.handleDiscoverSchema(context.Background(), cmd)

	results := ms.discoverResults()
	if len(results) != 1 {
		t.Fatalf("expected 1 discover result, got %d", len(results))
	}
	r := results[0]
	if !r.GetSuccess() {
		t.Fatalf("discover failed: %s", r.GetErrorMessage())
	}
	if len(r.GetTables()) != 3 {
		t.Fatalf("tables = %d, want 3", len(r.GetTables()))
	}
	if r.GetTables()[0].GetName() != "users" {
		t.Errorf("first table = %q, want users", r.GetTables()[0].GetName())
	}
	if r.GetTables()[0].GetNamespace() != "mydb" {
		t.Errorf("first table namespace = %q, want mydb", r.GetTables()[0].GetNamespace())
	}
	// Verify columns from integSchema.
	cols := r.GetTables()[0].GetColumns()
	if len(cols) != 2 {
		t.Fatalf("columns = %d, want 2", len(cols))
	}
	if cols[0].GetName() != "id" {
		t.Errorf("first column = %q, want id", cols[0].GetName())
	}
}

// TestDiscoverSchemaConnectError verifies discover reports failure when
// the connector can't connect.
func TestDiscoverSchemaConnectError(t *testing.T) {
	a, ms := newTestAgent(t)

	const srcType connector.ConnectorType = "integ_discover_err"

	src := &testConnector{
		connectErr: fmt.Errorf("connection refused"),
		catalog:    integCatalog("db", "t1"),
	}
	connector.RegisterSource(srcType, func() connector.Source { return src })

	cfg := map[string]any{"host": "bad-host"}
	encCfg := encryptConfig(t, &a.keys.Private.PublicKey, cfg)

	cmd := &agentv1.DiscoverSchema{
		RequestId:       "req-discover-err",
		ConnectorType:   string(srcType),
		EncryptedConfig: encCfg,
	}

	a.handleDiscoverSchema(context.Background(), cmd)

	results := ms.discoverResults()
	if len(results) != 1 {
		t.Fatalf("expected 1 discover result, got %d", len(results))
	}
	if results[0].GetSuccess() {
		t.Fatal("expected discover to fail")
	}
}

// TestTestConnection verifies the connection test flow.
func TestTestConnection(t *testing.T) {
	a, ms := newTestAgent(t)

	const srcType connector.ConnectorType = "integ_testconn_src"

	src := &testConnector{
		catalog: integCatalog("mydb", "users", "orders"),
	}
	connector.RegisterSource(srcType, func() connector.Source { return src })

	cfg := map[string]any{"host": "localhost", "database": "mydb"}
	encCfg := encryptConfig(t, &a.keys.Private.PublicKey, cfg)

	cmd := &agentv1.TestConnection{
		RequestId:       "req-test-1",
		ConnectionName:  "My MySQL",
		ConnectorType:   string(srcType),
		EncryptedConfig: encCfg,
	}

	a.handleTestConnection(context.Background(), cmd)

	results := ms.testResults()
	if len(results) != 1 {
		t.Fatalf("expected 1 test result, got %d", len(results))
	}
	r := results[0]
	if !r.GetSuccess() {
		t.Fatalf("test failed: %s", r.GetErrorMessage())
	}
	if len(r.GetTables()) != 2 {
		t.Errorf("tables = %d, want 2", len(r.GetTables()))
	}
}

// TestTestConnectionFailure verifies that a failed check reports correctly.
func TestTestConnectionFailure(t *testing.T) {
	a, ms := newTestAgent(t)

	const srcType connector.ConnectorType = "integ_testconn_fail"

	src := &testConnector{
		checkErr: fmt.Errorf("connection refused"),
	}
	connector.RegisterSource(srcType, func() connector.Source { return src })

	cfg := map[string]any{"host": "bad-host"}
	encCfg := encryptConfig(t, &a.keys.Private.PublicKey, cfg)

	cmd := &agentv1.TestConnection{
		RequestId:       "req-test-fail",
		ConnectionName:  "Bad MySQL",
		ConnectorType:   string(srcType),
		EncryptedConfig: encCfg,
	}

	a.handleTestConnection(context.Background(), cmd)

	results := ms.testResults()
	if len(results) != 1 {
		t.Fatalf("expected 1 test result, got %d", len(results))
	}
	if results[0].GetSuccess() {
		t.Fatal("expected test to fail")
	}
}

// TestJobEmptyDiscoverCompletesWithZeroRows verifies that when the source
// discovers 0 tables, the job completes (not fails) with 0 rows.
func TestJobEmptyDiscoverCompletesWithZeroRows(t *testing.T) {
	a, ms := newTestAgent(t)

	const srcType connector.ConnectorType = "integ_empty_src"
	const dstType connector.ConnectorType = "integ_empty_dst"

	src := &testConnector{
		catalog: &connector.Catalog{}, // empty — no tables
	}
	dst := &testConnector{}

	connector.RegisterSource(srcType, func() connector.Source { return src })
	connector.RegisterDestination(dstType, func() connector.Destination { return dst })

	cfg := map[string]any{"host": "localhost", "database": "empty"}
	encCfg := encryptConfig(t, &a.keys.Private.PublicKey, cfg)

	cmd := &agentv1.StartJob{
		JobId:   "job-empty",
		JobName: "empty-db",
		RunId:   "run-empty",
		Source: &agentv1.EncryptedConnection{
			ConnectionId:    "conn-src",
			ConnectorType:   string(srcType),
			EncryptedConfig: encCfg,
		},
		Dest: &agentv1.EncryptedConnection{
			ConnectionId:    "conn-dst",
			ConnectorType:   string(dstType),
			EncryptedConfig: encCfg,
		},
		Workers: 1,
	}

	a.handleStartJob(context.Background(), cmd)

	completed := ms.completedEvents()
	if len(completed) != 1 {
		failed := ms.failedEvents()
		if len(failed) > 0 {
			t.Fatalf("job failed (should complete with 0 rows): %s", failed[0].GetErrorMessage())
		}
		t.Fatalf("expected 1 completed event, got %d", len(completed))
	}
	if completed[0].GetTotalRows() != 0 {
		t.Errorf("TotalRows = %d, want 0", completed[0].GetTotalRows())
	}
}

// ====================================================================
// blockingRecordStream blocks Read until blockCh is closed.

type blockingRecordStream struct {
	schema  *arrow.Schema
	blockCh chan struct{}
	ready   chan struct{} // closed when Next() is first called
	once    sync.Once
}

func (b *blockingRecordStream) Schema() *arrow.Schema { return b.schema }

func (b *blockingRecordStream) Next() bool {
	b.once.Do(func() { close(b.ready) })
	<-b.blockCh
	return false
}

func (b *blockingRecordStream) Record() arrow.Record { return nil }
func (b *blockingRecordStream) Err() error           { return nil }
func (b *blockingRecordStream) Close() error         { return nil }
