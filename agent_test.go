package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/zwiron/pkg/logger"
	agentv1 "github.com/zwiron/proto/gen/go/agent/v1"
)

func TestDecryptConfig_RoundTrip(t *testing.T) {
	t.Parallel()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	original := map[string]any{"host": "localhost", "port": float64(5432)}
	plaintext, _ := json.Marshal(original)
	ciphertext, err := rsa.EncryptOAEP(sha256.New(), rand.Reader, &priv.PublicKey, plaintext, nil)
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	cfg, err := DecryptConfig(priv, ciphertext)
	if err != nil {
		t.Fatalf("DecryptConfig: %v", err)
	}
	if cfg.GetString("host") != "localhost" {
		t.Fatalf("host = %q", cfg.GetString("host"))
	}
}

func TestDecryptConfig_BadCiphertext(t *testing.T) {
	t.Parallel()
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	_, err := DecryptConfig(priv, []byte("not-encrypted"))
	if err == nil {
		t.Fatal("expected error for bad ciphertext")
	}
}

func TestDecryptConfig_InvalidJSON(t *testing.T) {
	t.Parallel()
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	ciphertext, _ := rsa.EncryptOAEP(sha256.New(), rand.Reader, &priv.PublicKey, []byte("not-json"), nil)
	_, err := DecryptConfig(priv, ciphertext)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestKeyPairFromPrivate(t *testing.T) {
	t.Parallel()
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	kp, err := keyPairFromPrivate(priv)
	if err != nil {
		t.Fatalf("keyPairFromPrivate: %v", err)
	}
	if kp.Private != priv {
		t.Fatal("Private key mismatch")
	}
	if kp.PublicPEM == "" {
		t.Fatal("expected non-empty PublicPEM")
	}
	if !contains(kp.PublicPEM, "BEGIN PUBLIC KEY") {
		t.Fatalf("PublicPEM missing header: %s", kp.PublicPEM[:40])
	}
}

func TestCdcPositionKey_Deterministic(t *testing.T) {
	t.Parallel()
	cmd := &agentv1.StartJob{
		Source: &agentv1.EncryptedConnection{ConnectionId: "src1"},
		Dest:   &agentv1.EncryptedConnection{ConnectionId: "dst1"},
	}
	k1 := cdcPositionKey(cmd)
	k2 := cdcPositionKey(cmd)
	if k1 != k2 {
		t.Fatalf("keys differ: %q vs %q", k1, k2)
	}
	if len(k1) != 16 {
		t.Fatalf("key len = %d, want 16", len(k1))
	}
}

func TestCdcPositionKey_DifferentInputs(t *testing.T) {
	t.Parallel()
	cmd1 := &agentv1.StartJob{
		Source: &agentv1.EncryptedConnection{ConnectionId: "src1"},
		Dest:   &agentv1.EncryptedConnection{ConnectionId: "dst1"},
	}
	cmd2 := &agentv1.StartJob{
		Source: &agentv1.EncryptedConnection{ConnectionId: "src2"},
		Dest:   &agentv1.EncryptedConnection{ConnectionId: "dst1"},
	}
	if cdcPositionKey(cmd1) == cdcPositionKey(cmd2) {
		t.Fatal("different inputs should produce different keys")
	}
}

func TestProtoToTransformSpec_Nil(t *testing.T) {
	t.Parallel()
	if protoToTransformSpec(nil) != nil {
		t.Fatal("expected nil for nil input")
	}
}

func TestProtoToTransformSpec_Empty(t *testing.T) {
	t.Parallel()
	if protoToTransformSpec([]*agentv1.TransformRule{}) != nil {
		t.Fatal("expected nil for empty input")
	}
}

func TestProtoToTransformSpec_WithRules(t *testing.T) {
	t.Parallel()
	rules := []*agentv1.TransformRule{
		{
			Table: "users",
			Columns: []*agentv1.ColumnMapping{
				{Source: "email", Dest: "user_email"},
				{Source: "ssn", Drop: true},
			},
			FilterConditions: []*agentv1.FilterCondition{
				{Column: "active", Op: "=", Value: "true"},
			},
		},
	}
	spec := protoToTransformSpec(rules)
	if spec == nil {
		t.Fatal("expected non-nil spec")
	}
	if len(spec.Tables) != 1 {
		t.Fatalf("tables = %d, want 1", len(spec.Tables))
	}
	tt := spec.Tables[0]
	if tt.Table != "users" {
		t.Fatalf("table = %q", tt.Table)
	}
	if len(tt.Columns) != 2 {
		t.Fatalf("columns = %d, want 2", len(tt.Columns))
	}
	if tt.Columns[0].Dest != "user_email" {
		t.Fatalf("col[0].Dest = %q", tt.Columns[0].Dest)
	}
	if !tt.Columns[1].Drop {
		t.Fatal("col[1].Drop should be true")
	}
	if len(tt.Filters) != 1 {
		t.Fatalf("filters = %d, want 1", len(tt.Filters))
	}
	if tt.Filters[0].Column != "active" {
		t.Fatalf("filter.Column = %q", tt.Filters[0].Column)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstring(s, substr)
}

func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func testLogger() *logger.Logger {
	return logger.New(logger.Config{Level: "error", ServiceName: "test", Format: "json"})
}

// --------------- Key management tests ---------------

func TestLoadOrGenerateKeys_NewKey(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "agent.key")
	kp, err := LoadOrGenerateKeys(path, testLogger())
	if err != nil {
		t.Fatalf("LoadOrGenerateKeys: %v", err)
	}
	if kp == nil {
		t.Fatal("expected non-nil KeyPair")
	}
	if kp.Private == nil {
		t.Fatal("expected non-nil Private key")
	}
	if !contains(kp.PublicPEM, "BEGIN PUBLIC KEY") {
		t.Fatal("PublicPEM missing header")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("key file not created on disk: %v", err)
	}
}

func TestLoadOrGenerateKeys_ExistingKey(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "agent.key")

	kp1, err := LoadOrGenerateKeys(path, testLogger())
	if err != nil {
		t.Fatalf("first call: %v", err)
	}

	kp2, err := LoadOrGenerateKeys(path, testLogger())
	if err != nil {
		t.Fatalf("second call: %v", err)
	}

	if kp1 == nil || kp2 == nil {
		t.Fatal("expected non-nil key pairs")
	}
	if kp1.Private == nil || kp2.Private == nil {
		t.Fatal("expected non-nil Private keys")
	}
}

func TestLoadOrGenerateKeys_Idempotent(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "agent.key")

	kp1, err := LoadOrGenerateKeys(path, testLogger())
	if err != nil {
		t.Fatalf("first call: %v", err)
	}

	kp2, err := LoadOrGenerateKeys(path, testLogger())
	if err != nil {
		t.Fatalf("second call: %v", err)
	}

	if kp1.PublicPEM != kp2.PublicPEM {
		t.Fatal("PublicPEM differs between generate and load")
	}
}

func TestLoadKeys_BadPEM(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "bad.key")
	if err := os.WriteFile(path, []byte("not-a-pem-file"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := loadKeys(path)
	if err == nil {
		t.Fatal("expected error for bad PEM data")
	}
}

func TestLoadKeys_BadDER(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "bad.key")
	block := &pem.Block{Type: "RSA PRIVATE KEY", Bytes: []byte("garbage")}
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := loadKeys(path)
	if err == nil {
		t.Fatal("expected error for bad DER data")
	}
}

// --------------- CDC position persistence tests ---------------

func TestCDCPositions_SaveAndLoad(t *testing.T) {
	t.Parallel()
	a := &Agent{
		dataDir: t.TempDir(),
		mu:      sync.Mutex{},
	}
	if err := a.saveCDCPosition("key1", []byte("pos1")); err != nil {
		t.Fatalf("saveCDCPosition: %v", err)
	}
	got, err := a.loadCDCPosition("key1")
	if err != nil {
		t.Fatalf("loadCDCPosition: %v", err)
	}
	if !bytes.Equal(got, []byte("pos1")) {
		t.Fatalf("position = %q, want %q", got, "pos1")
	}
}

func TestCDCPositions_LoadNonExistent(t *testing.T) {
	t.Parallel()
	a := &Agent{
		dataDir: t.TempDir(),
		mu:      sync.Mutex{},
	}
	got, err := a.loadCDCPosition("key1")
	if err != nil {
		t.Fatalf("loadCDCPosition: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil, got %q", got)
	}
}

func TestCDCPositions_MultipleKeys(t *testing.T) {
	t.Parallel()
	a := &Agent{
		dataDir: t.TempDir(),
		mu:      sync.Mutex{},
	}
	if err := a.saveCDCPosition("key1", []byte("pos1")); err != nil {
		t.Fatal(err)
	}
	if err := a.saveCDCPosition("key2", []byte("pos2")); err != nil {
		t.Fatal(err)
	}

	got1, err := a.loadCDCPosition("key1")
	if err != nil {
		t.Fatal(err)
	}
	got2, err := a.loadCDCPosition("key2")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got1, []byte("pos1")) {
		t.Fatalf("key1 = %q, want %q", got1, "pos1")
	}
	if !bytes.Equal(got2, []byte("pos2")) {
		t.Fatalf("key2 = %q, want %q", got2, "pos2")
	}
}

func TestCDCPositions_Overwrite(t *testing.T) {
	t.Parallel()
	a := &Agent{
		dataDir: t.TempDir(),
		mu:      sync.Mutex{},
	}
	if err := a.saveCDCPosition("key1", []byte("pos1")); err != nil {
		t.Fatal(err)
	}
	if err := a.saveCDCPosition("key1", []byte("pos2")); err != nil {
		t.Fatal(err)
	}
	got, err := a.loadCDCPosition("key1")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("pos2")) {
		t.Fatalf("key1 = %q, want %q", got, "pos2")
	}
}

// --------------- Job logger test ---------------

func TestNewJobLogger(t *testing.T) {
	t.Parallel()
	l := newJobLogger(testLogger(), "job-1")
	if l == nil {
		t.Fatal("expected non-nil logger")
	}
}

// --------------- Transform conversion tests ---------------

func TestProtoToTransformSpec_MultipleTablesWithFilters(t *testing.T) {
	t.Parallel()
	rules := []*agentv1.TransformRule{
		{
			Table: "users",
			Columns: []*agentv1.ColumnMapping{
				{Source: "email", Dest: "user_email"},
			},
		},
		{
			Table: "orders",
			Columns: []*agentv1.ColumnMapping{
				{Source: "total", Cast: "float64"},
			},
			FilterConditions: []*agentv1.FilterCondition{
				{Column: "status", Op: "=", Value: "active"},
				{Column: "amount", Op: ">", Value: "100"},
			},
		},
	}
	spec := protoToTransformSpec(rules)
	if spec == nil {
		t.Fatal("expected non-nil spec")
	}
	if len(spec.Tables) != 2 {
		t.Fatalf("tables = %d, want 2", len(spec.Tables))
	}
	// First table.
	if spec.Tables[0].Table != "users" {
		t.Errorf("table[0] = %q", spec.Tables[0].Table)
	}
	if len(spec.Tables[0].Columns) != 1 {
		t.Fatalf("table[0] columns = %d, want 1", len(spec.Tables[0].Columns))
	}
	// Second table with cast and filters.
	if spec.Tables[1].Columns[0].Cast != "float64" {
		t.Errorf("table[1] col[0].Cast = %q", spec.Tables[1].Columns[0].Cast)
	}
	if len(spec.Tables[1].Filters) != 2 {
		t.Fatalf("table[1] filters = %d, want 2", len(spec.Tables[1].Filters))
	}
	if spec.Tables[1].Filters[0].Op != "=" {
		t.Errorf("filter[0].Op = %q", spec.Tables[1].Filters[0].Op)
	}
	if spec.Tables[1].Filters[1].Op != ">" {
		t.Errorf("filter[1].Op = %q", spec.Tables[1].Filters[1].Op)
	}
}

func TestProtoToTransformSpec_DropAndHash(t *testing.T) {
	t.Parallel()
	rules := []*agentv1.TransformRule{
		{
			Table: "users",
			Columns: []*agentv1.ColumnMapping{
				{Source: "ssn", Drop: true},
				{Source: "id"},
			},
		},
	}
	spec := protoToTransformSpec(rules)
	if spec == nil {
		t.Fatal("expected non-nil spec")
	}
	if !spec.Tables[0].Columns[0].Drop {
		t.Error("col[0].Drop should be true")
	}
	if spec.Tables[0].Columns[1].Drop {
		t.Error("col[1].Drop should be false")
	}
}

// --------------- HandleCancelJob tests ---------------

func TestHandleCancelJob_ExistingJob(t *testing.T) {
	t.Parallel()
	var cancelled bool
	a := &Agent{
		log:     testLogger(),
		dataDir: t.TempDir(),
		mu:      sync.Mutex{},
		cancels: map[string]context.CancelFunc{
			"job-1": func() { cancelled = true },
		},
	}
	a.handleCancelJob(context.Background(), &agentv1.CancelJob{
		JobId:  "job-1",
		Reason: "user requested",
	})
	if !cancelled {
		t.Fatal("expected cancel function to be called")
	}
}

func TestHandleCancelJob_NonExistentJob(t *testing.T) {
	t.Parallel()
	a := &Agent{
		log:     testLogger(),
		dataDir: t.TempDir(),
		mu:      sync.Mutex{},
		cancels: map[string]context.CancelFunc{},
	}
	// Should not panic.
	a.handleCancelJob(context.Background(), &agentv1.CancelJob{
		JobId:  "nonexistent",
		Reason: "test",
	})
}

// --------------- CDC position key edge cases ---------------

func TestCdcPositionKey_EmptyInputs(t *testing.T) {
	t.Parallel()
	cmd := &agentv1.StartJob{
		Source: &agentv1.EncryptedConnection{ConnectionId: ""},
		Dest:   &agentv1.EncryptedConnection{ConnectionId: ""},
	}
	k := cdcPositionKey(cmd)
	if len(k) != 16 {
		t.Fatalf("key len = %d, want 16", len(k))
	}
}

func TestCdcPositionKey_NilConnections(t *testing.T) {
	t.Parallel()
	cmd := &agentv1.StartJob{}
	k := cdcPositionKey(cmd)
	if len(k) != 16 {
		t.Fatalf("key len = %d, want 16", len(k))
	}
}

// --------------- Pause / Resume ---------------

func TestHandlePause_CancelsRunningJobs(t *testing.T) {
	t.Parallel()
	var cancelled1, cancelled2 bool
	a := &Agent{
		log:     testLogger(),
		dataDir: t.TempDir(),
		mu:      sync.Mutex{},
		cancels: map[string]context.CancelFunc{
			"job-1": func() { cancelled1 = true },
			"job-2": func() { cancelled2 = true },
		},
	}
	a.handlePause(context.Background(), &agentv1.PauseAgent{Reason: "maintenance"})
	if !cancelled1 || !cancelled2 {
		t.Fatal("expected both jobs to be cancelled")
	}
	if !a.paused {
		t.Fatal("expected agent to be paused")
	}
}
