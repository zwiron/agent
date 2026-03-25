package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/json"
	"testing"

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
