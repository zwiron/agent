package main

import (
	"testing"

	"github.com/zwiron/engine/validation"
)

// ---------------------------------------------------------------------------
// Report builder tests — uses shared validation result types
// ---------------------------------------------------------------------------

func TestBuildValidationReport_AllMatch(t *testing.T) {
	structural := []validation.StructuralResult{
		{Table: "users", SourceRows: 100, DestRows: 100, Match: true},
		{Table: "orders", SourceRows: 200, DestRows: 200, Match: true},
	}
	content := []validation.ContentResult{
		{Table: "users", TotalRows: 100, SourceChecksum: "abc", DestChecksum: "abc", Match: true},
		{Table: "orders", TotalRows: 200, SourceChecksum: "def", DestChecksum: "def", Match: true},
	}
	schemas := []validation.SchemaResult{
		{Table: "users", Match: true},
		{Table: "orders", Match: true},
	}
	semantic := []validation.SemanticResult{
		{RuleName: "sum_check", Passed: true},
	}

	r := buildValidationReport("job1", "run1", structural, content, schemas, semantic)

	if r.StructuralAccuracy != 100 {
		t.Errorf("StructuralAccuracy = %v, want 100", r.StructuralAccuracy)
	}
	if r.ContentAccuracy != 100 {
		t.Errorf("ContentAccuracy = %v, want 100", r.ContentAccuracy)
	}
	if r.SchemaFidelity != 100 {
		t.Errorf("SchemaFidelity = %v, want 100", r.SchemaFidelity)
	}
	if r.SemanticAccuracy != 100 {
		t.Errorf("SemanticAccuracy = %v, want 100", r.SemanticAccuracy)
	}
	if r.OverallAccuracy != 100 {
		t.Errorf("OverallAccuracy = %v, want 100", r.OverallAccuracy)
	}
	if r.ContentTablesTotal != 2 {
		t.Errorf("ContentTablesTotal = %v, want 2", r.ContentTablesTotal)
	}
	if r.ContentTablesMatch != 2 {
		t.Errorf("ContentTablesMatch = %v, want 2", r.ContentTablesMatch)
	}
	if r.TotalRowsHashed != 300 {
		t.Errorf("TotalRowsHashed = %v, want 300", r.TotalRowsHashed)
	}
}

func TestBuildValidationReport_ContentMismatch(t *testing.T) {
	structural := []validation.StructuralResult{
		{Table: "users", SourceRows: 100, DestRows: 100, Match: true},
	}
	content := []validation.ContentResult{
		{Table: "users", TotalRows: 100, SourceChecksum: "abc", DestChecksum: "xyz", Match: false,
			DiffCount: 5, MissingInDest: 2, ExtraInDest: 1, Modified: 2},
	}

	r := buildValidationReport("job1", "run1", structural, content, nil, nil)

	if r.ContentAccuracy != 0 {
		t.Errorf("ContentAccuracy = %v, want 0", r.ContentAccuracy)
	}
	if r.ContentTablesDiff != 1 {
		t.Errorf("ContentTablesDiff = %v, want 1", r.ContentTablesDiff)
	}
	if r.TotalDiffRows != 5 {
		t.Errorf("TotalDiffRows = %v, want 5", r.TotalDiffRows)
	}
	if r.OverallAccuracy >= 100 {
		t.Errorf("OverallAccuracy = %v, want < 100", r.OverallAccuracy)
	}
	// Verify per-table breakdown on proto output.
	if r.Content[0].MissingInDestCount != 2 {
		t.Errorf("MissingInDestCount = %v, want 2", r.Content[0].MissingInDestCount)
	}
	if r.Content[0].ExtraInDestCount != 1 {
		t.Errorf("ExtraInDestCount = %v, want 1", r.Content[0].ExtraInDestCount)
	}
	if r.Content[0].ModifiedCount != 2 {
		t.Errorf("ModifiedCount = %v, want 2", r.Content[0].ModifiedCount)
	}
}

func TestBuildValidationReport_SchemaMismatch(t *testing.T) {
	structural := []validation.StructuralResult{
		{Table: "users", SourceRows: 100, DestRows: 100, Match: true},
	}
	schemas := []validation.SchemaResult{
		{Table: "users", Match: false, Diffs: []validation.SchemaDiff{
			{ColumnName: "age", DiffType: "type_changed", SourceValue: "int", DestValue: "bigint"},
		}},
	}

	r := buildValidationReport("job1", "run1", structural, nil, schemas, nil)

	if r.SchemaFidelity != 0 {
		t.Errorf("SchemaFidelity = %v, want 0", r.SchemaFidelity)
	}
	expected := (validation.WeightStructural * 100) / (validation.WeightStructural + validation.WeightSchema)
	if r.OverallAccuracy < expected-0.01 || r.OverallAccuracy > expected+0.01 {
		t.Errorf("OverallAccuracy = %v, want ~%v", r.OverallAccuracy, expected)
	}
}

func TestBuildValidationReport_OnlyStructural(t *testing.T) {
	structural := []validation.StructuralResult{
		{Table: "users", SourceRows: 50, DestRows: 50, Match: true},
		{Table: "orders", SourceRows: 10, DestRows: 8, Match: false},
	}

	r := buildValidationReport("job1", "run1", structural, nil, nil, nil)

	if r.StructuralAccuracy != 50 {
		t.Errorf("StructuralAccuracy = %v, want 50", r.StructuralAccuracy)
	}
	if r.OverallAccuracy != 50 {
		t.Errorf("OverallAccuracy = %v, want 50 (should equal structural when only layer)", r.OverallAccuracy)
	}
}

// TestContentValidation_DiffBreakdown verifies diff counts propagate through report builder.
func TestContentValidation_DiffBreakdown(t *testing.T) {
	content := []validation.ContentResult{
		{Table: "users", TotalRows: 1000, SourceChecksum: "a", DestChecksum: "b", Match: false,
			DiffCount: 7, MissingInDest: 3, ExtraInDest: 1, Modified: 3},
		{Table: "orders", TotalRows: 500, SourceChecksum: "x", DestChecksum: "x", Match: true},
	}

	r := buildValidationReport("job1", "run1", nil, content, nil, nil)

	if r.TotalDiffRows != 7 {
		t.Errorf("TotalDiffRows = %v, want 7", r.TotalDiffRows)
	}
	if r.ContentTablesDiff != 1 {
		t.Errorf("ContentTablesDiff = %v, want 1", r.ContentTablesDiff)
	}
	if r.ContentTablesMatch != 1 {
		t.Errorf("ContentTablesMatch = %v, want 1", r.ContentTablesMatch)
	}
	if r.Content[0].MissingInDestCount != 3 {
		t.Errorf("users MissingInDestCount = %v, want 3", r.Content[0].MissingInDestCount)
	}
	if r.Content[0].ExtraInDestCount != 1 {
		t.Errorf("users ExtraInDestCount = %v, want 1", r.Content[0].ExtraInDestCount)
	}
	if r.Content[0].ModifiedCount != 3 {
		t.Errorf("users ModifiedCount = %v, want 3", r.Content[0].ModifiedCount)
	}
}

// ---------------------------------------------------------------------------
// Proto converter tests
// ---------------------------------------------------------------------------

func TestToSharedSemanticRules(t *testing.T) {
	rules := toSharedSemanticRules(nil)
	if len(rules) != 0 {
		t.Errorf("nil input should produce empty slice, got %d", len(rules))
	}
}
