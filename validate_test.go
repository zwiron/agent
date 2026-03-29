package main

import (
	"testing"

	agentv1 "github.com/zwiron/proto/gen/go/agent/v1"
)

func TestBuildValidationReport_AllMatch(t *testing.T) {
	structural := []*agentv1.TableValidation{
		{TableName: "users", SourceRows: 100, DestRows: 100, Match: true},
		{TableName: "orders", SourceRows: 200, DestRows: 200, Match: true},
	}
	content := []*agentv1.ContentValidation{
		{TableName: "users", TotalRows: 100, SourceChecksum: "abc", DestChecksum: "abc", Match: true},
		{TableName: "orders", TotalRows: 200, SourceChecksum: "def", DestChecksum: "def", Match: true},
	}
	schemas := []*agentv1.SchemaComparison{
		{TableName: "users", Match: true},
		{TableName: "orders", Match: true},
	}
	semantic := []*agentv1.SemanticValidation{
		{RuleName: "sum_check", Passed: true},
	}

	r := buildValidationReport("job1", "run1", structural, content, schemas, semantic, nil, nil)

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
	structural := []*agentv1.TableValidation{
		{TableName: "users", SourceRows: 100, DestRows: 100, Match: true},
	}
	content := []*agentv1.ContentValidation{
		{TableName: "users", TotalRows: 100, SourceChecksum: "abc", DestChecksum: "xyz", Match: false, DiffRowCount: 5},
	}
	diffs := []*agentv1.RowDiff{
		{TableName: "users", PrimaryKey: "1", DiffType: "modified"},
		{TableName: "users", PrimaryKey: "2", DiffType: "missing_in_dest"},
	}

	r := buildValidationReport("job1", "run1", structural, content, nil, nil, diffs, nil)

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
	if len(r.RowDiffs) != 2 {
		t.Errorf("RowDiffs len = %v, want 2", len(r.RowDiffs))
	}
}

func TestBuildValidationReport_SchemaMismatch(t *testing.T) {
	structural := []*agentv1.TableValidation{
		{TableName: "users", SourceRows: 100, DestRows: 100, Match: true},
	}
	schemas := []*agentv1.SchemaComparison{
		{TableName: "users", Match: false, Differences: []*agentv1.SchemaDiff{
			{ColumnName: "age", DiffType: "type_changed", SourceValue: "int", DestValue: "bigint"},
		}},
	}

	r := buildValidationReport("job1", "run1", structural, nil, schemas, nil, nil, nil)

	if r.SchemaFidelity != 0 {
		t.Errorf("SchemaFidelity = %v, want 0", r.SchemaFidelity)
	}
	// Only structural (0.30) and schema (0.15) active.
	// structural=100, schema=0 → weighted = (0.30*100 + 0.15*0) / (0.30+0.15) = 30/0.45 = 66.67
	expected := (weightStructural * 100) / (weightStructural + weightSchema)
	if r.OverallAccuracy < expected-0.01 || r.OverallAccuracy > expected+0.01 {
		t.Errorf("OverallAccuracy = %v, want ~%v", r.OverallAccuracy, expected)
	}
}

func TestBuildValidationReport_OnlyStructural(t *testing.T) {
	structural := []*agentv1.TableValidation{
		{TableName: "users", SourceRows: 50, DestRows: 50, Match: true},
		{TableName: "orders", SourceRows: 10, DestRows: 8, Match: false},
	}

	r := buildValidationReport("job1", "run1", structural, nil, nil, nil, nil, nil)

	// 1 of 2 match = 50%
	if r.StructuralAccuracy != 50 {
		t.Errorf("StructuralAccuracy = %v, want 50", r.StructuralAccuracy)
	}
	// Only structural active → overall = structural accuracy
	if r.OverallAccuracy != 50 {
		t.Errorf("OverallAccuracy = %v, want 50 (should equal structural when only layer)", r.OverallAccuracy)
	}
}

func TestComputeOverallAccuracy_NoLayers(t *testing.T) {
	r := &agentv1.ValidationReport{}
	if got := computeOverallAccuracy(r); got != 0 {
		t.Errorf("computeOverallAccuracy(empty) = %v, want 0", got)
	}
}

func TestCompareSchemas_Identical(t *testing.T) {
	src := []byte(`[{"name":"id","type":"int","nullable":false},{"name":"name","type":"text","nullable":true}]`)
	diffs := compareSchemas(src, src)
	if len(diffs) != 0 {
		t.Errorf("identical schemas should have 0 diffs, got %d", len(diffs))
	}
}

func TestCompareSchemas_TypeChanged(t *testing.T) {
	src := []byte(`[{"name":"id","type":"int","nullable":false}]`)
	dst := []byte(`[{"name":"id","type":"bigint","nullable":false}]`)
	diffs := compareSchemas(src, dst)
	if len(diffs) != 1 {
		t.Fatalf("expected 1 diff, got %d", len(diffs))
	}
	if diffs[0].DiffType != "type_changed" {
		t.Errorf("DiffType = %q, want type_changed", diffs[0].DiffType)
	}
	if diffs[0].SourceValue != "int" || diffs[0].DestValue != "bigint" {
		t.Errorf("values = %q/%q, want int/bigint", diffs[0].SourceValue, diffs[0].DestValue)
	}
}

func TestCompareSchemas_MissingAndExtra(t *testing.T) {
	src := []byte(`[{"name":"id","type":"int","nullable":false},{"name":"deleted_at","type":"timestamp","nullable":true}]`)
	dst := []byte(`[{"name":"id","type":"int","nullable":false},{"name":"extra_col","type":"text","nullable":true}]`)
	diffs := compareSchemas(src, dst)

	var missing, extra int
	for _, d := range diffs {
		switch d.DiffType {
		case "missing_in_dest":
			missing++
		case "extra_in_dest":
			extra++
		}
	}
	if missing != 1 {
		t.Errorf("missing_in_dest count = %d, want 1", missing)
	}
	if extra != 1 {
		t.Errorf("extra_in_dest count = %d, want 1", extra)
	}
}

func TestCountDiffType(t *testing.T) {
	diffs := []*agentv1.RowDiff{
		{DiffType: "modified"},
		{DiffType: "missing_in_dest"},
		{DiffType: "modified"},
		{DiffType: "extra_in_dest"},
	}
	if got := countDiffType(diffs, "modified"); got != 2 {
		t.Errorf("countDiffType(modified) = %d, want 2", got)
	}
	if got := countDiffType(diffs, "missing_in_dest"); got != 1 {
		t.Errorf("countDiffType(missing_in_dest) = %d, want 1", got)
	}
	if got := countDiffType(nil, "modified"); got != 0 {
		t.Errorf("countDiffType(nil) = %d, want 0", got)
	}
}
