package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/zwiron/connector"
	agentv1 "github.com/zwiron/proto/gen/go/agent/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// runOnDemandValidation executes the four-layer validation orchestrator.
// It is triggered by the TriggerValidation command from Atlas.
func (a *Agent) runOnDemandValidation(ctx context.Context, jobID, runID string, cmd *agentv1.TriggerValidation) {
	tracer := otel.Tracer("zwiron.agent")
	ctx, span := tracer.Start(ctx, "agent.on_demand_validation",
		trace.WithAttributes(
			attribute.String("job_id", jobID),
			attribute.String("run_id", runID),
			attribute.Int("table_count", len(cmd.GetTables())),
		),
	)
	defer span.End()

	start := time.Now()
	tables := cmd.GetTables()

	a.log.Info(ctx, "agent.validation.start",
		"job_id", jobID,
		"run_id", runID,
		"tables", tables,
	)

	a.sendValidationProgress(jobID, runID, "running", 0, int32(len(tables)))

	// Decrypt source config.
	srcConfig, err := DecryptConfig(a.keys.Private, cmd.GetSource().GetEncryptedConfig())
	if err != nil {
		a.sendValidationError(jobID, runID, fmt.Errorf("decrypt source config: %w", err))
		return
	}

	// Decrypt destination config.
	dstConfig, err := DecryptConfig(a.keys.Private, cmd.GetDest().GetEncryptedConfig())
	if err != nil {
		a.sendValidationError(jobID, runID, fmt.Errorf("decrypt dest config: %w", err))
		return
	}

	srcType := connector.ConnectorType(cmd.GetSource().GetConnectorType())
	dstType := connector.ConnectorType(cmd.GetDest().GetConnectorType())

	// Open source connection.
	src, err := connector.GetSource(srcType)
	if err != nil {
		a.sendValidationError(jobID, runID, fmt.Errorf("source registry: %w", err))
		return
	}
	srcConn, ok := src.(connector.Connection)
	if !ok {
		a.sendValidationError(jobID, runID, fmt.Errorf("source connector does not implement Connection"))
		return
	}
	if err := srcConn.Connect(ctx, srcConfig); err != nil {
		a.sendValidationError(jobID, runID, fmt.Errorf("source connect: %w", err))
		return
	}
	defer srcConn.Close(ctx)

	// Open destination connection.
	dst, err := connector.GetDestination(dstType)
	if err != nil {
		a.sendValidationError(jobID, runID, fmt.Errorf("dest registry: %w", err))
		return
	}
	dstConn, ok := dst.(connector.Connection)
	if !ok {
		a.sendValidationError(jobID, runID, fmt.Errorf("dest connector does not implement Connection"))
		return
	}
	if err := dstConn.Connect(ctx, dstConfig); err != nil {
		a.sendValidationError(jobID, runID, fmt.Errorf("dest connect: %w", err))
		return
	}
	defer dstConn.Close(ctx)

	// Layer 1: Structural validation (row counts).
	structural := a.validateStructural(ctx, jobID, src, dst, tables)
	a.sendValidationProgress(jobID, runID, "structural_done", 20, int32(len(tables)))

	// Layer 2: Content validation (full-table checksums + row diffs).
	content, diffs := a.validateContentFull(ctx, jobID, src, dst, tables)
	a.sendValidationProgress(jobID, runID, "content_done", 50, int32(len(tables)))

	// Layer 3: Schema comparison.
	schemas := a.validateSchema(ctx, jobID, src, dst, tables)
	a.sendValidationProgress(jobID, runID, "schema_done", 65, int32(len(tables)))

	// Layer 4: Semantic validation (user-defined assertions).
	semantic := a.validateSemantic(ctx, jobID, src, dst, tables, cmd.GetSemanticRules())
	a.sendValidationProgress(jobID, runID, "semantic_done", 80, int32(len(tables)))

	// Data profiling (bonus layer attached to the report).
	profiles := a.profileData(ctx, jobID, src, dst, tables)
	a.sendValidationProgress(jobID, runID, "profiling_done", 90, int32(len(tables)))

	// Build and send report.
	report := buildValidationReport(jobID, runID, structural, content, schemas, semantic, diffs, profiles)
	report.DurationMs = time.Since(start).Milliseconds()
	report.ValidatedAt = timestamppb.Now()

	a.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_ValidationReport{
			ValidationReport: report,
		},
	})

	a.sendValidationProgress(jobID, runID, "completed", 100, int32(len(tables)))

	a.log.Info(ctx, "agent.validation.done",
		"job_id", jobID,
		"run_id", runID,
		"duration_ms", report.DurationMs,
		"overall_accuracy", report.OverallAccuracy,
	)
}

// ---------------------------------------------------------------------------
// Layer 1: Structural (row counts)
// ---------------------------------------------------------------------------

func (a *Agent) validateStructural(ctx context.Context, jobID string, src connector.Source, dst interface{}, tables []string) []*agentv1.TableValidation {
	srcCounter, srcOK := src.(connector.RowCounter)
	dstCounter, dstOK := dst.(connector.RowCounter)

	var results []*agentv1.TableValidation
	for _, tbl := range tables {
		tv := &agentv1.TableValidation{TableName: tbl}

		if !srcOK {
			tv.Error = "source does not support row counting"
			results = append(results, tv)
			continue
		}
		if !dstOK {
			tv.Error = "destination does not support row counting"
			results = append(results, tv)
			continue
		}

		srcRows, err := srcCounter.RowCount(ctx, tbl)
		if err != nil {
			tv.Error = fmt.Sprintf("source count: %v", err)
			results = append(results, tv)
			continue
		}
		tv.SourceRows = srcRows

		dstRows, err := dstCounter.RowCount(ctx, tbl)
		if err != nil {
			tv.Error = fmt.Sprintf("dest count: %v", err)
			results = append(results, tv)
			continue
		}
		tv.DestRows = dstRows
		tv.Match = srcRows == dstRows

		a.log.Info(ctx, "agent.validation.structural",
			"job_id", jobID,
			"table", tbl,
			"source_rows", srcRows,
			"dest_rows", dstRows,
			"match", tv.Match,
		)

		results = append(results, tv)
	}
	return results
}

// ---------------------------------------------------------------------------
// Layer 2: Content (checksums + row diffs)
// ---------------------------------------------------------------------------

func (a *Agent) validateContentFull(ctx context.Context, jobID string, src connector.Source, dst interface{}, tables []string) ([]*agentv1.ContentValidation, []*agentv1.RowDiff) {
	srcHasher, srcOK := src.(connector.DataHasher)
	dstHasher, dstOK := dst.(connector.DataHasher)

	var content []*agentv1.ContentValidation
	var allDiffs []*agentv1.RowDiff

	for _, tbl := range tables {
		cv := &agentv1.ContentValidation{TableName: tbl}

		if !srcOK || !dstOK {
			cv.Error = "connector does not support data hashing"
			content = append(content, cv)
			continue
		}

		srcChecksum, srcRows, err := srcHasher.HashTable(ctx, tbl)
		if err != nil {
			cv.Error = fmt.Sprintf("source hash: %v", err)
			content = append(content, cv)
			continue
		}
		cv.SourceChecksum = srcChecksum
		cv.TotalRows = srcRows

		dstChecksum, _, err := dstHasher.HashTable(ctx, tbl)
		if err != nil {
			cv.Error = fmt.Sprintf("dest hash: %v", err)
			content = append(content, cv)
			continue
		}
		cv.DestChecksum = dstChecksum
		cv.Match = srcChecksum == dstChecksum

		if !cv.Match {
			tableDiffs := a.findRowDiffs(ctx, jobID, tbl, src, dst)
			cv.DiffRowCount = int64(len(tableDiffs))
			allDiffs = append(allDiffs, tableDiffs...)
		}

		a.log.Info(ctx, "agent.validation.content",
			"job_id", jobID,
			"table", tbl,
			"match", cv.Match,
			"diff_rows", cv.DiffRowCount,
		)

		content = append(content, cv)
	}
	return content, allDiffs
}

func (a *Agent) findRowDiffs(ctx context.Context, jobID, table string, src connector.Source, dst interface{}) []*agentv1.RowDiff {
	srcDiffer, srcOK := src.(connector.RowDiffer)
	dstDiffer, dstOK := dst.(connector.RowDiffer)
	if !srcOK || !dstOK {
		a.log.Warn(ctx, "agent.validation.row_diff_unsupported", "job_id", jobID, "table", table)
		return nil
	}

	srcRows, err := srcDiffer.HashRows(ctx, table)
	if err != nil {
		a.log.Error(ctx, "agent.validation.src_hash_rows", "job_id", jobID, "table", table, "error", err)
		return nil
	}

	dstRows, err := dstDiffer.HashRows(ctx, table)
	if err != nil {
		a.log.Error(ctx, "agent.validation.dst_hash_rows", "job_id", jobID, "table", table, "error", err)
		return nil
	}

	var diffs []*agentv1.RowDiff

	// Rows in source but not in dest, or modified.
	for pk, srcHash := range srcRows {
		dstHash, exists := dstRows[pk]
		if !exists {
			diffs = append(diffs, &agentv1.RowDiff{
				TableName:  table,
				PrimaryKey: pk,
				DiffType:   "missing_in_dest",
				SourceHash: srcHash,
			})
		} else if srcHash != dstHash {
			diffs = append(diffs, &agentv1.RowDiff{
				TableName:  table,
				PrimaryKey: pk,
				DiffType:   "modified",
				SourceHash: srcHash,
				DestHash:   dstHash,
			})
		}
	}

	// Rows in dest but not in source.
	for pk, dstHash := range dstRows {
		if _, exists := srcRows[pk]; !exists {
			diffs = append(diffs, &agentv1.RowDiff{
				TableName:  table,
				PrimaryKey: pk,
				DiffType:   "extra_in_dest",
				DestHash:   dstHash,
			})
		}
	}

	return diffs
}

// ---------------------------------------------------------------------------
// Layer 3: Schema comparison
// ---------------------------------------------------------------------------

func (a *Agent) validateSchema(ctx context.Context, jobID string, src connector.Source, dst interface{}, tables []string) []*agentv1.SchemaComparison {
	srcComparer, srcOK := src.(connector.SchemaComparer)
	dstComparer, dstOK := dst.(connector.SchemaComparer)

	var results []*agentv1.SchemaComparison
	for _, tbl := range tables {
		sc := &agentv1.SchemaComparison{TableName: tbl}

		if !srcOK || !dstOK {
			sc.Error = "connector does not support schema comparison"
			results = append(results, sc)
			continue
		}

		srcJSON, err := srcComparer.GetTableSchema(ctx, tbl)
		if err != nil {
			sc.Error = fmt.Sprintf("source schema: %v", err)
			results = append(results, sc)
			continue
		}
		sc.SourceSchema = string(srcJSON)

		dstJSON, err := dstComparer.GetTableSchema(ctx, tbl)
		if err != nil {
			sc.Error = fmt.Sprintf("dest schema: %v", err)
			results = append(results, sc)
			continue
		}
		sc.DestSchema = string(dstJSON)

		sc.Differences = compareSchemas(srcJSON, dstJSON)
		sc.Match = len(sc.Differences) == 0

		a.log.Info(ctx, "agent.validation.schema",
			"job_id", jobID,
			"table", tbl,
			"match", sc.Match,
			"diffs", len(sc.Differences),
		)

		results = append(results, sc)
	}
	return results
}

// schemaColumn represents one column in the JSON schema returned by GetTableSchema.
type schemaColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	Default  string `json:"default"`
}

// compareSchemas compares two JSON-encoded column arrays and returns diffs.
func compareSchemas(srcJSON, dstJSON []byte) []*agentv1.SchemaDiff {
	if bytes.Equal(srcJSON, dstJSON) {
		return nil
	}

	var srcCols, dstCols []schemaColumn
	if err := json.Unmarshal(srcJSON, &srcCols); err != nil {
		return []*agentv1.SchemaDiff{{ColumnName: "_parse_error", DiffType: "parse_error", SourceValue: err.Error()}}
	}
	if err := json.Unmarshal(dstJSON, &dstCols); err != nil {
		return []*agentv1.SchemaDiff{{ColumnName: "_parse_error", DiffType: "parse_error", DestValue: err.Error()}}
	}

	srcMap := make(map[string]schemaColumn, len(srcCols))
	for _, c := range srcCols {
		srcMap[c.Name] = c
	}
	dstMap := make(map[string]schemaColumn, len(dstCols))
	for _, c := range dstCols {
		dstMap[c.Name] = c
	}

	var diffs []*agentv1.SchemaDiff

	for _, sc := range srcCols {
		dc, exists := dstMap[sc.Name]
		if !exists {
			diffs = append(diffs, &agentv1.SchemaDiff{
				ColumnName:  sc.Name,
				DiffType:    "missing_in_dest",
				SourceValue: sc.Type,
			})
			continue
		}
		if sc.Type != dc.Type {
			diffs = append(diffs, &agentv1.SchemaDiff{
				ColumnName:  sc.Name,
				DiffType:    "type_changed",
				SourceValue: sc.Type,
				DestValue:   dc.Type,
			})
		}
		if sc.Nullable != dc.Nullable {
			diffs = append(diffs, &agentv1.SchemaDiff{
				ColumnName:  sc.Name,
				DiffType:    "nullable_changed",
				SourceValue: fmt.Sprintf("%v", sc.Nullable),
				DestValue:   fmt.Sprintf("%v", dc.Nullable),
			})
		}
	}

	for _, dc := range dstCols {
		if _, exists := srcMap[dc.Name]; !exists {
			diffs = append(diffs, &agentv1.SchemaDiff{
				ColumnName: dc.Name,
				DiffType:   "extra_in_dest",
				DestValue:  dc.Type,
			})
		}
	}

	return diffs
}

// ---------------------------------------------------------------------------
// Layer 4: Semantic validation (user-defined assertions)
// ---------------------------------------------------------------------------

func (a *Agent) validateSemantic(ctx context.Context, jobID string, src connector.Source, dst interface{}, tables []string, rules []*agentv1.SemanticRule) []*agentv1.SemanticValidation {
	if len(rules) == 0 {
		return nil
	}

	srcQuerier, srcOK := src.(connector.RawQuerier)
	dstQuerier, dstOK := dst.(connector.RawQuerier)

	var results []*agentv1.SemanticValidation
	for _, rule := range rules {
		sv := &agentv1.SemanticValidation{
			RuleName:   rule.GetName(),
			RuleType:   rule.GetRuleType(),
			TableName:  rule.GetTableName(),
			Expression: rule.GetExpression(),
		}

		if !srcOK || !dstOK {
			sv.Error = "connector does not support raw queries"
			results = append(results, sv)
			continue
		}

		srcVal, err := srcQuerier.QueryScalar(ctx, rule.GetExpression())
		if err != nil {
			sv.Error = fmt.Sprintf("source query: %v", err)
			results = append(results, sv)
			continue
		}
		sv.Expected = fmt.Sprintf("%v", srcVal)

		dstVal, err := dstQuerier.QueryScalar(ctx, rule.GetExpression())
		if err != nil {
			sv.Error = fmt.Sprintf("dest query: %v", err)
			results = append(results, sv)
			continue
		}
		sv.Actual = fmt.Sprintf("%v", dstVal)

		sv.Passed = sv.Expected == sv.Actual

		a.log.Info(ctx, "agent.validation.semantic",
			"job_id", jobID,
			"rule", rule.GetName(),
			"passed", sv.Passed,
		)

		results = append(results, sv)
	}
	return results
}

// ---------------------------------------------------------------------------
// Data profiling
// ---------------------------------------------------------------------------

func (a *Agent) profileData(ctx context.Context, jobID string, src connector.Source, dst interface{}, tables []string) []*agentv1.DataProfile {
	srcProfiler, srcOK := src.(connector.DataProfiler)
	dstProfiler, dstOK := dst.(connector.DataProfiler)

	var results []*agentv1.DataProfile
	for _, tbl := range tables {
		dp := &agentv1.DataProfile{TableName: tbl}

		if !srcOK || !dstOK {
			dp.Error = "connector does not support data profiling"
			results = append(results, dp)
			continue
		}

		srcJSON, srcDups, err := srcProfiler.ProfileTable(ctx, tbl)
		if err != nil {
			dp.Error = fmt.Sprintf("source profile: %v", err)
			results = append(results, dp)
			continue
		}
		dp.SourceProfile = string(srcJSON)
		dp.SourceDuplicates = srcDups

		dstJSON, dstDups, err := dstProfiler.ProfileTable(ctx, tbl)
		if err != nil {
			dp.Error = fmt.Sprintf("dest profile: %v", err)
			results = append(results, dp)
			continue
		}
		dp.DestProfile = string(dstJSON)
		dp.DestDuplicates = dstDups

		results = append(results, dp)
	}
	return results
}

// ---------------------------------------------------------------------------
// Report builder + accuracy computation
// ---------------------------------------------------------------------------

// buildValidationReport assembles the final report and computes accuracy scores.
func buildValidationReport(
	jobID, runID string,
	structural []*agentv1.TableValidation,
	content []*agentv1.ContentValidation,
	schemas []*agentv1.SchemaComparison,
	semantic []*agentv1.SemanticValidation,
	diffs []*agentv1.RowDiff,
	profiles []*agentv1.DataProfile,
) *agentv1.ValidationReport {
	r := &agentv1.ValidationReport{
		JobId:      jobID,
		RunId:      runID,
		Structural: structural,
		Content:    content,
		Schemas:    schemas,
		Semantic:   semantic,
		RowDiffs:   diffs,
		Profiles:   profiles,
	}

	// Structural accuracy: % of tables where row counts match.
	if len(structural) > 0 {
		matched := 0
		valid := 0
		for _, tv := range structural {
			if tv.Error != "" {
				continue
			}
			valid++
			if tv.Match {
				matched++
			}
		}
		if valid > 0 {
			r.StructuralAccuracy = float64(matched) / float64(valid) * 100
		}
	}

	// Content accuracy: % of tables with matching checksums.
	if len(content) > 0 {
		var totalTables, matchTables, diffTables int32
		var totalRows, totalDiff int64
		for _, cv := range content {
			if cv.Error != "" {
				continue
			}
			totalTables++
			totalRows += cv.TotalRows
			if cv.Match {
				matchTables++
			} else {
				diffTables++
				totalDiff += cv.DiffRowCount
			}
		}
		r.ContentTablesTotal = totalTables
		r.ContentTablesMatch = matchTables
		r.ContentTablesDiff = diffTables
		r.TotalRowsHashed = totalRows
		r.TotalDiffRows = totalDiff
		if totalTables > 0 {
			r.ContentAccuracy = float64(matchTables) / float64(totalTables) * 100
		}
	}

	// Schema fidelity: % of tables with matching schemas.
	if len(schemas) > 0 {
		matched := 0
		valid := 0
		for _, sc := range schemas {
			if sc.Error != "" {
				continue
			}
			valid++
			if sc.Match {
				matched++
			}
		}
		if valid > 0 {
			r.SchemaFidelity = float64(matched) / float64(valid) * 100
		}
	}

	// Semantic accuracy: % of rules that passed.
	if len(semantic) > 0 {
		passed := 0
		valid := 0
		for _, sv := range semantic {
			if sv.Error != "" {
				continue
			}
			valid++
			if sv.Passed {
				passed++
			}
		}
		if valid > 0 {
			r.SemanticAccuracy = float64(passed) / float64(valid) * 100
		}
	}

	r.OverallAccuracy = computeOverallAccuracy(r)
	return r
}

// Accuracy weights for the overall score.
const (
	weightStructural = 0.30
	weightContent    = 0.35
	weightSchema     = 0.15
	weightSemantic   = 0.20
)

// computeOverallAccuracy calculates a weighted average of layer accuracies.
// Layers that were not executed (empty slices) are excluded from the denominator.
func computeOverallAccuracy(r *agentv1.ValidationReport) float64 {
	var totalWeight, weightedSum float64

	if len(r.Structural) > 0 {
		totalWeight += weightStructural
		weightedSum += weightStructural * r.StructuralAccuracy
	}
	if len(r.Content) > 0 {
		totalWeight += weightContent
		weightedSum += weightContent * r.ContentAccuracy
	}
	if len(r.Schemas) > 0 {
		totalWeight += weightSchema
		weightedSum += weightSchema * r.SchemaFidelity
	}
	if len(r.Semantic) > 0 {
		totalWeight += weightSemantic
		weightedSum += weightSemantic * r.SemanticAccuracy
	}

	if totalWeight == 0 {
		return 0
	}
	return weightedSum / totalWeight
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (a *Agent) sendValidationProgress(jobID, runID, phase string, pct int, tableCount int32) {
	a.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_ValidationProgress{
			ValidationProgress: &agentv1.ValidationProgress{
				JobId:       jobID,
				RunId:       runID,
				Phase:       phase,
				PercentDone: int32(pct),
				TableCount:  tableCount,
			},
		},
	})
}

func (a *Agent) sendValidationError(jobID, runID string, err error) {
	a.log.Error(nil, "agent.validation.error", "job_id", jobID, "run_id", runID, "error", err)
	a.sendEvent(&agentv1.ConnectRequest{
		Payload: &agentv1.ConnectRequest_ValidationError{
			ValidationError: &agentv1.ValidationError{
				JobId: jobID,
				RunId: runID,
				Error: err.Error(),
			},
		},
	})
}

func countDiffType(diffs []*agentv1.RowDiff, diffType string) int {
	n := 0
	for _, d := range diffs {
		if d.DiffType == diffType {
			n++
		}
	}
	return n
}
