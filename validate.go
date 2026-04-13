package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/zwiron/connector"
	"github.com/zwiron/engine/validation"
	agentv1 "github.com/zwiron/proto/gen/go/agent/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// defaultMaxDiffRows is no longer used — we count diffs but never store row data.
// const defaultMaxDiffRows = 10_000

// validationRuns tracks running validation contexts for cancellation.
var validationRuns sync.Map // map[string]context.CancelFunc keyed by "jobID:runID"

// cancelValidation cancels a running validation identified by jobID + runID.
func (a *Agent) cancelValidation(ctx context.Context, cmd *agentv1.CancelValidation) {
	key := cmd.GetJobId() + ":" + cmd.GetRunId()
	if cancel, ok := validationRuns.LoadAndDelete(key); ok {
		cancel.(context.CancelFunc)()
		a.log.Info(ctx, "agent.validation.cancelled", "job_id", cmd.GetJobId(), "run_id", cmd.GetRunId(), "reason", cmd.GetReason())
	}
}

// runOnDemandValidation executes the four-layer validation orchestrator.
// It is triggered by the TriggerValidation command from Atlas.
func (a *Agent) runOnDemandValidation(ctx context.Context, jobID, runID string, cmd *agentv1.TriggerValidation) {
	tracer := otel.Tracer("zwiron.agent")

	// Apply timeout if specified.
	if timeout := cmd.GetTimeoutSeconds(); timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
		defer cancel()
	}

	// Register for cancellation.
	cancelCtx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()
	key := jobID + ":" + runID
	validationRuns.Store(key, cancelFn)
	defer validationRuns.Delete(key)
	ctx = cancelCtx

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
	transformSpec := protoToTransformSpec(cmd.GetTransforms())
	hasTransforms := transformSpec != nil && !transformSpec.IsEmpty()

	a.log.Info(ctx, "agent.validation.start",
		"job_id", jobID,
		"run_id", runID,
		"tables", tables,
		"transform_aware", hasTransforms,
		"validation_mode", cmd.GetValidationMode(),
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

	// Check for cancellation before starting layers.
	if ctx.Err() != nil {
		a.sendValidationError(jobID, runID, fmt.Errorf("validation cancelled before start: %w", ctx.Err()))
		return
	}

	// Layer 1: Structural validation (row counts — transform-aware).
	structuralRes := validation.ValidateStructural(ctx, src, dst, tables, transformSpec)
	if ctx.Err() != nil {
		a.sendValidationError(jobID, runID, fmt.Errorf("cancelled during structural: %w", ctx.Err()))
		return
	}
	for _, r := range structuralRes {
		a.log.Info(ctx, "agent.validation.structural",
			"job_id", jobID, "table", r.Table,
			"source_rows", r.SourceRows, "dest_rows", r.DestRows,
			"match", r.Match, "has_filters", r.HasFilters,
		)
	}
	a.sendValidationProgress(jobID, runID, "structural_done", 20, int32(len(tables)))

	// Layer 2: Content validation (checksums + diff COUNTS only — no row data leaves).
	contentRes := validation.ValidateContent(ctx, src, dst, tables)
	if ctx.Err() != nil {
		a.sendValidationError(jobID, runID, fmt.Errorf("cancelled during content: %w", ctx.Err()))
		return
	}
	for _, r := range contentRes {
		a.log.Info(ctx, "agent.validation.content",
			"job_id", jobID, "table", r.Table,
			"match", r.Match, "diff_rows", r.DiffCount,
		)
	}
	a.sendValidationProgress(jobID, runID, "content_done", 50, int32(len(tables)))

	// Layer 3: Schema comparison (transform-aware — accounts for renames, drops, casts, computed).
	schemaRes := validation.ValidateSchema(ctx, src, dst, tables, transformSpec, cmd.GetDestSchema())
	if ctx.Err() != nil {
		a.sendValidationError(jobID, runID, fmt.Errorf("cancelled during schema: %w", ctx.Err()))
		return
	}
	for _, r := range schemaRes {
		a.log.Info(ctx, "agent.validation.schema",
			"job_id", jobID, "table", r.Table,
			"match", r.Match, "diffs", len(r.Diffs),
		)
	}
	a.sendValidationProgress(jobID, runID, "schema_done", 65, int32(len(tables)))

	// Layer 4: Semantic validation (user-defined assertions).
	semanticRules := toSharedSemanticRules(cmd.GetSemanticRules())
	semanticRes := validation.ValidateSemantic(ctx, src, dst, semanticRules)
	if ctx.Err() != nil {
		a.sendValidationError(jobID, runID, fmt.Errorf("cancelled during semantic: %w", ctx.Err()))
		return
	}
	for _, r := range semanticRes {
		a.log.Info(ctx, "agent.validation.semantic",
			"job_id", jobID, "rule", r.RuleName, "passed", r.Passed,
		)
	}
	a.sendValidationProgress(jobID, runID, "semantic_done", 90, int32(len(tables)))

	// Build and send report.
	report := buildValidationReport(jobID, runID, structuralRes, contentRes, schemaRes, semanticRes)
	report.DurationMs = time.Since(start).Milliseconds()
	report.ValidatedAt = timestamppb.Now()
	report.ValidationMode = cmd.GetValidationMode()
	report.TransformAware = hasTransforms

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
		"transform_aware", hasTransforms,
	)
}

// ---------------------------------------------------------------------------
// Proto converters — shared results → proto types
// ---------------------------------------------------------------------------

// toSharedSemanticRules converts proto SemanticRule → shared SemanticRule.
func toSharedSemanticRules(rules []*agentv1.SemanticRule) []validation.SemanticRule {
	out := make([]validation.SemanticRule, 0, len(rules))
	for _, r := range rules {
		out = append(out, validation.SemanticRule{
			Name:       r.GetName(),
			RuleType:   r.GetRuleType(),
			TableName:  r.GetTableName(),
			Expression: r.GetExpression(),
			Active:     true, // agent only receives active rules from Atlas
		})
	}
	return out
}

func toProtoStructural(results []validation.StructuralResult) []*agentv1.TableValidation {
	out := make([]*agentv1.TableValidation, 0, len(results))
	for _, r := range results {
		out = append(out, &agentv1.TableValidation{
			TableName:  r.Table,
			SourceRows: r.SourceRows,
			DestRows:   r.DestRows,
			Match:      r.Match,
			Error:      r.Error,
		})
	}
	return out
}

func toProtoContent(results []validation.ContentResult) []*agentv1.ContentValidation {
	out := make([]*agentv1.ContentValidation, 0, len(results))
	for _, r := range results {
		out = append(out, &agentv1.ContentValidation{
			TableName:          r.Table,
			SourceChecksum:     r.SourceChecksum,
			DestChecksum:       r.DestChecksum,
			TotalRows:          r.TotalRows,
			Match:              r.Match,
			DiffRowCount:       r.DiffCount,
			MissingInDestCount: r.MissingInDest,
			ExtraInDestCount:   r.ExtraInDest,
			ModifiedCount:      r.Modified,
			Error:              r.Error,
		})
	}
	return out
}

func toProtoSchemas(results []validation.SchemaResult) []*agentv1.SchemaComparison {
	out := make([]*agentv1.SchemaComparison, 0, len(results))
	for _, r := range results {
		sc := &agentv1.SchemaComparison{
			TableName:    r.Table,
			SourceSchema: r.SourceSchema,
			DestSchema:   r.DestSchema,
			Match:        r.Match,
			Error:        r.Error,
		}
		for _, d := range r.Diffs {
			sc.Differences = append(sc.Differences, &agentv1.SchemaDiff{
				ColumnName:  d.ColumnName,
				DiffType:    d.DiffType,
				SourceValue: d.SourceValue,
				DestValue:   d.DestValue,
			})
		}
		out = append(out, sc)
	}
	return out
}

func toProtoSemantic(results []validation.SemanticResult) []*agentv1.SemanticValidation {
	out := make([]*agentv1.SemanticValidation, 0, len(results))
	for _, r := range results {
		out = append(out, &agentv1.SemanticValidation{
			RuleName:   r.RuleName,
			RuleType:   r.RuleType,
			TableName:  r.TableName,
			Expression: r.Expression,
			Expected:   r.Expected,
			Actual:     r.Actual,
			Passed:     r.Passed,
			Error:      r.Error,
		})
	}
	return out
}

// ---------------------------------------------------------------------------
// Report builder + accuracy computation
// ---------------------------------------------------------------------------

// buildValidationReport converts shared results to proto and computes accuracy.
func buildValidationReport(
	jobID, runID string,
	structural []validation.StructuralResult,
	content []validation.ContentResult,
	schemas []validation.SchemaResult,
	semantic []validation.SemanticResult,
) *agentv1.ValidationReport {
	r := &agentv1.ValidationReport{
		JobId:      jobID,
		RunId:      runID,
		Structural: toProtoStructural(structural),
		Content:    toProtoContent(content),
		Schemas:    toProtoSchemas(schemas),
		Semantic:   toProtoSemantic(semantic),
	}

	// Compute accuracies from shared results.
	r.StructuralAccuracy = validation.StructuralAccuracy(structural)

	cs := validation.ContentAccuracy(content)
	r.ContentAccuracy = cs.Accuracy
	r.ContentTablesTotal = int32(cs.TablesTotal)
	r.ContentTablesMatch = int32(cs.TablesMatch)
	r.ContentTablesDiff = int32(cs.TablesDiff)
	r.TotalRowsHashed = cs.TotalRows
	r.TotalDiffRows = cs.TotalDiff

	r.SchemaFidelity = validation.SchemaAccuracy(schemas)
	r.SemanticAccuracy = validation.SemanticAccuracy(semantic)
	r.OverallAccuracy = validation.ComputeOverallAccuracy(
		r.StructuralAccuracy, r.ContentAccuracy,
		r.SchemaFidelity, r.SemanticAccuracy,
		len(structural) > 0, len(content) > 0,
		len(schemas) > 0, len(semantic) > 0,
	)

	return r
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
