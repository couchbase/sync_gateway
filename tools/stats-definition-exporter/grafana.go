// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included
// in the file licenses/APL2.txt.

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/grafana/grafana-foundation-sdk/go/cog"
	"github.com/grafana/grafana-foundation-sdk/go/common"
	sdkdashboard "github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/prometheus"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

// The dashboard generator uses builders from the official
// grafana-foundation-sdk (github.com/grafana/grafana-foundation-sdk/go). See
// that module's package docs for the authoritative schema of each type.

// grafanaFormatConfig holds the configuration for generating a Grafana dashboard.
type grafanaFormatConfig struct {
	metricPrefix   string // Prefix to add to metric names (e.g., "parsed_" for supportal)
	dashboardUID   string
	dashboardTitle string
	schemaVersion  uint16 // Grafana dashboard JSON schema version to pin (0 = SDK default).
	datasourceType string
	datasourceUID  string
	annotations    []*sdkdashboard.AnnotationQueryBuilder
	templateVars   []cog.Builder[sdkdashboard.VariableModel]
	baseLegend     string // Legend format for stats with no extra labels
	baseSelector   string // Label selector applied to all stats (with variable placeholders)
	// labelSelectors defines the additional selector fragment and legend suffix
	// for each Prometheus label, in the order they should appear in legends.
	labelSelectors []labelSelector
	// labelReplaces defines label_replace() calls to wrap around the PromQL expression.
	// Applied in order, outermost last.
	labelReplaces []labelReplace
}

// labelSelector defines the additional PromQL selector and legend text for a label.
// An empty Selector is legal and means "append to legend but do not filter"; this
// is used for labels (e.g. "replication") that have no matching template
// variable on the dashboard, so we surface them in legends without trying to
// filter by them.
type labelSelector struct {
	Label    string // Prometheus label name to match (e.g. "database", "collection")
	Selector string // e.g. `,database=~"$endpoint"`; empty = legend only, no filter
	Legend   string // e.g. " {{database}}"
}

// labelReplace defines a PromQL label_replace() operation.
type labelReplace struct {
	DstLabel    string // new label name
	Replacement string // replacement pattern (e.g. "$1")
	SrcLabel    string // source label to match against
	Regex       string // regex to apply to source label
}

// unitMapping maps Sync Gateway stat units to Grafana units.
// Stats without an explicit unit default to Grafana's "locale" unit, which
// renders values with the viewer's locale-appropriate thousands separator
// (e.g. 1,000,000 on en-US) instead of a single long run of digits.
var unitMapping = map[string]string{
	"":               "locale",
	"bytes":          "bytes",
	"nanoseconds":    "ns",
	"percent":        "percent",
	"seconds":        "s",
	"unix timestamp": "dateTimeAsIso",
}

// mapUnit converts a Sync Gateway unit to a Grafana unit.
func mapUnit(sgUnit string) string {
	if grafanaUnit, ok := unitMapping[sgUnit]; ok {
		return grafanaUnit
	}
	return "locale"
}

// hasLabel checks if the stat has a specific label.
func hasLabel(labels []string, label string) bool {
	return slices.Contains(labels, label)
}

// grafanaSubsystemName normalizes subsystem names used as Grafana row titles so
// stats that intentionally omit a subsystem are still grouped under a navigable,
// non-empty dashboard section.
func grafanaSubsystemName(subsystem string) string {
	if strings.TrimSpace(subsystem) == "" {
		return "general"
	}
	return subsystem
}

// statsBySubsystem groups stats by subsystem, returning ordered subsystem keys and a map of subsystem to sorted stat names.
func statsBySubsystem(stats statDefinitions) ([]string, map[string][]string) {
	grouped := make(map[string][]string)
	for name, stat := range stats {
		subsystem := grafanaSubsystemName(stat.Subsystem)
		grouped[subsystem] = append(grouped[subsystem], name)
	}

	subsystems := make([]string, 0, len(grouped))
	for subsystem, names := range grouped {
		slices.Sort(names)
		grouped[subsystem] = names
		subsystems = append(subsystems, subsystem)
	}
	slices.Sort(subsystems)

	return subsystems, grouped
}

// ptr returns a pointer to v — a small convenience for the few remaining
// places a pointer literal is the most readable option.
func ptr[T any](v T) *T { return &v }

// describeStat produces the panel description text shown in the Grafana UI:
// the stat's help text followed by a "---" separator and the Sync Gateway
// version range the stat is available in.
func describeStat(stat statDefinition) string {
	desc := stat.Help
	if stat.AddedVersion != "" {
		desc += "\n---\nSGW " + stat.AddedVersion + "+"
		if stat.DeprecatedVersion != "" {
			desc += " (deprecated " + stat.DeprecatedVersion + ")"
		}
	}
	return desc
}

// varQueryPrometheus wraps a raw PromQL variable query in the {query,refId}
// shape Grafana's Prometheus variable editor persists.
func varQueryPrometheus(query string) sdkdashboard.StringOrMap {
	return sdkdashboard.StringOrMap{
		Map: map[string]any{
			"query": query,
			"refId": "PrometheusVariableQueryEditor-variableQuery",
		},
	}
}

// selectAll returns a VariableOption pre-populated with Grafana's "All"
// selection for multi-value template variables.
func selectAll() sdkdashboard.VariableOption {
	return sdkdashboard.VariableOption{
		Text:  sdkdashboard.StringOrArrayOfString{String: ptr("All")},
		Value: sdkdashboard.StringOrArrayOfString{String: ptr("$__all")},
	}
}

// generateGrafanaDashboard creates a Grafana dashboard from stat definitions.
func generateGrafanaDashboard(stats statDefinitions, config grafanaFormatConfig) (sdkdashboard.Dashboard, error) {
	datasource := common.DataSourceRef{
		Type: ptr(config.datasourceType),
		Uid:  ptr(config.datasourceUID),
	}

	b := sdkdashboard.NewDashboardBuilder(config.dashboardTitle).
		Uid(config.dashboardUID).
		Tags([]string{"Sync Gateway"}).
		Tooltip(sdkdashboard.DashboardCursorSyncCrosshair).
		Time("now-7d", "now").
		Link(sdkdashboard.NewDashboardLinkBuilder("Sync Gateway").
			Type(sdkdashboard.DashboardLinkTypeDashboards).
			Icon("external link").
			Tags([]string{"Sync Gateway"}).
			IncludeVars(true).
			KeepTime(true))

	for _, a := range config.annotations {
		b = b.Annotation(a)
	}
	for _, v := range config.templateVars {
		b = b.WithVariable(v)
	}

	subsystems, grouped := statsBySubsystem(stats)
	for _, subsystem := range subsystems {
		row := sdkdashboard.NewRowBuilder(subsystem)
		for _, name := range grouped[subsystem] {
			row = row.WithPanel(createPanel(name, stats[name], datasource, config))
		}
		b = b.WithRow(row)
	}

	d, err := b.Build()
	if err != nil {
		return sdkdashboard.Dashboard{}, fmt.Errorf("build dashboard %q: %w", config.dashboardUID, err)
	}
	if config.schemaVersion != 0 {
		d.SchemaVersion = config.schemaVersion
	}
	return d, nil
}

// createPanel creates a single timeseries panel builder for a stat. Grid
// position is left for the enclosing DashboardBuilder.WithRow to assign.
func createPanel(name string, stat statDefinition, datasource common.DataSourceRef, config grafanaFormatConfig) *timeseries.PanelBuilder {
	metricName := config.metricPrefix + name

	b := timeseries.NewPanelBuilder().
		Title(metricName).
		Description(describeStat(stat)).
		Datasource(datasource).
		Span(24).
		Height(8).
		Unit(mapUnit(stat.Unit)).
		ColorScheme(sdkdashboard.NewFieldColorBuilder().Mode(sdkdashboard.FieldColorModeIdPaletteClassic)).
		Legend(common.NewVizLegendOptionsBuilder().
			DisplayMode(common.LegendDisplayModeTable).
			Placement(common.LegendPlacementRight).
			ShowLegend(true)).
		Tooltip(common.NewVizTooltipOptionsBuilder().
			Mode(common.TooltipDisplayModeMulti).
			Sort(common.SortOrderNone)).
		WithTarget(prometheus.NewDataqueryBuilder().
			RefId("A").
			Expr(buildExpr(metricName, stat, config)).
			LegendFormat(legendForLabels(stat.Labels, config)).
			EditorMode(prometheus.QueryEditorModeCode).
			Range())

	// Force integer y-axis ticks for stats whose underlying values are whole numbers.
	if stat.Format == "int" || stat.Format == "uint64" {
		b = b.Decimals(0)
	}

	return b
}

// buildExpr constructs the Prometheus query expression.
func buildExpr(metricName string, stat statDefinition, config grafanaFormatConfig) string {
	var sb strings.Builder
	sb.WriteString(metricName)
	sb.WriteString("{")
	sb.WriteString(config.baseSelector)
	for _, ls := range config.labelSelectors {
		if hasLabel(stat.Labels, ls.Label) && ls.Selector != "" {
			sb.WriteString(ls.Selector)
		}
	}
	sb.WriteString("}")

	expr := sb.String()
	for _, lr := range config.labelReplaces {
		expr = fmt.Sprintf(`label_replace(%s, "%s", "%s", "%s", "%s")`, expr, lr.DstLabel, lr.Replacement, lr.SrcLabel, lr.Regex)
	}
	return expr
}

// legendForLabels builds the legend format string for a stat based on its labels.
func legendForLabels(labels []string, config grafanaFormatConfig) string {
	legend := config.baseLegend
	for _, ls := range config.labelSelectors {
		if hasLabel(labels, ls.Label) {
			legend += ls.Legend
		}
	}
	return legend
}

// writeGrafanaDashboard writes the Grafana dashboard JSON to the writer.
func writeGrafanaDashboard(stats statDefinitions, config grafanaFormatConfig, writer io.Writer) error {
	dashboard, err := generateGrafanaDashboard(stats, config)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "\t")
	return encoder.Encode(dashboard)
}
