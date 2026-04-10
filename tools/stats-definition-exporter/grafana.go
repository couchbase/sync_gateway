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
	"io"
	"slices"
	"strings"
)

// GrafanaFormatConfig holds the configuration for generating a Grafana dashboard
type GrafanaFormatConfig struct {
	MetricPrefix      string // Prefix to add to metric names (e.g., "parsed_" for supportal)
	DashboardUID      string
	DashboardTitle    string
	SchemaVersion     int
	PluginVersion     string
	DatasourceType    string
	DatasourceUID     string
	ClusterLabel      string // "databaseUuid" for supportal, "databaseId" for capella
	ClusterVarName    string // Variable name for cluster selection
	ClusterQuery      string // Query to get cluster values
	NodeLabel         string // "nodeHostname" for supportal, "" for capella
	NodeVarName       string // Variable name for node selection (empty if not used)
	NodeQuery         string // Query to get node values
	EndpointVarName   string // Variable name for database/endpoint selection
	EndpointQuery     string // Query to get endpoint values
	DataSourceVarName string // Variable name for datasource selection (capella only)
	DataSourceRegex   string // Regex for datasource selection (capella only)
	Annotations       []Annotation
	HiddenVars        []TemplateVariable
	GlobalLegend      string // Legend format for global stats
	DBScopedLegend    string // Legend format for database-scoped stats
	GlobalSelector    string // Label selector for global stats (with variable placeholders)
	DBScopedSelector  string // Additional selector for database-scoped stats
}

// Annotation represents a Grafana dashboard annotation
type Annotation struct {
	BuiltIn     int           `json:"builtIn,omitempty"`
	Datasource  DatasourceRef `json:"datasource,omitempty"`
	Enable      bool          `json:"enable"`
	Hide        bool          `json:"hide"`
	IconColor   string        `json:"iconColor,omitempty"`
	Name        string        `json:"name,omitempty"`
	Expr        string        `json:"expr,omitempty"`
	TextFormat  string        `json:"textFormat,omitempty"`
	TitleFormat string        `json:"titleFormat,omitempty"`
	Type        string        `json:"type,omitempty"`
}

// Dashboard represents a Grafana dashboard
type Dashboard struct {
	Annotations   Annotations `json:"annotations"`
	Editable      bool        `json:"editable"`
	FiscalYear    int         `json:"fiscalYearStartMonth"`
	GraphTooltip  int         `json:"graphTooltip"`
	Links         []Link      `json:"links,omitempty"`
	Panels        []Panel     `json:"panels"`
	Preload       bool        `json:"preload"`
	Refresh       string      `json:"refresh"`
	SchemaVersion int         `json:"schemaVersion"`
	Tags          []string    `json:"tags"`
	Templating    Templating  `json:"templating"`
	Time          TimeRange   `json:"time"`
	Timepicker    Timepicker  `json:"timepicker"`
	Timezone      string      `json:"timezone"`
	Title         string      `json:"title"`
	UID           string      `json:"uid"`
	Version       int         `json:"version,omitempty"`
	WeekStart     string      `json:"weekStart"`
}

// Annotations contains the list of annotations
type Annotations struct {
	List []Annotation `json:"list"`
}

// Link represents a dashboard link
type Link struct {
	AsDropdown  bool     `json:"asDropdown"`
	Icon        string   `json:"icon"`
	IncludeVars bool     `json:"includeVars"`
	KeepTime    bool     `json:"keepTime"`
	Tags        []string `json:"tags,omitempty"`
	TargetBlank bool     `json:"targetBlank"`
	Title       string   `json:"title"`
	Tooltip     string   `json:"tooltip"`
	Type        string   `json:"type"`
	URL         string   `json:"url"`
}

// Panel represents a Grafana panel (timeseries or row)
type Panel struct {
	Collapsed     *bool          `json:"collapsed,omitempty"`
	Datasource    *DatasourceRef `json:"datasource,omitempty"`
	Description   string         `json:"description,omitempty"`
	FieldConfig   *FieldConfig   `json:"fieldConfig,omitempty"`
	GridPos       GridPos        `json:"gridPos"`
	ID            int            `json:"id"`
	Options       *PanelOptions  `json:"options,omitempty"`
	Panels        []Panel        `json:"panels,omitempty"`
	PluginVersion string         `json:"pluginVersion,omitempty"`
	Targets       []Target       `json:"targets,omitempty"`
	Title         string         `json:"title"`
	Type          string         `json:"type"`
}

// DatasourceRef is a reference to a datasource
type DatasourceRef struct {
	Type    string `json:"type"`
	UID     string `json:"uid"`
	Default bool   `json:"default,omitempty"`
}

// FieldConfig contains field configuration
type FieldConfig struct {
	Defaults  FieldDefaults `json:"defaults"`
	Overrides []Override    `json:"overrides"`
}

// FieldDefaults contains default field settings
type FieldDefaults struct {
	Color      ColorConfig   `json:"color"`
	Custom     CustomConfig  `json:"custom"`
	Mappings   []interface{} `json:"mappings"`
	Thresholds Thresholds    `json:"thresholds"`
	Unit       string        `json:"unit"`
}

// ColorConfig contains color settings
type ColorConfig struct {
	Mode string `json:"mode"`
}

// CustomConfig contains custom visualization settings
type CustomConfig struct {
	AxisBorderShow    bool            `json:"axisBorderShow"`
	AxisCenteredZero  bool            `json:"axisCenteredZero"`
	AxisColorMode     string          `json:"axisColorMode"`
	AxisLabel         string          `json:"axisLabel"`
	AxisPlacement     string          `json:"axisPlacement"`
	BarAlignment      int             `json:"barAlignment"`
	BarWidthFactor    float64         `json:"barWidthFactor,omitempty"`
	DrawStyle         string          `json:"drawStyle"`
	FillOpacity       int             `json:"fillOpacity"`
	GradientMode      string          `json:"gradientMode"`
	HideFrom          HideFrom        `json:"hideFrom"`
	InsertNulls       bool            `json:"insertNulls"`
	LineInterpolation string          `json:"lineInterpolation"`
	LineWidth         int             `json:"lineWidth"`
	PointSize         int             `json:"pointSize"`
	ScaleDistribution ScaleDist       `json:"scaleDistribution"`
	ShowPoints        string          `json:"showPoints"`
	ShowValues        bool            `json:"showValues"`
	SpanNulls         bool            `json:"spanNulls"`
	Stacking          Stacking        `json:"stacking"`
	ThresholdsStyle   ThresholdsStyle `json:"thresholdsStyle"`
}

// HideFrom contains hide settings
type HideFrom struct {
	Legend  bool `json:"legend"`
	Tooltip bool `json:"tooltip"`
	Viz     bool `json:"viz"`
}

// ScaleDist contains scale distribution settings
type ScaleDist struct {
	Type string `json:"type"`
}

// Stacking contains stacking settings
type Stacking struct {
	Group string `json:"group"`
	Mode  string `json:"mode"`
}

// ThresholdsStyle contains thresholds style settings
type ThresholdsStyle struct {
	Mode string `json:"mode"`
}

// Thresholds contains threshold settings
type Thresholds struct {
	Mode  string          `json:"mode"`
	Steps []ThresholdStep `json:"steps"`
}

// ThresholdStep is a single threshold step
type ThresholdStep struct {
	Color string      `json:"color"`
	Value interface{} `json:"value"`
}

// Override contains field override settings
type Override struct {
	Matcher    Matcher    `json:"matcher"`
	Properties []Property `json:"properties"`
}

// Matcher identifies which fields to override
type Matcher struct {
	ID      string `json:"id"`
	Options string `json:"options"`
}

// Property contains override property settings
type Property struct {
	ID    string      `json:"id"`
	Value interface{} `json:"value"`
}

// GridPos contains panel grid position
type GridPos struct {
	H int `json:"h"`
	W int `json:"w"`
	X int `json:"x"`
	Y int `json:"y"`
}

// PanelOptions contains panel-specific options
type PanelOptions struct {
	Legend  LegendOptions  `json:"legend"`
	Tooltip TooltipOptions `json:"tooltip"`
}

// LegendOptions contains legend settings
type LegendOptions struct {
	Calcs       []string `json:"calcs"`
	DisplayMode string   `json:"displayMode"`
	Placement   string   `json:"placement"`
	ShowLegend  bool     `json:"showLegend"`
}

// TooltipOptions contains tooltip settings
type TooltipOptions struct {
	HideZeros bool   `json:"hideZeros,omitempty"`
	Mode      string `json:"mode"`
	Sort      string `json:"sort"`
}

// Target represents a Prometheus query target
type Target struct {
	Datasource   DatasourceRef `json:"datasource"`
	EditorMode   string        `json:"editorMode"`
	Expr         string        `json:"expr"`
	Instant      bool          `json:"instant"`
	LegendFormat string        `json:"legendFormat"`
	Range        bool          `json:"range"`
	RefID        string        `json:"refId"`
}

// Templating contains template variables
type Templating struct {
	List []TemplateVariable `json:"list"`
}

// TemplateVariable represents a Grafana template variable
type TemplateVariable struct {
	Current     CurrentValue     `json:"current,omitempty"`
	Datasource  DatasourceRef    `json:"datasource,omitempty"`
	Definition  string           `json:"definition,omitempty"`
	Description string           `json:"description,omitempty"`
	Hide        int              `json:"hide,omitempty"`
	IncludeAll  bool             `json:"includeAll,omitempty"`
	Label       string           `json:"label,omitempty"`
	Multi       bool             `json:"multi,omitempty"`
	Name        string           `json:"name"`
	Options     []VariableOption `json:"options,omitempty"`
	Query       interface{}      `json:"query,omitempty"`
	Refresh     int              `json:"refresh"`
	Regex       string           `json:"regex,omitempty"`
	Type        string           `json:"type"`
}

// CurrentValue represents the current value of a template variable
type CurrentValue struct {
	Text  string `json:"text"`
	Value string `json:"value,omitempty"`
}

// VariableOption represents an option for a template variable
type VariableOption struct {
	Text     string `json:"text"`
	Value    string `json:"value"`
	Selected bool   `json:"selected,omitempty"`
}

// VariableQuery represents a variable query
type VariableQuery struct {
	Query   string `json:"query,omitempty"`
	RefID   string `json:"refId,omitempty"`
	QryType int    `json:"qryType,omitempty"`
}

// TimeRange represents the dashboard time range
type TimeRange struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// Timepicker contains time picker settings
type Timepicker struct {
	RefreshIntervals []string `json:"refresh_intervals,omitempty"`
}

// Supportal config for Supportal Grafana dashboards
var supportalConfig = GrafanaFormatConfig{
	MetricPrefix:     "parsed_",
	DashboardUID:     "sync-gateway-all",
	DashboardTitle:   "Sync Gateway All",
	SchemaVersion:    42,
	PluginVersion:    "12.4.2",
	DatasourceType:   "prometheus",
	DatasourceUID:    "mimir",
	ClusterLabel:     "databaseUuid",
	ClusterVarName:   "databaseUuid",
	ClusterQuery:     "label_values(databaseUuid)",
	NodeLabel:        "nodeHostname",
	NodeVarName:      "nodeHostname",
	NodeQuery:        "label_values(parsed_sgw_resource_utilization_uptime{databaseUuid=\"$databaseUuid\"},nodeHostname)",
	EndpointVarName:  "endpoint",
	EndpointQuery:    "label_values(parsed_sgw_database_doc_writes_bytes{databaseUuid=\"$databaseUuid\"},database)",
	GlobalLegend:     "{{nodeHostname}}",
	DBScopedLegend:   "{{nodeHostname}} {{database}}",
	GlobalSelector:   `databaseUuid="$databaseUuid",nodeHostname=~"$nodeHostname"`,
	DBScopedSelector: `,database=~"$endpoint"`,
	Annotations: []Annotation{
		{
			BuiltIn:    1,
			Datasource: DatasourceRef{Type: "grafana", UID: "-- Grafana --"},
			Enable:     true,
			Hide:       true,
			IconColor:  "rgba(0, 211, 255, 1)",
			Name:       "Annotations & Alerts",
			Type:       "dashboard",
		},
		{
			Datasource:  DatasourceRef{Type: "prometheus", UID: "mimir"},
			Enable:      false,
			Hide:        false,
			IconColor:   "#5794F2",
			Name:        "Show Restarts",
			Expr:        `parsed_sgw_resource_utilization_uptime{databaseUuid="$databaseUuid",nodeHostname=~"$nodeHostname"} <= 1200000000000`,
			TextFormat:  "SG Restart: {{nodeHostname}}",
			TitleFormat: "SG Restart: {{nodeHostname}}",
		},
	},
}

// Capella config for Capella/Cloud Grafana dashboards
var capellaConfig = GrafanaFormatConfig{
	MetricPrefix:      "",
	DashboardUID:      "sync-gateway-all-stats",
	DashboardTitle:    "Sync Gateway All Stats",
	SchemaVersion:     38,
	DatasourceType:    "prometheus",
	DatasourceUID:     "${DataSource}",
	ClusterLabel:      "databaseId",
	ClusterVarName:    "databaseId",
	ClusterQuery:      "label_values(sgw_up,databaseId)",
	NodeLabel:         "",
	NodeVarName:       "",
	NodeQuery:         "",
	EndpointVarName:   "endpoint",
	EndpointQuery:     "label_values(sgw_database_doc_writes_bytes{databaseId=\"$databaseId\"},database)",
	DataSourceVarName: "DataSource",
	DataSourceRegex:   "(ThanosV2|Thanos)",
	GlobalLegend:      "{{databaseId}}",
	DBScopedLegend:    "{{database}}",
	GlobalSelector:    `databaseId="$databaseId"`,
	DBScopedSelector:  `,database=~"$endpoint"`,
	Annotations: []Annotation{
		{
			BuiltIn:    1,
			Datasource: DatasourceRef{Type: "grafana", UID: "-- Grafana --"},
			Enable:     true,
			Hide:       true,
			IconColor:  "rgba(0, 211, 255, 1)",
			Name:       "Annotations & Alerts",
			Type:       "dashboard",
		},
	},
	HiddenVars: []TemplateVariable{
		{
			Name:       "syncgatewayId",
			Datasource: DatasourceRef{Type: "prometheus", UID: "P5DCFC7561CCDE821"},
			Definition: `label_values(sgw_up{databaseId=~"$databaseId"},syncgatewayId)`,
			Hide:       2,
			Query:      VariableQuery{Query: `label_values(sgw_up{databaseId=~"$databaseId"},syncgatewayId)`, RefID: "PrometheusVariableQueryEditor-VariableQuery"},
			Refresh:    1,
			Type:       "query",
		},
	},
}

// unitMapping maps Sync Gateway stat units to Grafana units
var unitMapping = map[string]string{
	"":               "none",
	"bytes":          "bytes",
	"nanoseconds":    "ns",
	"percent":        "percent",
	"seconds":        "s",
	"unix timestamp": "dateTimeAsIso",
}

// mapUnit converts a Sync Gateway unit to a Grafana unit
func mapUnit(sgUnit string) string {
	if grafanaUnit, ok := unitMapping[sgUnit]; ok {
		return grafanaUnit
	}
	return "none"
}

// hasDatabaseLabel checks if the stat has a database label
func hasDatabaseLabel(labels []string) bool {
	return slices.Contains(labels, "database")
}

// subsystemDisplayNames maps subsystem keys to human-readable display names for row panels
var subsystemDisplayNames = map[string]string{
	"audit":                "Audit",
	"cache":                "Cache",
	"collection":           "Collection",
	"config":               "Config",
	"database":             "Database",
	"delta_sync":           "Delta Sync",
	"gsi_views":            "GSI Views",
	"replication":          "Replication (ISGR)",
	"replication_pull":     "Replication Pull",
	"replication_push":     "Replication Push",
	"resource_utilization": "Resource Utilization",
	"security":             "Security",
	"shared_bucket_import": "Shared Bucket Import",
}

// subsystemDisplayName returns the display name for a subsystem
func subsystemDisplayName(subsystem string) string {
	if name, ok := subsystemDisplayNames[subsystem]; ok {
		return name
	}
	return subsystem
}

// statsBySubsystem groups stats by subsystem, returning ordered subsystem keys and a map of subsystem to sorted stat names
func statsBySubsystem(stats StatDefinitions) ([]string, map[string][]string) {
	grouped := make(map[string][]string)
	for name, stat := range stats {
		grouped[stat.Subsystem] = append(grouped[stat.Subsystem], name)
	}

	// Sort stat names within each subsystem
	subsystems := make([]string, 0, len(grouped))
	for subsystem, names := range grouped {
		slices.Sort(names)
		grouped[subsystem] = names
		subsystems = append(subsystems, subsystem)
	}
	slices.Sort(subsystems)

	return subsystems, grouped
}

// generateGrafanaDashboard creates a Grafana dashboard from stat definitions
func generateGrafanaDashboard(stats StatDefinitions, config GrafanaFormatConfig) Dashboard {
	subsystems, grouped := statsBySubsystem(stats)

	// Create panels grouped by subsystem with row panels
	panels := make([]Panel, 0, len(stats)+len(subsystems))
	yPos := 0
	panelID := 1

	for _, subsystem := range subsystems {
		statNames := grouped[subsystem]

		// Create child panels for this subsystem
		childPanels := make([]Panel, 0, len(statNames))
		childYPos := yPos + 1 // Child panels start after the row
		for _, name := range statNames {
			stat := stats[name]
			panel := createPanel(name, stat, config, panelID, childYPos)
			childPanels = append(childPanels, panel)
			panelID++
			childYPos += 8
		}

		// Create the row panel (collapsed, containing child panels)
		collapsed := true
		rowPanel := Panel{
			Collapsed: &collapsed,
			GridPos:   GridPos{H: 1, W: 24, X: 0, Y: yPos},
			ID:        panelID,
			Panels:    childPanels,
			Title:     subsystemDisplayName(subsystem),
			Type:      "row",
		}
		panels = append(panels, rowPanel)
		panelID++
		yPos++ // Row panels take 1 unit of height when collapsed
	}

	// Create template variables
	templateVars := createTemplateVariables(config)

	// Create dashboard
	dashboard := Dashboard{
		Annotations:  Annotations{List: config.Annotations},
		Editable:     true,
		FiscalYear:   0,
		GraphTooltip: 1,
		Links: []Link{
			{
				AsDropdown:  false,
				Icon:        "external link",
				IncludeVars: true,
				KeepTime:    true,
				Tags:        []string{"Sync Gateway"},
				TargetBlank: false,
				Title:       "Sync Gateway",
				Type:        "dashboards",
			},
		},
		Panels:        panels,
		Preload:       false,
		Refresh:       "",
		SchemaVersion: config.SchemaVersion,
		Tags:          []string{"Sync Gateway"},
		Templating:    Templating{List: templateVars},
		Time:          TimeRange{From: "now-30d", To: "now"},
		Timepicker:    Timepicker{},
		Timezone:      "browser",
		Title:         config.DashboardTitle,
		UID:           config.DashboardUID,
		WeekStart:     "",
	}

	if config.PluginVersion != "" {
		// PluginVersion is set on each panel, not the dashboard
	}

	return dashboard
}

// createPanel creates a single panel for a stat
func createPanel(name string, stat StatDefinition, config GrafanaFormatConfig, id int, yPos int) Panel {
	metricName := config.MetricPrefix + name

	// Build the Prometheus query expression
	expr := buildExpr(metricName, stat, config)

	// Determine legend format
	legendFormat := config.GlobalLegend
	if hasDatabaseLabel(stat.Labels) {
		legendFormat = config.DBScopedLegend
	}

	ds := DatasourceRef{
		Type: config.DatasourceType,
		UID:  config.DatasourceUID,
	}

	// Create the panel
	panel := Panel{
		Datasource:  &ds,
		Description: stat.Help,
		FieldConfig: &FieldConfig{
			Defaults: FieldDefaults{
				Color: ColorConfig{Mode: "palette-classic"},
				Custom: CustomConfig{
					AxisBorderShow:    false,
					AxisCenteredZero:  false,
					AxisColorMode:     "text",
					AxisLabel:         "",
					AxisPlacement:     "auto",
					BarAlignment:      0,
					BarWidthFactor:    0.6,
					DrawStyle:         "line",
					FillOpacity:       0,
					GradientMode:      "none",
					HideFrom:          HideFrom{Legend: false, Tooltip: false, Viz: false},
					InsertNulls:       false,
					LineInterpolation: "linear",
					LineWidth:         1,
					PointSize:         5,
					ScaleDistribution: ScaleDist{Type: "linear"},
					ShowPoints:        "auto",
					ShowValues:        false,
					SpanNulls:         false,
					Stacking:          Stacking{Group: "A", Mode: "none"},
					ThresholdsStyle:   ThresholdsStyle{Mode: "off"},
				},
				Mappings: []interface{}{},
				Thresholds: Thresholds{
					Mode: "absolute",
					Steps: []ThresholdStep{
						{Color: "green", Value: nil},
					},
				},
				Unit: mapUnit(stat.Unit),
			},
			Overrides: []Override{},
		},
		GridPos: GridPos{
			H: 8,
			W: 24,
			X: 0,
			Y: yPos,
		},
		ID: id,
		Options: &PanelOptions{
			Legend: LegendOptions{
				Calcs:       []string{},
				DisplayMode: "table",
				Placement:   "right",
				ShowLegend:  true,
			},
			Tooltip: TooltipOptions{
				HideZeros: false,
				Mode:      "multi",
				Sort:      "none",
			},
		},
		Targets: []Target{
			{
				Datasource: DatasourceRef{
					Type: config.DatasourceType,
					UID:  config.DatasourceUID,
				},
				EditorMode:   "code",
				Expr:         expr,
				Instant:      false,
				LegendFormat: legendFormat,
				Range:        true,
				RefID:        "A",
			},
		},
		Title: metricName,
		Type:  "timeseries",
	}

	if config.PluginVersion != "" {
		panel.PluginVersion = config.PluginVersion
	}

	return panel
}

// buildExpr constructs the Prometheus query expression
func buildExpr(metricName string, stat StatDefinition, config GrafanaFormatConfig) string {
	var sb strings.Builder
	sb.WriteString(metricName)
	sb.WriteString("{")
	sb.WriteString(config.GlobalSelector)
	if hasDatabaseLabel(stat.Labels) {
		sb.WriteString(config.DBScopedSelector)
	}
	sb.WriteString("}")
	return sb.String()
}

// createTemplateVariables creates the template variables for the dashboard
func createTemplateVariables(config GrafanaFormatConfig) []TemplateVariable {
	vars := make([]TemplateVariable, 0, 5)

	// Add datasource variable for Capella
	if config.DataSourceVarName != "" {
		vars = append(vars, TemplateVariable{
			Current: CurrentValue{
				Text:  "ThanosV2",
				Value: "P5766748FE00546FA",
			},
			Hide:       0,
			IncludeAll: false,
			Multi:      false,
			Name:       config.DataSourceVarName,
			Options:    []VariableOption{},
			Query:      "prometheus",
			Refresh:    1,
			Regex:      config.DataSourceRegex,
			Type:       "datasource",
		})
	}

	// Cluster variable (databaseUuid or databaseId)
	datasourceUID := config.DatasourceUID
	if config.DataSourceVarName != "" {
		datasourceUID = "P5DCFC7561CCDE821" // Capella uses a fixed datasource for variable queries
	}

	clusterVar := TemplateVariable{
		Datasource:  DatasourceRef{Type: "prometheus", UID: datasourceUID},
		Definition:  config.ClusterQuery,
		Description: "UUID of the cluster",
		IncludeAll:  false,
		Label:       "Cluster",
		Multi:       false,
		Name:        config.ClusterVarName,
		Options:     []VariableOption{},
		Query:       VariableQuery{Query: config.ClusterQuery, RefID: "PrometheusVariableQueryEditor-VariableQuery"},
		Refresh:     1,
		Regex:       "",
		Type:        "query",
	}
	vars = append(vars, clusterVar)

	// Node variable (only for supportal)
	if config.NodeVarName != "" {
		nodeVar := TemplateVariable{
			Current: CurrentValue{
				Text:  "All",
				Value: "$__all",
			},
			Datasource:  DatasourceRef{Type: "prometheus", UID: datasourceUID},
			Definition:  config.NodeQuery,
			Description: "SG node by hostname",
			IncludeAll:  true,
			Label:       "SG Node",
			Multi:       true,
			Name:        config.NodeVarName,
			Options:     []VariableOption{},
			Query:       VariableQuery{Query: config.NodeQuery, RefID: "PrometheusVariableQueryEditor-VariableQuery"},
			Refresh:     1,
			Regex:       "",
			Type:        "query",
		}
		vars = append(vars, nodeVar)
	}

	// Endpoint variable (database)
	endpointDatasourceUID := datasourceUID
	if config.DataSourceVarName != "" {
		// For Capella, use the variable reference
		endpointDatasourceUID = "${" + config.DataSourceVarName + "}"
	}
	endpointVar := TemplateVariable{
		Current: CurrentValue{
			Text:  "All",
			Value: "$__all",
		},
		Datasource: DatasourceRef{Type: "prometheus", UID: endpointDatasourceUID},
		Definition: config.EndpointQuery,
		IncludeAll: true,
		Multi:      true,
		Name:       config.EndpointVarName,
		Options:    []VariableOption{},
		Query:      VariableQuery{Query: config.EndpointQuery, RefID: "PrometheusVariableQueryEditor-VariableQuery"},
		Refresh:    1,
		Regex:      "",
		Type:       "query",
	}
	vars = append(vars, endpointVar)

	// Add hidden variables (for Capella)
	vars = append(vars, config.HiddenVars...)

	return vars
}

// writeGrafanaDashboard writes the Grafana dashboard JSON to the writer
func writeGrafanaDashboard(stats StatDefinitions, config GrafanaFormatConfig, writer io.Writer) error {
	dashboard := generateGrafanaDashboard(stats, config)

	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "\t")
	err := encoder.Encode(dashboard)
	if err != nil {
		return err
	}

	return nil
}
