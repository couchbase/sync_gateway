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

// grafanaFormatConfig holds the configuration for generating a Grafana dashboard
type grafanaFormatConfig struct {
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
	annotations       []annotation
	HiddenVars        []templateVariable
	GlobalLegend      string // Legend format for global stats
	DBScopedLegend    string // Legend format for database-scoped stats
	GlobalSelector    string // Label selector for global stats (with variable placeholders)
	DBScopedSelector  string // Additional selector for database-scoped stats
}

// annotation represents a Grafana dashboard annotation
type annotation struct {
	BuiltIn     int           `json:"builtIn,omitempty"`
	Datasource  datasourceRef `json:"datasource,omitempty"`
	Enable      bool          `json:"enable"`
	Hide        bool          `json:"hide"`
	IconColor   string        `json:"iconColor,omitempty"`
	Name        string        `json:"name,omitempty"`
	Expr        string        `json:"expr,omitempty"`
	TextFormat  string        `json:"textFormat,omitempty"`
	TitleFormat string        `json:"titleFormat,omitempty"`
	Type        string        `json:"type,omitempty"`
}

// dashboard represents a Grafana dashboard
type dashboard struct {
	annotations   annotations `json:"annotations"`
	Editable      bool        `json:"editable"`
	FiscalYear    int         `json:"fiscalYearStartMonth"`
	GraphTooltip  int         `json:"graphTooltip"`
	Links         []link      `json:"links,omitempty"`
	Panels        []panel     `json:"panels"`
	Preload       bool        `json:"preload"`
	Refresh       string      `json:"refresh"`
	SchemaVersion int         `json:"schemaVersion"`
	Tags          []string    `json:"tags"`
	templating    templating  `json:"templating"`
	Time          timeRange   `json:"time"`
	timepicker    timepicker  `json:"timepicker"`
	Timezone      string      `json:"timezone"`
	Title         string      `json:"title"`
	UID           string      `json:"uid"`
	Version       int         `json:"version,omitempty"`
	WeekStart     string      `json:"weekStart"`
}

// annotations contains the list of annotations
type annotations struct {
	List []annotation `json:"list"`
}

// link represents a dashboard link
type link struct {
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

// panel represents a Grafana panel (timeseries or row)
type panel struct {
	Collapsed     *bool          `json:"collapsed,omitempty"`
	Datasource    *datasourceRef `json:"datasource,omitempty"`
	Description   string         `json:"description,omitempty"`
	fieldConfig   *fieldConfig   `json:"fieldConfig,omitempty"`
	gridPos       gridPos        `json:"gridPos"`
	ID            int            `json:"id"`
	Options       *panelOptions  `json:"options,omitempty"`
	Panels        []panel        `json:"panels,omitempty"`
	PluginVersion string         `json:"pluginVersion,omitempty"`
	Targets       []target       `json:"targets,omitempty"`
	Title         string         `json:"title"`
	Type          string         `json:"type"`
}

// datasourceRef is a reference to a datasource
type datasourceRef struct {
	Type    string `json:"type"`
	UID     string `json:"uid"`
	Default bool   `json:"default,omitempty"`
}

// fieldConfig contains field configuration
type fieldConfig struct {
	Defaults  fieldDefaults `json:"defaults"`
	Overrides []override    `json:"overrides"`
}

// fieldDefaults contains default field settings
type fieldDefaults struct {
	Color      colorConfig   `json:"color"`
	Custom     customConfig  `json:"custom"`
	Mappings   []interface{} `json:"mappings"`
	thresholds thresholds    `json:"thresholds"`
	Unit       string        `json:"unit"`
}

// colorConfig contains color settings
type colorConfig struct {
	Mode string `json:"mode"`
}

// customConfig contains custom visualization settings
type customConfig struct {
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
	hideFrom          hideFrom        `json:"hideFrom"`
	InsertNulls       bool            `json:"insertNulls"`
	LineInterpolation string          `json:"lineInterpolation"`
	LineWidth         int             `json:"lineWidth"`
	PointSize         int             `json:"pointSize"`
	ScaleDistribution scaleDist       `json:"scaleDistribution"`
	ShowPoints        string          `json:"showPoints"`
	ShowValues        bool            `json:"showValues"`
	SpanNulls         bool            `json:"spanNulls"`
	stacking          stacking        `json:"stacking"`
	thresholdsStyle   thresholdsStyle `json:"thresholdsStyle"`
}

// hideFrom contains hide settings
type hideFrom struct {
	Legend  bool `json:"legend"`
	Tooltip bool `json:"tooltip"`
	Viz     bool `json:"viz"`
}

// scaleDist contains scale distribution settings
type scaleDist struct {
	Type string `json:"type"`
}

// stacking contains stacking settings
type stacking struct {
	Group string `json:"group"`
	Mode  string `json:"mode"`
}

// thresholdsStyle contains thresholds style settings
type thresholdsStyle struct {
	Mode string `json:"mode"`
}

// thresholds contains threshold settings
type thresholds struct {
	Mode  string          `json:"mode"`
	Steps []thresholdStep `json:"steps"`
}

// thresholdStep is a single threshold step
type thresholdStep struct {
	Color string      `json:"color"`
	Value interface{} `json:"value"`
}

// override contains field override settings
type override struct {
	matcher    matcher    `json:"matcher"`
	Properties []property `json:"properties"`
}

// matcher identifies which fields to override
type matcher struct {
	ID      string `json:"id"`
	Options string `json:"options"`
}

// property contains override property settings
type property struct {
	ID    string      `json:"id"`
	Value interface{} `json:"value"`
}

// gridPos contains panel grid position
type gridPos struct {
	H int `json:"h"`
	W int `json:"w"`
	X int `json:"x"`
	Y int `json:"y"`
}

// panelOptions contains panel-specific options
type panelOptions struct {
	Legend  legendOptions  `json:"legend"`
	Tooltip tooltipOptions `json:"tooltip"`
}

// legendOptions contains legend settings
type legendOptions struct {
	Calcs       []string `json:"calcs"`
	DisplayMode string   `json:"displayMode"`
	Placement   string   `json:"placement"`
	ShowLegend  bool     `json:"showLegend"`
}

// tooltipOptions contains tooltip settings
type tooltipOptions struct {
	HideZeros bool   `json:"hideZeros,omitempty"`
	Mode      string `json:"mode"`
	Sort      string `json:"sort"`
}

// target represents a Prometheus query target
type target struct {
	Datasource   datasourceRef `json:"datasource"`
	EditorMode   string        `json:"editorMode"`
	Expr         string        `json:"expr"`
	Instant      bool          `json:"instant"`
	LegendFormat string        `json:"legendFormat"`
	Range        bool          `json:"range"`
	RefID        string        `json:"refId"`
}

// templating contains template variables
type templating struct {
	List []templateVariable `json:"list"`
}

// templateVariable represents a Grafana template variable
type templateVariable struct {
	Current     currentValue     `json:"current,omitempty"`
	Datasource  datasourceRef    `json:"datasource,omitempty"`
	Definition  string           `json:"definition,omitempty"`
	Description string           `json:"description,omitempty"`
	Hide        int              `json:"hide,omitempty"`
	IncludeAll  bool             `json:"includeAll,omitempty"`
	Label       string           `json:"label,omitempty"`
	Multi       bool             `json:"multi,omitempty"`
	Name        string           `json:"name"`
	Options     []variableOption `json:"options,omitempty"`
	Query       interface{}      `json:"query,omitempty"`
	Refresh     int              `json:"refresh"`
	Regex       string           `json:"regex,omitempty"`
	Type        string           `json:"type"`
}

// currentValue represents the current value of a template variable
type currentValue struct {
	Text  string `json:"text"`
	Value string `json:"value,omitempty"`
}

// variableOption represents an option for a template variable
type variableOption struct {
	Text     string `json:"text"`
	Value    string `json:"value"`
	Selected bool   `json:"selected,omitempty"`
}

// variableQuery represents a variable query
type variableQuery struct {
	Query   string `json:"query,omitempty"`
	RefID   string `json:"refId,omitempty"`
	QryType int    `json:"qryType,omitempty"`
}

// timeRange represents the dashboard time range
type timeRange struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// timepicker contains time picker settings
type timepicker struct {
	RefreshIntervals []string `json:"refresh_intervals,omitempty"`
}

// Supportal config for Supportal Grafana dashboards
var supportalConfig = grafanaFormatConfig{
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
	annotations: []annotation{
		{
			BuiltIn:    1,
			Datasource: datasourceRef{Type: "grafana", UID: "-- Grafana --"},
			Enable:     true,
			Hide:       true,
			IconColor:  "rgba(0, 211, 255, 1)",
			Name:       "annotations & Alerts",
			Type:       "dashboard",
		},
		{
			Datasource:  datasourceRef{Type: "prometheus", UID: "mimir"},
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
var capellaConfig = grafanaFormatConfig{
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
	annotations: []annotation{
		{
			BuiltIn:    1,
			Datasource: datasourceRef{Type: "grafana", UID: "-- Grafana --"},
			Enable:     true,
			Hide:       true,
			IconColor:  "rgba(0, 211, 255, 1)",
			Name:       "annotations & Alerts",
			Type:       "dashboard",
		},
	},
	HiddenVars: []templateVariable{
		{
			Name:       "syncgatewayId",
			Datasource: datasourceRef{Type: "prometheus", UID: "P5DCFC7561CCDE821"},
			Definition: `label_values(sgw_up{databaseId=~"$databaseId"},syncgatewayId)`,
			Hide:       2,
			Query:      variableQuery{Query: `label_values(sgw_up{databaseId=~"$databaseId"},syncgatewayId)`, RefID: "PrometheusVariableQueryEditor-variableQuery"},
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
func statsBySubsystem(stats statDefinitions) ([]string, map[string][]string) {
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
func generateGrafanaDashboard(stats statDefinitions, config grafanaFormatConfig) dashboard {
	subsystems, grouped := statsBySubsystem(stats)

	// Create panels grouped by subsystem with row panels
	panels := make([]panel, 0, len(stats)+len(subsystems))
	yPos := 0
	panelID := 1

	for _, subsystem := range subsystems {
		statNames := grouped[subsystem]

		// Create child panels for this subsystem
		childPanels := make([]panel, 0, len(statNames))
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
		rowPanel := panel{
			Collapsed: &collapsed,
			gridPos:   gridPos{H: 1, W: 24, X: 0, Y: yPos},
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
	dashboard := dashboard{
		annotations:  annotations{List: config.annotations},
		Editable:     true,
		FiscalYear:   0,
		GraphTooltip: 1,
		Links: []link{
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
		templating:    templating{List: templateVars},
		Time:          timeRange{From: "now-30d", To: "now"},
		timepicker:    timepicker{},
		Timezone:      "browser",
		Title:         config.DashboardTitle,
		UID:           config.DashboardUID,
		WeekStart:     "",
	}

	return dashboard
}

// createPanel creates a single panel for a stat
func createPanel(name string, stat statDefinition, config grafanaFormatConfig, id int, yPos int) panel {
	metricName := config.MetricPrefix + name

	// Build the Prometheus query expression
	expr := buildExpr(metricName, stat, config)

	// Determine legend format
	legendFormat := config.GlobalLegend
	if hasDatabaseLabel(stat.Labels) {
		legendFormat = config.DBScopedLegend
	}

	ds := datasourceRef{
		Type: config.DatasourceType,
		UID:  config.DatasourceUID,
	}

	// Create the panel
	panel := panel{
		Datasource:  &ds,
		Description: stat.Help,
		fieldConfig: &fieldConfig{
			Defaults: fieldDefaults{
				Color: colorConfig{Mode: "palette-classic"},
				Custom: customConfig{
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
					hideFrom:          hideFrom{Legend: false, Tooltip: false, Viz: false},
					InsertNulls:       false,
					LineInterpolation: "linear",
					LineWidth:         1,
					PointSize:         5,
					ScaleDistribution: scaleDist{Type: "linear"},
					ShowPoints:        "auto",
					ShowValues:        false,
					SpanNulls:         false,
					stacking:          stacking{Group: "A", Mode: "none"},
					thresholdsStyle:   thresholdsStyle{Mode: "off"},
				},
				Mappings: []interface{}{},
				thresholds: thresholds{
					Mode: "absolute",
					Steps: []thresholdStep{
						{Color: "green", Value: nil},
					},
				},
				Unit: mapUnit(stat.Unit),
			},
			Overrides: []override{},
		},
		gridPos: gridPos{
			H: 8,
			W: 24,
			X: 0,
			Y: yPos,
		},
		ID: id,
		Options: &panelOptions{
			Legend: legendOptions{
				Calcs:       []string{},
				DisplayMode: "table",
				Placement:   "right",
				ShowLegend:  true,
			},
			Tooltip: tooltipOptions{
				HideZeros: false,
				Mode:      "multi",
				Sort:      "none",
			},
		},
		Targets: []target{
			{
				Datasource: datasourceRef{
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
func buildExpr(metricName string, stat statDefinition, config grafanaFormatConfig) string {
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
func createTemplateVariables(config grafanaFormatConfig) []templateVariable {
	vars := make([]templateVariable, 0, 5)

	// Add datasource variable for Capella
	if config.DataSourceVarName != "" {
		vars = append(vars, templateVariable{
			Current: currentValue{
				Text:  "ThanosV2",
				Value: "P5766748FE00546FA",
			},
			Hide:       0,
			IncludeAll: false,
			Multi:      false,
			Name:       config.DataSourceVarName,
			Options:    []variableOption{},
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

	clusterVar := templateVariable{
		Datasource:  datasourceRef{Type: "prometheus", UID: datasourceUID},
		Definition:  config.ClusterQuery,
		Description: "UUID of the cluster",
		IncludeAll:  false,
		Label:       "Cluster",
		Multi:       false,
		Name:        config.ClusterVarName,
		Options:     []variableOption{},
		Query:       variableQuery{Query: config.ClusterQuery, RefID: "PrometheusVariableQueryEditor-variableQuery"},
		Refresh:     1,
		Regex:       "",
		Type:        "query",
	}
	vars = append(vars, clusterVar)

	// Node variable (only for supportal)
	if config.NodeVarName != "" {
		nodeVar := templateVariable{
			Current: currentValue{
				Text:  "All",
				Value: "$__all",
			},
			Datasource:  datasourceRef{Type: "prometheus", UID: datasourceUID},
			Definition:  config.NodeQuery,
			Description: "SG node by hostname",
			IncludeAll:  true,
			Label:       "SG Node",
			Multi:       true,
			Name:        config.NodeVarName,
			Options:     []variableOption{},
			Query:       variableQuery{Query: config.NodeQuery, RefID: "PrometheusVariableQueryEditor-variableQuery"},
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
	endpointVar := templateVariable{
		Current: currentValue{
			Text:  "All",
			Value: "$__all",
		},
		Datasource: datasourceRef{Type: "prometheus", UID: endpointDatasourceUID},
		Definition: config.EndpointQuery,
		IncludeAll: true,
		Multi:      true,
		Name:       config.EndpointVarName,
		Options:    []variableOption{},
		Query:      variableQuery{Query: config.EndpointQuery, RefID: "PrometheusVariableQueryEditor-variableQuery"},
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
func writeGrafanaDashboard(stats statDefinitions, config grafanaFormatConfig, writer io.Writer) error {
	dashboard := generateGrafanaDashboard(stats, config)

	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "\t")
	err := encoder.Encode(dashboard)
	if err != nil {
		return err
	}

	return nil
}
