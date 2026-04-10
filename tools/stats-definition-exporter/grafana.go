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
	MetricPrefix   string // Prefix to add to metric names (e.g., "parsed_" for supportal)
	DashboardUID   string
	DashboardTitle string
	SchemaVersion  int
	PluginVersion  string
	DatasourceType string
	DatasourceUID  string
	annotations    []annotation
	templateVars   []templateVariable
	BaseLegend     string // Legend format for stats with no extra labels
	BaseSelector   string // Label selector applied to all stats (with variable placeholders)
	// LabelSelectors defines the additional selector fragment and legend suffix
	// for each Prometheus label, in the order they should appear in legends.
	LabelSelectors []labelSelector
	// LabelReplaces defines label_replace() calls to wrap around the PromQL expression.
	// Applied in order, outermost last.
	LabelReplaces []labelReplace
}

// labelSelector defines the additional PromQL selector and legend text for a label.
type labelSelector struct {
	Label    string // Prometheus label name to match (e.g. "database", "collection")
	Selector string // e.g. `,database=~"$endpoint"`
	Legend   string // e.g. " {{database}}"
}

// labelReplace defines a PromQL label_replace() operation.
type labelReplace struct {
	DstLabel    string // new label name
	Replacement string // replacement pattern (e.g. "$1")
	SrcLabel    string // source label to match against
	Regex       string // regex to apply to source label
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
	Annotations   annotations `json:"annotations"`
	Editable      bool        `json:"editable"`
	FiscalYear    int         `json:"fiscalYearStartMonth"`
	GraphTooltip  int         `json:"graphTooltip"`
	Links         []link      `json:"links,omitempty"`
	Panels        []panel     `json:"panels"`
	Preload       bool        `json:"preload"`
	Refresh       string      `json:"refresh"`
	SchemaVersion int         `json:"schemaVersion"`
	Tags          []string    `json:"tags"`
	Templating    templating  `json:"templating"`
	Time          timeRange   `json:"time"`
	Timepicker    timepicker  `json:"timepicker"`
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
	FieldConfig   *fieldConfig   `json:"fieldConfig,omitempty"`
	GridPos       gridPos        `json:"gridPos"`
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
	Thresholds thresholds    `json:"thresholds"`
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
	HideFrom          hideFrom        `json:"hideFrom"`
	InsertNulls       bool            `json:"insertNulls"`
	LineInterpolation string          `json:"lineInterpolation"`
	LineWidth         int             `json:"lineWidth"`
	PointSize         int             `json:"pointSize"`
	ScaleDistribution scaleDist       `json:"scaleDistribution"`
	ShowPoints        string          `json:"showPoints"`
	ShowValues        bool            `json:"showValues"`
	SpanNulls         bool            `json:"spanNulls"`
	Stacking          stacking        `json:"stacking"`
	ThresholdsStyle   thresholdsStyle `json:"thresholdsStyle"`
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
	Matcher    matcher    `json:"matcher"`
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

// hasLabel checks if the stat has a specific label
func hasLabel(labels []string, label string) bool {
	return slices.Contains(labels, label)
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
			GridPos:   gridPos{H: 1, W: 24, X: 0, Y: yPos},
			ID:        panelID,
			Panels:    childPanels,
			Title:     subsystem,
			Type:      "row",
		}
		panels = append(panels, rowPanel)
		panelID++
		yPos++ // Row panels take 1 unit of height when collapsed
	}

	// Create dashboard
	dashboard := dashboard{
		Annotations:  annotations{List: config.annotations},
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
		Templating:    templating{List: config.templateVars},
		Time:          timeRange{From: "now-7d", To: "now"},
		Timepicker:    timepicker{},
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
	legendFormat := legendForLabels(stat.Labels, config)

	ds := datasourceRef{
		Type: config.DatasourceType,
		UID:  config.DatasourceUID,
	}

	// Create the panel
	panel := panel{
		Datasource:  &ds,
		Description: stat.Help,
		FieldConfig: &fieldConfig{
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
					HideFrom:          hideFrom{Legend: false, Tooltip: false, Viz: false},
					InsertNulls:       false,
					LineInterpolation: "linear",
					LineWidth:         1,
					PointSize:         5,
					ScaleDistribution: scaleDist{Type: "linear"},
					ShowPoints:        "auto",
					ShowValues:        false,
					SpanNulls:         false,
					Stacking:          stacking{Group: "A", Mode: "none"},
					ThresholdsStyle:   thresholdsStyle{Mode: "off"},
				},
				Mappings: []interface{}{},
				Thresholds: thresholds{
					Mode: "absolute",
					Steps: []thresholdStep{
						{Color: "green", Value: nil},
					},
				},
				Unit: mapUnit(stat.Unit),
			},
			Overrides: []override{},
		},
		GridPos: gridPos{
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
	sb.WriteString(config.BaseSelector)
	for _, ls := range config.LabelSelectors {
		if hasLabel(stat.Labels, ls.Label) && ls.Selector != "" {
			sb.WriteString(ls.Selector)
		}
	}
	sb.WriteString("}")

	expr := sb.String()
	for _, lr := range config.LabelReplaces {
		expr = `label_replace(` + expr + `, "` + lr.DstLabel + `", "` + lr.Replacement + `", "` + lr.SrcLabel + `", "` + lr.Regex + `")`
	}
	return expr
}

// legendForLabels builds the legend format string for a stat based on its labels
func legendForLabels(labels []string, config grafanaFormatConfig) string {
	legend := config.BaseLegend
	for _, ls := range config.LabelSelectors {
		if hasLabel(labels, ls.Label) {
			legend += ls.Legend
		}
	}
	return legend
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
