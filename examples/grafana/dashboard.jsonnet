local grafana = import 'grafonnet/grafonnet/grafana.libsonnet';
local dashboard = grafana.dashboard;
local row = grafana.row;
local singlestat = grafana.singlestat;
local graphPanel = grafana.graphPanel;
local prometheus = grafana.prometheus;

dashboard.new(
  'Couchbase Sync Gateway Dashboard',
  description='',
  refresh='10s',
  time_from='now-1h',
  tags=['couchbase'],
  editable=true,
)
.addTemplate(
  grafana.template.datasource(
    'PROMETHEUS_DS',
    'prometheus',
    'Prometheus',
    hide='label',
  )
)
.addTemplate(
  grafana.template.new(
    'instance',
    '$PROMETHEUS_DS',
    'label_values(sgw_up, instance)',
    label='Instance',
    refresh='load',
    includeAll=true,
    multi=true,
  )
)
.addTemplate(
  grafana.template.new(
    'database',
    '$PROMETHEUS_DS',
    'label_values(sgw_database_sequence_get_count{instance=~"$instance"}, database)',
    label='Database',
    refresh='load',
    includeAll=true,
    multi=true,
  )
)
.addTemplate(
  grafana.template.new(
    'replication',
    '$PROMETHEUS_DS',
    'label_values(sgw_replication_sgr_num_docs_pushed{instance=~"$instance"}, replication)',
    label='SGW-SGW Replications',
    refresh='load',
    includeAll=true,
    multi=true,
  )
)
.addTemplate(
  grafana.template.interval(
    'interval',
    '30s',
    '30s',
    hide=2,
  )
)
.addRow(
  row.new(
    title='Resources',
    collapse=false,
  )
  .addPanel(
    graphPanel.new(
      'CPU Utilization',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='percent',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_resource_utilization_process_cpu_percent_utilization{instance=~"$instance"}',
        legendFormat='{{ instance }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Memory Utilization',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='bytes',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_resource_utilization_process_memory_resident{instance=~"$instance"}',
        legendFormat='{{ instance }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Network Transfer',
      span=12,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='Bps',
      nullPointMode='null as zero',
    )
    .addSeriesOverride(
      {
        alias: '/sent/',
        transform: 'negative-Y',
      }
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_resource_utilization_pub_net_bytes_sent{instance=~"$instance"}[$interval]) +
          rate(sgw_resource_utilization_admin_net_bytes_sent{instance=~"$instance"}[$interval])
        ',
        legendFormat='{{ instance }} sent',
      )
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_resource_utilization_pub_net_bytes_recv{instance=~"$instance"}[$interval]) +
          rate(sgw_resource_utilization_admin_net_bytes_recv{instance=~"$instance"}[$interval])
        ',
        legendFormat='{{ instance }} recv',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Overall Heap Usage',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='bytes',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_resource_utilization_go_memstats_sys{instance=~"$instance"}',
        legendFormat='{{ instance }} sys',
      )
    )
    .addTarget(
      prometheus.target(
        'sgw_resource_utilization_go_memstats_heapalloc{instance=~"$instance"}',
        legendFormat='{{ instance }} heapalloc',
      )
    )
    .addTarget(
      prometheus.target(
        'sgw_resource_utilization_go_memstats_heapidle{instance=~"$instance"}',
        legendFormat='{{ instance }} heapidle',
      )
    )
    .addTarget(
      prometheus.target(
        'sgw_resource_utilization_go_memstats_heapreleased{instance=~"$instance"}',
        legendFormat='{{ instance }} heapreleased',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Overall Stack Usage',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='bytes',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_resource_utilization_go_memstats_stacksys{instance=~"$instance"}',
        legendFormat='{{ instance }} stacksys',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Garbage Collection time [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='ns',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'increase(sgw_resource_utilization_go_memstats_pausetotalns{instance=~"$instance"}[$interval])',
        legendFormat='{{ instance }} pause time',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Logging',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='short',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_resource_utilization_error_count{instance=~"$instance"}',
        legendFormat='{{ instance }} total errors',
      )
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_resource_utilization_error_count{instance=~"$instance"}[$interval])',
        legendFormat='{{ instance }} errors/sec',
      )
    )
    .addTarget(
      prometheus.target(
        'sgw_resource_utilization_warn_count{instance=~"$instance"}',
        legendFormat='{{ instance }} total warns',
      )
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_resource_utilization_warn_count{instance=~"$instance"}[$interval])',
        legendFormat='{{ instance }} warns/sec',
      )
    )
  )
)
.addRow(
  row.new(
    title='Cache',
    collapse=false,
  )
  .addPanel(
    graphPanel.new(
      'Channel Cache Utilization',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='short',
      min=0,
      decimals=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_cache_chan_cache_active_revs{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} active revs',
      )
    )
    .addTarget(
      prometheus.target(
        'sgw_cache_chan_cache_tombstone_revs{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} tombstone revs',
      )
    )
    .addTarget(
      prometheus.target(
        'sgw_cache_chan_cache_removal_revs{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} removal revs',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Channel Cache Management',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='short',
      min=0,
      decimals=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_cache_chan_cache_channels_added{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} channels added',
      )
    )
    .addTarget(
      prometheus.target(
        'sgw_cache_chan_cache_channels_evicted_inactive{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} channels evicted(inactive)',
      )
    )
    .addTarget(
      prometheus.target(
        'sgw_cache_chan_cache_channels_evicted_nru{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} channels evicted(nru)',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Channel Cache Hits',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      stack=true,
      nullPointMode='null as zero',
      decimals=0,
      min=0,
    )
    .addTarget(
      prometheus.target(
        'sgw_cache_chan_cache_hits{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} hits',
      )
    )
    .addTarget(
      prometheus.target(
        'sgw_cache_chan_cache_misses{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} misses',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Channel Cache Size',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='short',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        '(
            sgw_cache_chan_cache_active_revs{instance=~"$instance",database=~"$database"} +
            sgw_cache_chan_cache_tombstone_revs{instance=~"$instance",database=~"$database"} +
            sgw_cache_chan_cache_removal_revs{instance=~"$instance",database=~"$database"}
          ) / sgw_cache_chan_cache_num_channels{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} average',
      )
    )
    .addTarget(
      prometheus.target(
        'sgw_cache_chan_cache_max_entries{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} max',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Channel Cache Count',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='short',
      min=0,
      decimals=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_cache_chan_cache_num_channels{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Revs Cache Performance (hit/miss)',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='short',
      stack=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_cache_rev_cache_hits{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} hits',
      )
    )
    .addTarget(
      prometheus.target(
        'sgw_cache_rev_cache_misses{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} misses',
      )
    )
  )
)
.addRow(
  row.new(
    title='Database Stats',
    collapse=false,
  )
  .addPanel(
    graphPanel.new(
      'Number of Active Replications',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='short',
      min=0,
      decimals=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_database_num_replications_active{instance=~"$instance",database=~"$database"} +
          (sgw_replication_pull_num_replications_active{instance=~"$instance",database=~"$database"} OR on() vector(0))',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of new Replications [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_database_num_replications_total{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Closed Replications',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='short',
      min=0,
      decimals=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_database_num_replications_total{instance=~"$instance",database=~"$database"} -
          sgw_database_num_replications_active{instance=~"$instance",database=~"$database"}
        ',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of document writes [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_database_num_doc_writes{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      '% of docs in conflict',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='percent',
      min=0,
    )
    .addTarget(
      prometheus.target(
        'sgw_replication_push_conflict_write_count{instance=~"$instance",database=~"$database"} /
          sgw_database_num_doc_writes{instance=~"$instance",database=~"$database"}
        ',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of document reads [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_database_num_doc_reads_rest{instance=~"$instance",database=~"$database"}[$interval]) +
          rate(sgw_database_num_doc_reads_blip{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
)
.addRow(
  row.new(
    title='Delta Sync',
    collapse=false,
  )
  .addPanel(
    graphPanel.new(
      'Delta Cache Hit Ratio',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      stack=true,
      format='short',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_delta_sync_delta_cache_hit{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} hits',
      )
    )
    .addTarget(
      prometheus.target(
        'sgw_delta_sync_delta_cache_miss{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} misses',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Delta Hit/Miss rate',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='short',
      stack=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_delta_sync_deltas_requested{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} requested',
      )
    )
    .addTarget(
      prometheus.target(
        'sgw_delta_sync_deltas_sent{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} sent',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of documents sent to SG as a delta [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_delta_sync_delta_push_doc_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of pull replications using deltas [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='ops',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_delta_sync_delta_pull_replication_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Percentage of pulled documents using delta',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      stack=true,
      format='percent',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        '100 * (
          sgw_delta_sync_deltas_sent{instance=~"$instance",database=~"$database"} /
          sgw_database_num_doc_reads_blip{instance=~"$instance",database=~"$database"}
        )',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Percentage of pushed documents using delta',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      stack=true,
      format='percent',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        '100 * (
          sgw_delta_sync_delta_push_doc_count{instance=~"$instance",database=~"$database"} /
          sgw_replication_push_doc_push_count{instance=~"$instance",database=~"$database"}
        )',
        legendFormat='{{ database }}',
      )
    )
  )
)
.addRow(
  row.new(
    title='Import',
    collapse=false,
  )
  .addPanel(
    graphPanel.new(
      'Docs Imported',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_shared_bucket_import_import_count{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} total',
      )
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_shared_bucket_import_import_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }} rate [$interval]',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of errors as a result of doc import [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_shared_bucket_import_import_error_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Import cancels as a result of CAS match [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_shared_bucket_import_import_cancel_cas{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} total',
      )
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_shared_bucket_import_import_cancel_cas{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Import Processing Time [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_shared_bucket_import_import_processing_time{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
)
.addRow(
  row.new(
    title='CBL Push Replication',
    collapse=false,
  )
  .addPanel(
    graphPanel.new(
      'Average Write Processing Time',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='ns',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_replication_push_write_processing_time{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of documents pushed [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_replication_push_doc_push_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Average Sync Function Processing Time',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='ns',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'increase(sgw_replication_push_sync_function_time{instance=~"$instance",database=~"$database"}[$interval]) /
          increase(sgw_replication_push_sync_function_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Average ProposeChange Processing Time',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='ns',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_replication_push_propose_change_time{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Number of ProposeChange messages [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'increase(sgw_replication_push_propose_change_time{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of attachments pushed [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_replication_push_attachment_push_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Average size of attachments pushed',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='bytes',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_replication_push_attachment_push_bytes{instance=~"$instance",database=~"$database"} /
          sgw_replication_push_attachment_push_count
        ',
        legendFormat='{{ database }}',
      )
    )
  )
)
.addRow(
  row.new(
    title='CBL Pull Replication',
    collapse=false,
  )
  .addPanel(
    graphPanel.new(
      'Changes Request Processing Latency',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='s',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_replication_pull_request_changes_count{instance=~"$instance",database=~"$database"} /
          sgw_replication_pull_request_changes_time',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Server DCP Feed Processing Latency',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='ns',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_database_dcp_caching_time{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Revision Send Message Processing Latency ',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='ns',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_replication_pull_rev_send_latency{instance=~"$instance",database=~"$database"} /
          sgw_replication_pull_rev_send_count',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of attachments pulled [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_replication_pull_attachment_pull_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Average size of attachments pulled',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='bytes',
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_replication_pull_attachment_pull_bytes{instance=~"$instance",database=~"$database"} /
          sgw_replication_pull_attachment_pull_count
        ',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of documents pulled in 2.x [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_database_num_doc_reads_blip{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Total Replications by type',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      decimals=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_replication_pull_num_pull_repl_total_one_shot{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} one shot',
      )
    )
    .addTarget(
      prometheus.target(
        'sgw_replication_pull_num_pull_repl_total_continuous{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} continuous',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Active Replications by type',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      decimals=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_replication_pull_num_pull_repl_active_one_shot{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} one shot',
      )
    )
    .addTarget(
      prometheus.target(
        'sgw_replication_pull_num_pull_repl_active_continuous{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }} continuous',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of New Initial Replications [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_replication_pull_num_pull_repl_since_zero{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Replications Caught up Count',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      decimals=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'sgw_replication_pull_num_pull_repl_caught_up{instance=~"$instance",database=~"$database"}',
        legendFormat='{{ database }}',
      )
    )
  )
)
.addRow(
  row.new(
    title='Security',
    collapse=false,
  )
  .addPanel(
    graphPanel.new(
      'Rate of Doc Rejections (by Sync Function) [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_security_num_docs_rejected{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of Access Failures (by Sync Function) [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_security_num_access_errors{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of sync Function Auth Failure Count [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_security_auth_failed_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
)
.addRow(
  row.new(
    title='GSI VIew / Query',
    collapse=false,
  )
  .addPanel(
    graphPanel.new(
      'Rate of queries [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw::gsi::total_queries{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of channel queries [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_gsi_views_channels_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of access queries [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_gsi_views_access_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of allDocs queries [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_gsi_views_allDocs_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of role access queries [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_gsi_views_roleAccess_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of principal queries [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_gsi_views_principals_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of resync queries [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_gsi_views_resync_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of sequences queries [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_gsi_views_sequences_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of sessions queries [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_gsi_views_sessions_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of tombstone queries [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_gsi_views_tombstones_count{instance=~"$instance",database=~"$database"}[$interval])',
        legendFormat='{{ database }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Maintenance Queries',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
      decimals=0,
    )
    .addTarget(
      prometheus.target(
        'sgw::gsi::total_queries{instance=~"$instance",database=~"$database"} -
          sgw_gsi_views_channels_count -
          sgw_gsi_views_access_count -
          sgw_gsi_views_roleAccess_count -
          sgw_gsi_views_allDocs_count
        ',
        legendFormat='{{ database }}',
      )
    )
  )
)
.addRow(
  row.new(
    title='SG to SG Replications',
    collapse=false,
  )
  .addPanel(
    graphPanel.new(
      'Number of docs transferred',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='short',
      nullPointMode='null as zero',
      min=0,
      decimals=0,
    )
    .addTarget(
      prometheus.target(
        'sgw_replication_sgr_num_docs_pushed{instance=~"$instance",replication=~"$replication"}',
        legendFormat='{{ replication }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Number of attachments pushed total',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='short',
      nullPointMode='null as zero',
      min=0,
      decimals=0,
    )
    .addTarget(
      prometheus.target(
        'sgw_replication_sgr_num_attachments_transferred{instance=~"$instance",replication=~"$replication"}',
        legendFormat='{{ replication }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Attachment bytes transferred',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='short',
      nullPointMode='null as zero',
      min=0,
      decimals=0,
    )
    .addTarget(
      prometheus.target(
        'sgw_replication_sgr_num_attachment_bytes_transferred{instance=~"$instance",replication=~"$replication"}',
        legendFormat='{{ replication }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Number of permanent errors on doc transfers',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='short',
      nullPointMode='null as zero',
      min=0,
      decimals=0,
    )
    .addTarget(
      prometheus.target(
        'sgw_replication_sgr_num_docs_failed_to_push{instance=~"$instance",replication=~"$replication"}',
        legendFormat='{{ replication }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Number of documents checked',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      format='short',
      nullPointMode='null as zero',
      min=0,
      decimals=0,
    )
    .addTarget(
      prometheus.target(
        'sgw_replication_sgr_docs_checked_sent{instance=~"$instance",replication=~"$replication"}',
        legendFormat='{{ replication }}',
      )
    )
  )
  .addPanel(
    graphPanel.new(
      'Rate of docs transferred [$interval]',
      span=6,
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_sort='current',
      legend_sortDesc=true,
      min=0,
      nullPointMode='null as zero',
    )
    .addTarget(
      prometheus.target(
        'rate(sgw_replication_sgr_num_docs_pushed{instance=~"$instance",replication=~"$replication"}[$interval])',
        legendFormat='{{ replication }}',
      )
    )
  )
)