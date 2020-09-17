# Sync Gateway Prometheus Setup #

With Sync Gateway version 2.8, a new endpoint is provided that exposes stats in a Prometheus compatible format. This feature is available as a "Developer Preview" feature.  In order to enable this, one must simply provide the `metricsInterface` config option within the Sync Gateway config. For example setting this to ":4986" will mean the metrics endpoint is exposed on port 4986.

In order to make the installation and setup process easier we have provided both a `prometheus.yml` file and a set of rules under `rules/sync-gateway.rules.yml`.
 
The `prometheus.yml` file specifies the configuration required to scrape the Sync Gateway metrics target. The provided `prometheus.yml` file points to the `metricsInterface` being accessible on `sync_gateway:4986/_metrics`. This can be changed to a different host or multiple targets can be added if required.

Within this file a rules directory is provided which contains some pre-configured rules: 
 - A total queries record that adds up all query counts and saves it as `sgw::gsi::total_queries`
 - A few example alerts
  
  Prometheus should be setup to use the `prometheus.yml` file and the `rules` directory should be put into `/etc/prometheus`. If the directory can't be put in that location then the `rule_files` configuration option within `prometheus.yml` can be modified to point to a different location. 