# Couchbase Sync Gateway Service installation script

The shell script sync_gateway_service_install.sh will install sync_gateway as a service on Ubuntu 12/14
, RedHat/CentOS 6/7 and MAC OS X 10.9 and later. This installation script is not available for MS Windows.

The script determines the type of system it is running on, then copies one of the service templates in script_templates
to the appropriate service directoy. The template is also populated with system specific configuration properties.

The shell script may be run without any parameters, in this case a service called sync_gateway will be created, there is a prerequisite that
the user sync_gateway already exists on the system.

If the default configuration is not suitable, or multiple service instances need to be created, arguments can be passed to the shell script.

To see the list of arguments available pass a -h or --help parameter to the script, the output will be similar to:

```
sync_gateway_service_install.sh
	-h --help
	--runas=<The user account to run sync_gateway as; default (sync_gateway)>
	--runbase=<The directory to run sync_gateway from; defaut (/home/sync_gateway)>
	--sgpath=<The path to the sync_gateway executable; default (/opt/couchbase-sync-gateway/bin/sync_gateway)>
	--cfgpath=<The path to the sync_gateway JSON config file; default (/home/sync_gateway/sync_gateway.json)>
	--logsdir=<The path to the log file direcotry; default (/home/sync_gateway/logs)>
```

This script creates an init service to run a sync_gateway instance.

The shell script must be run using the root account or using 'sudo' from a non privilaged account.


