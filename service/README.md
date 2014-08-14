# Couchbase Sync Gateway Service installation script

The shell script sync_gateway_service_install.sh will install sync_gateway as a service on Ubuntu 10/12 and RedHat/CentOS 5/6 servers

The script determines the type of system it is running on, then copies one of the service templates in script_templates
to the appropriate service directoy. The template is also populated with system specific configuration properties.

The shell script may be run without any parameters, in this case a service called sync_gateway will be created, there is a prerequisite that
the user sync_gateway already exists on the system.

If the default configuration is not suitable, or multiple service instances need to be created, arguments can be passed to the shell script.

To see the list of arguments available pass a -h or --help parameter to the script, the output will be similar to:

```
sync_gateway_service_install.sh
-h --help
  --os : Manually set the target OS for the service <Ubuntu | Redhat >; default (auto detected)
  --ver : Manually set the target OS version; default (auto detected)
  --runas : The user account to run sync_gateway as; default (sync_gateway)
  --runbase : The directory to run sync_gateway from; defaut (/home/sync_gateway)
  --pidfile : The path where the .pid file will be created; default (/var/run/sync-gateway.pid)
  --sgpath : The path to the sync_gateway executable; default (/opt/couchbase-sync-gateway/bin/sync_gateway)
  --srccfgdir : The path to the source sync_gateway JSON config directory)
  --srccfgname : The name of the source sync_gateway JSON config file)
  --cfgpath : The path to the sync_gateway JSON config file; default (/home/sync_gateway/sync_gateway.json)
  --logsdir : The path to the log file direcotry; default (/home/sync_gateway/logs)
  --servicename : The name of the service to install; default (sync_gateway)
```

This script creates an init service to run a sync_gateway instance.
If you want to install more than one service instance
create additional services with different names.


The shell script must be run using the root account or using 'sudo' from a non privilaged account.


