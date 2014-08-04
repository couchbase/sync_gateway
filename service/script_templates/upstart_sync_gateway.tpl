description "Sync Gateway Upstart Script"
version "0.1.0"
author "Andrew Reslan"

# Upstart env vars 
env RUNAS=${RUNAS_TEMPLATE_VAR}
env RUNBASE=${RUNBASE_TEMPLATE_VAR}
env PIDFILE=${PIDFILE_TEMPLATE_VAR}
env GATEWAY=${GATEWAY_TEMPLATE_VAR}
env CONFIG=${CONFIG_TEMPLATE_VAR}
env LOGS=${LOGS_TEMPLATE_VAR}

name=${SERVICE_NAME}
stdout_log="$LOGS/$name_access.log"
stderr_log="$LOGS/$name_error.log"

# Keep the server running on crash or machine reboot
start on started mountall
stop on shutdown
respawn

pre-start script
  mkdir -p $LOGS
  chown -R $RUNAS:$RUNAS $LOGS

  mkdir -p $HOME/data
  chown -R $RUNAS:$RUNAS $HOME/data
end script

exec start-stop-daemon --start --chuid $RUNAS --chdir $RUNBASE --make-pidfile --pidfile $PIDFILE --startas $GATEWAY -- $CONFIG >> $LOGS/sync_access.log 2>> $LOGS/sync_error.log
