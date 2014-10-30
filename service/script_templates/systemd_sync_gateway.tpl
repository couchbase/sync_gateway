[Unit]
Description=Sync Gateway server
After=syslog.target
After=network.target

[Service]
Environment="RUNAS=${RUNAS_TEMPLATE_VAR}"
Environment="RUNBASE=${RUNBASE_TEMPLATE_VAR}"
Environment="env PIDFILE=${PIDFILE_TEMPLATE_VAR}"
Environment="${GATEWAY_TEMPLATE_VAR}"
Environment="CONFIG=${CONFIG_TEMPLATE_VAR}"
Environment="LOGS=${LOGS_TEMPLATE_VAR}"
Environment="NAME=${SERVICE_NAME}"
Type=simple
User=\${RUNAS}
ExecStart=\$GATEWAY \$CONFIG >> \${LOGS}/\${NAME}_access.log 2>> \${LOGS}/\${NAME}_error.log

# Give a reasonable amount of time for the server to start up/shut down
TimeoutSec=60

[Install]
WantedBy=multi-user.target