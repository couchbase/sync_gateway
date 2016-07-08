[Unit]
Description=Couchbase Sync Gateway server
After=syslog.target
After=network.target

[Service]
LimitNOFILE=65535
Environment=\"RUNBASE=${RUNBASE_TEMPLATE_VAR}\"
Environment=\"GATEWAY=${GATEWAY_TEMPLATE_VAR}\"
Environment=\"CONFIG=${CONFIG_TEMPLATE_VAR}\"
Environment=\"LOGS=${LOGS_TEMPLATE_VAR}\"
Environment=\"NAME=${SERVICE_NAME}\"
Type=simple
User=${RUNAS_TEMPLATE_VAR}
WorkingDirectory=${RUNBASE_TEMPLATE_VAR}
ExecStartPre=/bin/mkdir -p ${LOGS_TEMPLATE_VAR}
ExecStartPre=/bin/chown -R ${RUNAS_TEMPLATE_VAR}:${RUNAS_TEMPLATE_VAR} ${LOGS_TEMPLATE_VAR}
ExecStartPre=/bin/mkdir -p ${RUNBASE_TEMPLATE_VAR}/data
ExecStartPre=/bin/chown -R ${RUNAS_TEMPLATE_VAR}:${RUNAS_TEMPLATE_VAR} ${RUNBASE_TEMPLATE_VAR}/data
ExecStart=/bin/bash -c '\${GATEWAY} \${CONFIG} >> \${LOGS}/\${NAME}_access.log 2>> \${LOGS}/\${NAME}_error.log'
Restart=on-failure

# Give a reasonable amount of time for the server to start up/shut down
TimeoutSec=60

[Install]
WantedBy=multi-user.target
