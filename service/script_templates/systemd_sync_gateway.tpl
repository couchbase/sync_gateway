[Unit]
Description=Redmine server
After=syslog.target
After=network.target

[Service]
Type=simple
User=<redmine user>
Group=<redmine group>
ExecStart=/bin/ruby <redmine home>/script/rails server webrick -e production

# Give a reasonable amount of time for the server to start up/shut down
TimeoutSec=300

[Install]
WantedBy=multi-user.target