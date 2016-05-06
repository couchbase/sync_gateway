#!/bin/sh
### BEGIN INIT INFO
# Provides:          ${SERVICE_NAME}
# Required-Start:    $remote_fs $syslog
# Required-Stop:     $remote_fs $syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start sync_gateway service at boot time
# Description:       Allow sync_gateway to be run as a service on linux/unix distros
### END INIT INFO

RUNAS=${RUNAS_TEMPLATE_VAR}
RUNBASE=${RUNBASE_TEMPLATE_VAR}
PIDFILE=${PIDFILE_TEMPLATE_VAR}
GATEWAY=${GATEWAY_TEMPLATE_VAR}
CONFIG=${CONFIG_TEMPLATE_VAR}
LOGS=${LOGS_TEMPLATE_VAR}

name=${SERVICE_NAME}
stdout_log=\${LOGS}/\${name}_access.log
stderr_log=\${LOGS}/\${name}_error.log

get_pid() {
    cat \"\$PIDFILE\"    
}

is_running() {
    [ -f \"\$PIDFILE\" ] && ps \`get_pid\` > /dev/null 2>&1
}

mkdir -p \$LOGS
chown -R \$RUNAS:\$RUNAS \$LOGS

mkdir -p \$RUNBASE/data
chown -R \$RUNAS:\$RUNAS \$RUNBASE/data

case \"\$1\" in
    start)
        if is_running; then
            echo "Already started"
        else
            echo "Starting $name"
            cd \"\$RUNBASE\"
            sudo -u \"\$RUNAS\" \$GATEWAY \$CONFIG >> \"\$stdout_log\" 2>> \"\$stderr_log\" &
            echo \$! > \"\$PIDFILE\"
            if ! is_running; then
                echo "Unable to start, see \$stdout_log and \$stderr_log"
                exit 1
            fi
        fi
        ;;
    stop)
        if is_running; then
            echo -n "Stopping $name.."
            kill \`get_pid\`
            for i in {1..10}
            do
                if ! is_running; then
                    break
                fi
                
                echo -n "."
                sleep 1
            done
            echo
            
            if is_running; then
                echo "Not stopped, may still be shutting down or shutdown may have failed"
                exit 1
            else
                echo "Stopped"
                if [ -f \"\$PIDFILE\" ]; then
                    rm \"\$PIDFILE\"
                fi
            fi
        else
            echo "Not running"
        fi
        ;;
    restart)
        service \$name stop
        if is_running; then
            echo "Unable to stop, will not attempt to start"
            exit 1
        fi
        service \$name start
        ;;
    status)
        if is_running; then
            echo "Running"
        else
            echo "Stopped"
            exit 1
        fi
        ;;
    *)
        exit 1
        ;;
esac

exit 0