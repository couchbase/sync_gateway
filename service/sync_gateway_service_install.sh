#!/bin/sh

# Set default values
OS=""
VER=""
SERVICE_NAME="sync_gateway"
SRCCFGDIR=../examples
SRCCFG=serviceconfig.json
RUNAS_TEMPLATE_VAR=sync_gateway
RUNBASE_TEMPLATE_VAR=/home/sync_gateway
PIDFILE_TEMPLATE_VAR=/var/run/sync-gateway.pid
GATEWAYROOT_TEMPLATE_VAR=/opt/couchbase-sync-gateway
GATEWAY_TEMPLATE_VAR=/opt/couchbase-sync-gateway/bin/sync_gateway
CONFIG_TEMPLATE_VAR=${RUNBASE_TEMPLATE_VAR}/sync_gateway.json
LOGS_TEMPLATE_VAR=${RUNBASE_TEMPLATE_VAR}/logs
SERVICE_CMD_ONLY=false


usage()
{
    echo "This script creates an init service to run a sync_gateway instance."
    echo "If you want to install more than one service instance"
    echo "create additional services with different names."
    echo ""
    echo "sync_gateway_service_install.sh"
    echo "    -h --help"
    echo "    --runas=<The user account to run sync_gateway as; default (sync_gateway)>"
    echo "    --runbase=<The directory to run sync_gateway from; defaut (/home/sync_gateway)>"
    echo "    --sgpath=<The path to the sync_gateway executable; default (/opt/couchbase-sync-gateway/bin/sync_gateway)>"
    echo "    --cfgpath=<The path to the sync_gateway JSON config file; default (/home/sync_gateway/sync_gateway.json)>"
    echo "    --logsdir=<The path to the log file direcotry; default (/home/sync_gateway/logs)>"
    echo ""
}
 
ostype() {
    ARCH=$(uname -m | sed 's/x86_//;s/i[3-6]86/32/')

    if [ -f /etc/lsb-release ]; then
        OS=$(lsb_release -si)
        VER=$(lsb_release -sr)
    elif [ -f /etc/debian_version ]; then
        OS=Debian  # XXX or Ubuntu??
        VER=$(cat /etc/debian_version)
    elif [ -f /etc/redhat-release ]; then
        OS=RedHat
        VER=`cat /etc/redhat-release | sed s/.*release\ // | sed s/\ .*//`
    elif [ -f /etc/system-release ]; then
        OS=RedHat
        VER=5.0
    else
        OS=$(uname -s)
        VER=$(uname -r)
    fi

    OS_MAJOR_VERSION=`echo $VER | sed 's/\..*$//'`
    OS_MINOR_VERSION=`echo $VER | sed s/[0-9]*\.//`
}

# expand template variables + preserve formatting
render_template() {
  eval "echo \"$(cat $1)\""
}

# sets up the output directories for logs and data
setup_output_dirs() {
    mkdir -p ${LOGS_TEMPLATE_VAR}
    chown -R ${RUNAS_TEMPLATE_VAR}  ${LOGS_TEMPLATE_VAR}
    mkdir -p ${RUNBASE_TEMPLATE_VAR}/data
    chown -R ${RUNAS_TEMPLATE_VAR} ${RUNBASE_TEMPLATE_VAR}/data
}

# Run pre installation actions
pre_install_actions() {
    # Check that runtime user account exists
    if [ "$OS" != "Darwin" ] && [ -z `id -u $RUNAS_TEMPLATE_VAR 2>/dev/null` ]; then
        echo "The sync_gateway runtime user account does not exist \"$RUNAS_TEMPLATE_VAR\"." > /dev/stderr
        exit 1
    fi

    # Check that the runtime base directory exists
    if [ ! -d "$RUNBASE_TEMPLATE_VAR" ]; then
        echo "The runtime base directory does not exist \"$RUNBASE_TEMPLATE_VAR\"." > /dev/stderr
        exit 1
    fi

    # Check that the sync_gateway executable exists
    if [ ! -x "$GATEWAY_TEMPLATE_VAR" ]; then
        echo "The sync_gateway executable does not exist \"$GATEWAY_TEMPLATE_VAR\"." > /dev/stderr
        exit 1
    fi

    # Check that the sync_gateway src JSON config directory exists
    if [ ! -d "$SRCCFGDIR" ]; then
        echo "The sync_gateway source JSON config file directory does not exist \"$SRCCFGDIR\"." > /dev/stderr
        exit 1
    fi

    # Check that the sync_gateway src JSON config file exists
    if [ ! -r "$SRCCFGDIR/$SRCCFG" ]; then
        echo "The sync_gateway source JSON config file does not exist\"$SRCCFGDIR/$SRCCFG\"." > /dev/stderr
        exit 1
    fi

    # If a /tmp/log_upr_client.sock socket exists from a previous installation remove it
    if [ -S /tmp/log_upr_client.sock ]; then
        rm -f /tmp/log_upr_client.sock
    fi

    # Copy a default config if defined config file does not exist
    if [ ! -e "$CONFIG_TEMPLATE_VAR" ]; then
        mkdir -p `dirname ${CONFIG_TEMPLATE_VAR}`
        cp $SRCCFGDIR/$SRCCFG $CONFIG_TEMPLATE_VAR
        chown ${RUNAS_TEMPLATE_VAR}:${RUNAS_TEMPLATE_VAR} ${CONFIG_TEMPLATE_VAR}
    fi

    setup_output_dirs
}

#
#script starts here
#

#Figure out the OS type of the current system
ostype

#If the OS is MAC OSX, set the default user account home path to /Users/sync_gateway
if [ "$OS" = "Darwin" ]; then
    RUNBASE_TEMPLATE_VAR=/Users/sync_gateway
    CONFIG_TEMPLATE_VAR=${RUNBASE_TEMPLATE_VAR}/sync_gateway.json
    LOGS_TEMPLATE_VAR=${RUNBASE_TEMPLATE_VAR}/logs
fi

# Make sure we are running with root privilages
if [ `id -u` != 0 ]; then
    echo "This script should be run as root." > /dev/stderr
    exit 1
fi

# Process the command line args
while [ "$1" != "" ]; do
    PARAM=`echo $1 | awk -F= '{print $1}'`
    VALUE=`echo $1 | awk -F= '{print $2}'`
    case $PARAM in
        -h | --help)
            usage
            exit
            ;;
        --runas)
            RUNAS_TEMPLATE_VAR=$VALUE
            if [ "$OS" != "Darwin" ]; then
                RUNBASE_TEMPLATE_VAR=`getent passwd "$VALUE" | cut -d: -f 6`
            else
                RUNBASE_TEMPLATE_VAR=`eval "echo ~$VALUE"`
            fi
            CONFIG_TEMPLATE_VAR=${RUNBASE_TEMPLATE_VAR}/sync_gateway.json
            LOGS_TEMPLATE_VAR=${RUNBASE_TEMPLATE_VAR}/logs
            ;;
        --runbase)
            RUNBASE_TEMPLATE_VAR=$VALUE
            ;;
        --sgpath)
            GATEWAY_TEMPLATE_VAR=$VALUE
            ;;
        --cfgpath)
            CONFIG_TEMPLATE_VAR=$VALUE
            ;;
        --logsdir)
            LOGS_TEMPLATE_VAR=$VALUE
            ;;
        --servicecmd)
            SERVICE_CMD_ONLY=true
            ;;
        *)
            echo "ERROR: unknown parameter \"$PARAM\""
            usage
            exit 1
            ;;
    esac
    shift
done

#Install the service for the specific platform
case $OS in
    Debian)
        case $OS_MAJOR_VERSION in
            8)
                if [ "$SERVICE_CMD_ONLY" = true ]; then
                    echo "systemctl start ${SERVICE_NAME}"
                else
                    pre_install_actions
		    mkdir -p /usr/lib/systemd/system
                    render_template script_templates/systemd_debian_sync_gateway.tpl > /usr/lib/systemd/system/${SERVICE_NAME}.service
                    systemctl enable ${SERVICE_NAME}
                    systemctl start ${SERVICE_NAME}
                fi
                ;;
        esac
    ;;
    Ubuntu)
        case $OS_MAJOR_VERSION in
            12|14)
                if [ "$SERVICE_CMD_ONLY" = true ]; then
                    echo "service ${SERVICE_NAME} start"
                else
                    pre_install_actions
                    render_template script_templates/upstart_ubuntu_sync_gateway.tpl > /etc/init/${SERVICE_NAME}.conf
                    service ${SERVICE_NAME} start
                fi
                ;;
            16)
                if [ "$SERVICE_CMD_ONLY" = true ]; then
                    echo "systemctl start ${SERVICE_NAME}"
                else
                    pre_install_actions
                    render_template script_templates/systemd_debian_sync_gateway.tpl > /lib/systemd/system/${SERVICE_NAME}.service
                    systemctl enable ${SERVICE_NAME}
                    systemctl start ${SERVICE_NAME}
                fi
                ;;
            *)
                echo "ERROR: Unsupported Ubuntu Version \"$VER\""
                usage
                exit 1
                ;;
        esac
        ;;
    RedHat|CentOS)
        case $OS_MAJOR_VERSION in
            5) 
                if [ "$SERVICE_CMD_ONLY" = true ]; then
                    echo "service ${SERVICE_NAME} start"
                else
                    #override location for logs and sync gateway config
                    LOGS_TEMPLATE_VAR=/var/log/${SERVICE_NAME}
                    CONFIG_TEMPLATE_VAR=/opt/${SERVICE_NAME}/etc/sync_gateway.json

                    pre_install_actions
                    render_template script_templates/sysv_sync_gateway.tpl > /etc/init.d/${SERVICE_NAME}
                    chmod 755 /etc/init.d/${SERVICE_NAME}
                    PATH=/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin
                    chkconfig --add ${SERVICE_NAME}
                    chkconfig ${SERVICE_NAME} on
                    service ${SERVICE_NAME} start
                fi
                ;;
            6)
                if [ "$SERVICE_CMD_ONLY" = true ]; then
                    echo "initctl start ${SERVICE_NAME}"
                else
                    pre_install_actions
                    render_template script_templates/upstart_redhat_sync_gateway.tpl > /etc/init/${SERVICE_NAME}.conf
                    initctl start ${SERVICE_NAME}
                fi
                ;;
            7)
                if [ "$SERVICE_CMD_ONLY" = true ]; then
                    echo "systemctl start ${SERVICE_NAME}"
                else
                    pre_install_actions
                    render_template script_templates/systemd_sync_gateway.tpl > /usr/lib/systemd/system/${SERVICE_NAME}.service
                    systemctl enable ${SERVICE_NAME}
                    systemctl start ${SERVICE_NAME}
                fi
                ;;
            *)
                echo "ERROR: Unsupported RedHat/CentOS Version \"$VER\""
                usage
                exit 1
                ;;
        esac
        ;;
    Darwin)
        if [ "$SERVICE_CMD_ONLY" = true ]; then
            echo "launchctl start /Library/LaunchDaemons/com.couchbase.mobile.sync_gateway.plist"
        else
            pre_install_actions
            render_template script_templates/com.couchbase.mobile.sync_gateway.plist > /Library/LaunchDaemons/com.couchbase.mobile.sync_gateway.plist
            launchctl load /Library/LaunchDaemons/com.couchbase.mobile.sync_gateway.plist
        fi
        ;;
    *)
        echo "ERROR: unknown OS \"$OS\""
        usage
        exit 1
        ;;
esac

