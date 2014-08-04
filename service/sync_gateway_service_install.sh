#!/bin/sh

# Set default values
OS=""
VER=""
SERVICE_NAME="sync_gateway"
SRCSGCFG=../examples/admin_party.json
RUNAS_TEMPLATE_VAR=sync_gateway
RUNBASE_TEMPLATE_VAR=/home/sync_gateway
PIDFILE_TEMPLATE_VAR=/var/run/sync-gateway.pid
GATEWAY_TEMPLATE_VAR=/opt/couchbase-sync-gateway/bin/sync_gateway
CONFIG_TEMPLATE_VAR=/home/sync_gateway/sync_gateway.json
LOGS_TEMPLATE_VAR=/home/sync_gateway/logs


usage()
{
    echo "This script creates an init service to run a sync_gateway instance."
    echo "If you want to install more than one service instance"
    echo "create additional services with different names."
    echo ""
    echo "sync_gateway_service_install.sh"
    echo "\t-h --help"
    echo "\t--os : Manually set the target OS for the service <Ubuntu | Redhat >; default (auto detected)"
    echo "\t--ver : Manually set the target OS version; default (auto detected)"
    echo "\t--runas : The user account to run sync_gateway as; default (sync_gateway)"
    echo "\t--runbase : The directory to run sync_gateway from; defaut (/home/sync_gateway)"
    echo "\t--pidfile : The path where the .pid file will be created; default (/var/run/sync-gateway.pid)"
    echo "\t--sgpath : The path to the sync_gateway executable; default (/opt/couchbase-sync-gateway/bin/sync_gateway)"
    echo "\t--cfgpath : The path to the sync_gateway JSON config file; default (/home/sync_gateway/sync_gateway.json)"
    echo "\t--logsdir : The path to the log file direcotry; default (/home/sync_gateway/logs)"
    echo "\t--servicename : The name of the service to install; default (sync_gateway)"
    echo ""
}
 
ostype() {
    ARCH=$(uname -m | sed 's/x86_//;s/i[3-6]86/32/')

    if [ -f /etc/lsb-release ]; then
        . /etc/lsb-release
        OS=$DISTRIB_ID
        VER=$DISTRIB_RELEASE
    elif [ -f /etc/debian_version ]; then
        OS=Debian  # XXX or Ubuntu??
        VER=$(cat /etc/debian_version)
    elif [ -f /etc/redhat-release ]; then
        OS=RedHat
    else
        OS=$(uname -s)
        VER=$(uname -r)
    fi

    OS_MAJOR_VERSION=`echo $VER | sed s/\.[0-9]*$//`
    OS_MINOR_VERSION=`echo $VER | sed s/[0-9]*\.//`
}

# expand variables + preserve formatting
render_template() {
  eval "echo \"$(cat $1)\""
}

#script start here
#
if [ `id -u` != 0 ]; then
    echo "This script should be run as root." > /dev/stderr
    exit 1
fi

while [ "$1" != "" ]; do
    PARAM=`echo $1 | awk -F= '{print $1}'`
    VALUE=`echo $1 | awk -F= '{print $2}'`
    case $PARAM in
        -h | --help)
            usage
            exit
            ;;
        --os)
            OS=$VALUE
            ;;
        --ver)
            VER=$VALUE
            ;;
        --runas)
            RUNAS_TEMPLATE_VAR=$VALUE
            ;;
        --runbase)
            RUNBASE_TEMPLATE_VAR=$VALUE
            ;;
        --pidfile)
            PIDFILE_TEMPLATE_VAR=$VALUE
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
        --servicename)
            SERVICE_NAME=$VALUE
            ;;
        *)
            echo "ERROR: unknown parameter \"$PARAM\""
            usage
            exit 1
            ;;
    esac
    shift
done

if  ["$OS" = ""] && ["$VER" = ""] ; then
    ostype
fi

case $OS in
    Ubuntu)
        render_template script_templates/upstart_sync_gateway.tpl > /etc/init/${SERVICE_NAME}.conf
        cp $SRCSGCFG $CONFIG_TEMPLATE_VAR
        service ${SERVICE_NAME} start
        ;;
    RedHat)
        render_template script_templates/sysv_sync_gateway.tpl > /etc/init.d/${SERVICE_NAME}
        chkconfig ${SERVICE_NAME} --add
        chkconfig ${SERVICE_NAME} on
        service ${SERVICE_NAME} start
        ;;
    *)
        echo "ERROR: unknown OS \"$OS\""
        usage
        exit 1
        ;;
esac

