#!/bin/sh

# Set default values
OS=""
VER=""
SERVICE_NAME="sync_gateway"
SRCCFGDIR=../examples
SRCCFG=admin_party.json
RUNAS_TEMPLATE_VAR=sync_gateway
RUNBASE_TEMPLATE_VAR=/home/sync_gateway
PIDFILE_TEMPLATE_VAR=/var/run/sync-gateway.pid
GATEWAYROOT_TEMPLATE_VAR=/opt/couchbase-sync-gateway
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
    echo "\t--srccfgdir : The path to the source sync_gateway JSON config directory)"
    echo "\t--srccfgname : The name of the source sync_gateway JSON config file)"
    echo "\t--cfgpath : The path to the sync_gateway JSON config file; default (/home/sync_gateway/sync_gateway.json)"
    echo "\t--logsdir : The path to the log file direcotry; default (/home/sync_gateway/logs)"
    echo "\t--servicename : The name of the service to install; default (sync_gateway)"
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

#script start here
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
        --srccfgdir)
            SRCCFGDIR=$VALUE
            ;;
        --srccfgname)
            SRCCFG=$VALUE
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
    shift
done

# If OS and VER were not provided on the command line get them for current env
if  ["$OS" = ""] && ["$VER" = ""] ; then
    ostype
fi

# Check that runtime user account exists
if [ "$OS" != "Darwin" ]; then
    if [ -z `id -u $RUNAS_TEMPLATE_VAR 2>/dev/null` ]; then
        echo "The sync_gateway runtime user account does not exist \"$RUNAS_TEMPLATE_VAR\"." > /dev/stderr
        exit 1
    fi
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

#Install the service for the specific platform
case $OS in
    Ubuntu)
        case $OS_MAJOR_VERSION in
            10|12|14)
                render_template script_templates/upstart_ubuntu_sync_gateway.tpl > /etc/init/${SERVICE_NAME}.conf
                cp $SRCCFGDIR/$SRCCFG $CONFIG_TEMPLATE_VAR
                service ${SERVICE_NAME} start
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
                render_template script_templates/sysv_sync_gateway.tpl > /etc/init.d/${SERVICE_NAME}
                chmod 755 /etc/init.d/${SERVICE_NAME}
                cp $SRCCFGDIR/$SRCCFG $CONFIG_TEMPLATE_VAR
                PATH=/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin
                chkconfig --add ${SERVICE_NAME}
                chkconfig ${SERVICE_NAME} on
                service ${SERVICE_NAME} start
                ;;
            6)
                render_template script_templates/upstart_redhat_sync_gateway.tpl > /etc/init/${SERVICE_NAME}.conf
                cp $SRCCFGDIR/$SRCCFG $CONFIG_TEMPLATE_VAR
                initctl start ${SERVICE_NAME}
                ;;
            7)
                render_template script_templates/systemd_sync_gateway.tpl > /usr/lib/systemd/system/${SERVICE_NAME}.service
                cp $SRCCFGDIR/$SRCCFG $CONFIG_TEMPLATE_VAR
                systemctl enable ${SERVICE_NAME}
                systemctl start ${SERVICE_NAME}
                ;;
            *)
                echo "ERROR: Unsupported RedHat/CentOS Version \"$VER\""
                usage
                exit 1
                ;;
        esac
        ;;
    Darwin)
        render_template script_templates/com.couchbase.mobile.sync_gateway.plist > /Library/LaunchDaemons/com.couchbase.mobile.sync_gateway.plist
        launchctl load /Library/LaunchDaemons/com.couchbase.mobile.sync_gateway.plist
        ;;
    *)
        echo "ERROR: unknown OS \"$OS\""
        usage
        exit 1
        ;;
esac

