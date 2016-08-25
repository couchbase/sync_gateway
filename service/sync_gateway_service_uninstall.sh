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


usage()
{
    echo "This script removes an init service to run a sync_gateway instance."
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

#Install the service for the specific platform
case $OS in
    Debian)
        case $OS_MAJOR_VERSION in
            8) 
                systemctl stop ${SERVICE_NAME}
                systemctl disable ${SERVICE_NAME}
                
                if [ -f /usr/lib/systemd/system/${SERVICE_NAME}.service ]; then
                	rm /usr/lib/systemd/system/${SERVICE_NAME}.service
                fi
                ;;
         esac
    ;;
    Ubuntu)
        case $OS_MAJOR_VERSION in
            12|14)
		service ${SERVICE_NAME} stop
                if [ -f /etc/init/${SERVICE_NAME}.conf ]; then
                	rm /etc/init/${SERVICE_NAME}.conf
                fi
                ;;
            16)
                systemctl stop ${SERVICE_NAME}
                systemctl disable ${SERVICE_NAME}
                
                if [ -f /lib/systemd/system/${SERVICE_NAME}.service ]; then
                	rm /lib/systemd/system/${SERVICE_NAME}.service
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
                PATH=/usr/kerberos/sbin:/usr/kerberos/bin:/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin:/root/bin
                service ${SERVICE_NAME} stop
                chkconfig ${SERVICE_NAME} off
                chkconfig --del ${SERVICE_NAME}
                
                if [ -f /etc/init.d/${SERVICE_NAME} ]; then
                	rm /etc/init.d/${SERVICE_NAME}
                fi
                ;;
            6)
                initctl stop ${SERVICE_NAME}
                if [ -f /etc/init/${SERVICE_NAME}.conf ]; then
                	rm /etc/init/${SERVICE_NAME}.conf
                fi
                ;;
            7)
                systemctl stop ${SERVICE_NAME}
                systemctl disable ${SERVICE_NAME}
                
                if [ -f /usr/lib/systemd/system/${SERVICE_NAME}.service ]; then
                	rm /usr/lib/systemd/system/${SERVICE_NAME}.service
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
        launchctl unload /Library/LaunchDaemons/com.couchbase.mobile.sync_gateway.plist
        if [ -f /Library/LaunchDaemons/com.couchbase.mobile.sync_gateway.plist ]; then
        	rm /Library/LaunchDaemons/com.couchbase.mobile.sync_gateway.plist
        fi
        ;;
    *)
        echo "ERROR: unknown OS \"$OS\""
        usage
        exit 1
        ;;
esac

