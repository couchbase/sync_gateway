#!/bin/sh

# Copyright 2015-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# Set default values
OS=""
VER=""
SERVICE_NAME="sync_gateway"
# Determine the absolute path of the installation directory
# $( dirname "$0" ) get the directory containing this script
# if we are already in the containing directory we will get '.'
# && pwd will prepend the current directory if the path is relative
# this will result in an absolute path
# Note this line is evaluated not executed in the current shell so
# the current directory is not changed by the 'cd' command
SCRIPT_DIR="$(cd "$(dirname "$0")" >/dev/null && pwd)"
INSTALL_DIR="$(dirname "${SCRIPT_DIR}")"
SRCCFGDIR=${INSTALL_DIR}/examples
SRCCFG=serviceconfig.json
RUNAS_TEMPLATE_VAR=sync_gateway
PIDFILE_TEMPLATE_VAR=/var/run/sync-gateway.pid
RUNBASE_TEMPLATE_VAR=/home/sync_gateway
GATEWAY_TEMPLATE_VAR=${INSTALL_DIR}/bin/sync_gateway
CONFIG_TEMPLATE_VAR=${RUNBASE_TEMPLATE_VAR}/sync_gateway.json
LOGS_TEMPLATE_VAR=${RUNBASE_TEMPLATE_VAR}/logs
SERVICE_CMD_ONLY=false

usage() {
  echo "This script upgrades a service to run a sync_gateway instance."
}

ostype() {
  if [ -x "$(command -v lsb_release)" ]; then
    OS=$(lsb_release -si)
    VER=$(lsb_release -sr)
  elif [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$(echo "${ID}")
    if [ "${OS}" = "debian" ]; then
      VER=$(cat /etc/debian_version)
    else
      VER=$VERSION_ID
    fi
  elif [ -f /etc/redhat-release ]; then
    OS=rhel
    VER=$(cat /etc/redhat-release | sed s/.*release\ // | sed s/\ .*//)
  elif [ -f /etc/system-release ]; then
    OS=rhel
    VER=5.0
  else
    OS=$(uname -s)
    VER=$(uname -r)
  fi

  OS=$(echo "${OS}" | tr "[:upper:]" "[:lower:]")
  OS_MAJOR_VERSION=$(echo $VER | sed 's/\..*$//')
  OS_MINOR_VERSION=$(echo $VER | sed s/[0-9]*\.//)
}

#
#script starts here
#

#Figure out the OS type of the current system
ostype

#If the OS is MAC OSX, set the default user account home path to /Users/sync_gateway
if [ "$OS" = "darwin" ]; then
  RUNBASE_TEMPLATE_VAR=/Users/sync_gateway
  CONFIG_TEMPLATE_VAR=${RUNBASE_TEMPLATE_VAR}/sync_gateway.json
  LOGS_TEMPLATE_VAR=${RUNBASE_TEMPLATE_VAR}/logs
fi

# Make sure we are running with root privilages
if [ $(id -u) != 0 ]; then
  echo "This script should be run as root." >/dev/stderr
  exit 1
fi

#Install the service for the specific platform
case $OS in
debian)
  case 1:${OS_MAJOR_VERSION:--} in
  $((OS_MAJOR_VERSION >= 8))*)
    systemctl stop ${SERVICE_NAME}
    systemctl start ${SERVICE_NAME}
    ;;
  esac
  ;;
ubuntu)
  case 1:${OS_MAJOR_VERSION:--} in
  $((OS_MAJOR_VERSION >= 16))*)
    systemctl stop ${SERVICE_NAME}
    systemctl start ${SERVICE_NAME}
    ;;
  $((OS_MAJOR_VERSION >= 12))*)
    service ${SERVICE_NAME} stop
    service ${SERVICE_NAME} start
    ;;
  *)
    echo "ERROR: Unsupported Ubuntu Version \"$VER\""
    usage
    exit 1
    ;;
  esac
  ;;
redhat* | rhel* | centos | ol | rocky | almalinux )
  case 1:${OS_MAJOR_VERSION:--} in
  $((OS_MAJOR_VERSION >= 7))*)
    systemctl stop ${SERVICE_NAME}
    systemctl start ${SERVICE_NAME}
    ;;
  *)
    echo "ERROR: Unsupported RedHat/CentOS/Rocky/Alma Version \"$VER\""
    usage
    exit 1
    ;;
  esac
  ;;
amzn*)
  case 1:${OS_MAJOR_VERSION:--} in
  $((OS_MAJOR_VERSION >= 2))*)
    systemctl stop ${SERVICE_NAME}
    systemctl start ${SERVICE_NAME}
    ;;
  *)
    echo "ERROR: Unsupported Amazon Linux Version \"$VER\""
    usage
    exit 1
    ;;
  esac
  ;;
darwin)
  launchctl unload /Library/LaunchDaemons/com.couchbase.mobile.sync_gateway.plist
  launchctl load /Library/LaunchDaemons/com.couchbase.mobile.sync_gateway.plist
  ;;
*)
  echo "ERROR: unknown OS \"$OS\""
  usage
  exit 1
  ;;
esac
