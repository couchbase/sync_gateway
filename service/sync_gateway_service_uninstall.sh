#!/bin/sh

# Copyright 2015-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# Set default values
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
PLATFORM="$(uname)"

usage() {
  echo "This script removes a systemd or launchctl service to run a sync_gateway instance."
}

#
#script starts here
#

#If the OS is MAC OSX, set the default user account home path to /Users/sync_gateway
if [ "$PLATFORM" = "Darwin" ]; then
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
case $PLATFORM in
Linux)
    systemctl stop ${SERVICE_NAME}
    systemctl disable ${SERVICE_NAME}

    if [ -f /usr/lib/systemd/system/${SERVICE_NAME}.service ]; then
      rm /usr/lib/systemd/system/${SERVICE_NAME}.service
    fi
    ;;
Darwin)
  launchctl unload /Library/LaunchDaemons/com.couchbase.mobile.sync_gateway.plist
  if [ -f /Library/LaunchDaemons/com.couchbase.mobile.sync_gateway.plist ]; then
    rm /Library/LaunchDaemons/com.couchbase.mobile.sync_gateway.plist
  fi
  ;;
*)
  echo "ERROR: unknown platform \"$PLATFORM\""
  usage
  exit 1
  ;;
esac
