#!/bin/bash

# Copyright 2024-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

# This code is intneded to be run from within a docker container of a specific platform and runs a subset of the service scripts. The full service can not be validated since systemd does not work in docker contains. This is intended to run in /bin/sh to test dash environments on debian systems.

set -eux -o pipefail

SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")

cd ${SCRIPT_DIR}/../service

./sync_gateway_service_install.sh --servicecmd

# /etc/os-release doesn't exist on Darwin
if [ -f /etc/os-release ]; then
    . /etc/os-release
    case ${ID} in
        amzn)
            yum install -y shadow-utils systemd
            ;;
    esac

    groupadd -r sync_gateway
    useradd -g sync_gateway sync_gateway

    # bash would support export -f for a systemctl wrapper, but dash does not support exporting aliases or functions

    mkdir -p /tmp/systemctl_wrapper

    cat << 'EOF' > /tmp/systemctl_wrapper/systemctl
#!/bin/bash

set -eu -o pipefail

case ${1:-} in
start)
    echo "No-op systemctl start in docker, since we're not running systemd"
    ;;
stop)
    echo "No-op systemctl stop in docker, since we're not running systemd"
    ;;
*)
    echo "Running systemctl $@"
    command /usr/bin/systemctl "$@"
    ;;
esac
EOF

    chmod +x /tmp/systemctl_wrapper/systemctl

    export PATH=/tmp/systemctl_wrapper:$PATH
fi

./sync_gateway_service_install.sh
./sync_gateway_service_upgrade.sh
./sync_gateway_service_uninstall.sh

# test again with runas option
./sync_gateway_service_install.sh --runas=root
./sync_gateway_service_upgrade.sh
./sync_gateway_service_uninstall.sh

echo "Successful service test"
