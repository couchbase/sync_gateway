#!/usr/bin/env python3
# Copyright 2024-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

# /// script
# requires-python = ">=3.10"
# ///

"""Start a Couchbase Server cluster using cbdinocluster."""

import argparse
import os
import re
import shlex
import subprocess
import sys
import tempfile
import textwrap

CBDINOCLUSTER = "github.com/couchbaselabs/cbdinocluster@latest"
DEFAULT_CBS_VERSION = "8.0.1"
DEFAULT_SERVICES = "kv,n1ql,index"
DEFAULT_MEMORY_MB = 3072
DEFAULT_NODES = 1
# Tracks the cluster this script last allocated from a given working directory, so repeated
# local invocations (e.g. re-running tests) reuse the running cluster instead of allocating a
# new one every time.
STATE_FILE_NAME = ".cbdinocluster-sg-cluster-id"
# Written next to the state file unless --env-file says otherwise, so that a plain invocation
# leaves something sourceable behind rather than only printing the exports to stdout.
DEFAULT_ENV_FILE_NAME = "cbs.env"
# cbdinocluster keeps its configuration in a single file in the user's home directory, written by
# 'cbdinocluster init'. Every other subcommand fails to load its config until that file exists.
CBDINOCLUSTER_CONFIG_PATH = os.path.join(os.path.expanduser("~"), ".cbdinocluster")


def run(args: list[str], **kwargs) -> subprocess.CompletedProcess:
    print(f"+ {' '.join(args)}", flush=True)
    return subprocess.run(args, check=True, **kwargs)


def ensure_initialized() -> None:
    """Run 'cbdinocluster init' if it hasn't been run before, since no other subcommand works
    without the config file it writes. Existing configuration is left alone."""
    if os.path.exists(CBDINOCLUSTER_CONFIG_PATH):
        return
    print(
        f"{CBDINOCLUSTER_CONFIG_PATH} not found, initializing cbdinocluster for docker",
        flush=True,
    )
    # Sync Gateway only ever uses the docker deployer, so skip the cloud/k8s providers - they
    # default to enabled under --auto and would otherwise probe for credentials we don't have.
    run(
        [
            "go",
            "run",
            CBDINOCLUSTER,
            "init",
            "--auto",
            "--disable-k8s",
            "--disable-capella",
            "--disable-aws",
            "--disable-azure",
            "--disable-gcp",
            "--disable-dns",
        ]
    )


def find_reusable_cluster(version: str, nodes: int, services: str) -> str | None:
    """Return the cluster ID recorded in the cwd's state file, if it's still running and
    matches the requested version/node count/services. Otherwise return None."""
    state_path = os.path.join(os.getcwd(), STATE_FILE_NAME)
    if not os.path.exists(state_path):
        return None

    with open(state_path) as f:
        cluster_id = f.read().strip()
    if not cluster_id:
        return None

    result = subprocess.run(
        ["go", "run", CBDINOCLUSTER, "get-definition", cluster_id],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        if "not found" in stderr.lower():
            print(
                f"Cluster {cluster_id} recorded in {state_path} is no longer running; "
                "allocating a new one",
                flush=True,
            )
            return None
        # Unknown failure - don't assume the cluster is gone and silently allocate a duplicate.
        raise RuntimeError(
            f"Failed to check status of cluster {cluster_id} recorded in {state_path}: "
            f"{stderr or 'get-definition failed with no stderr output'}"
        )

    definition = result.stdout
    version_match = re.search(r"version:\s*(\S+)", definition)
    nodes_match = re.search(r"count:\s*(\S+)", definition)
    services_match = re.search(r"services:\s*\[([^\]]*)\]", definition)
    requested_services = {s.strip() for s in services.split(",") if s.strip()}
    existing_services = (
        {s.strip() for s in services_match.group(1).split(",") if s.strip()}
        if services_match
        else None
    )
    if (
        version_match is None
        or nodes_match is None
        or existing_services is None
        or version_match.group(1) != version
        or nodes_match.group(1) != str(nodes)
        or existing_services != requested_services
    ):
        print(
            f"Cluster {cluster_id} recorded in {state_path} doesn't match the requested "
            f"version={version}/nodes={nodes}/services={sorted(requested_services)}; "
            "allocating a new one",
            flush=True,
        )
        return None

    print(f"Reusing existing cluster {cluster_id} recorded in {state_path}", flush=True)
    return cluster_id


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Start a Couchbase Server cluster via cbdinocluster."
    )
    parser.add_argument(
        "--version",
        default=os.environ.get("COUCHBASE_SERVER_VERSION", DEFAULT_CBS_VERSION),
        help="Couchbase Server version to deploy "
        f"(default: ${{COUCHBASE_SERVER_VERSION}} or {DEFAULT_CBS_VERSION})",
    )
    parser.add_argument(
        "--nodes",
        type=int,
        default=int(os.environ.get("COUCHBASE_NUM_NODES", DEFAULT_NODES)),
        help="Number of nodes in the cluster, all running the same services "
        f"(default: $COUCHBASE_NUM_NODES or {DEFAULT_NODES})",
    )
    parser.add_argument(
        "--purpose",
        default=os.environ.get("CBDINOCLUSTER_PURPOSE", ""),
        help="Optional purpose label for the cluster (default: $CBDINOCLUSTER_PURPOSE)",
    )
    parser.add_argument(
        "--services",
        default=os.environ.get("COUCHBASE_SERVICES", DEFAULT_SERVICES),
        help="Comma-separated list of services to run on each node "
        f"(default: $COUCHBASE_SERVICES or {DEFAULT_SERVICES})",
    )
    parser.add_argument(
        "--kv-memory-mb",
        type=int,
        default=int(os.environ.get("COUCHBASE_KV_MEMORY_MB", DEFAULT_MEMORY_MB)),
        help="KV service memory quota in MB "
        f"(default: $COUCHBASE_KV_MEMORY_MB or {DEFAULT_MEMORY_MB})",
    )
    parser.add_argument(
        "--index-memory-mb",
        type=int,
        default=int(os.environ.get("COUCHBASE_INDEX_MEMORY_MB", DEFAULT_MEMORY_MB)),
        help="Index service memory quota in MB "
        f"(default: $COUCHBASE_INDEX_MEMORY_MB or {DEFAULT_MEMORY_MB})",
    )
    parser.add_argument(
        "--tls",
        action="store_true",
        help="Request a TLS (couchbases://) connection string instead of the default couchbase://",
    )
    parser.add_argument(
        "--env-file",
        default=os.path.join(os.getcwd(), DEFAULT_ENV_FILE_NAME),
        help="Path to write shell-sourceable 'export SG_TEST_COUCHBASE_SERVER_URL=...' and "
        f"'export CBS_CLUSTER_ID=...' lines to (default: ./{DEFAULT_ENV_FILE_NAME})",
    )
    opts = parser.parse_args()

    ensure_initialized()

    state_path = os.path.join(os.getcwd(), STATE_FILE_NAME)
    cluster_id = find_reusable_cluster(opts.version, opts.nodes, opts.services)

    if cluster_id is None:
        services = ", ".join(s.strip() for s in opts.services.split(",") if s.strip())

        # A version containing '/' is a full docker image reference (e.g.
        # ghcr.io/cb-vanilla/server:8.1.0), not a plain version string - pull it directly via
        # docker.image instead of letting cbdinocluster resolve 'version' against its registries.
        node_docker_block = ""
        if "/" in opts.version:
            node_docker_block = f"    docker:\n      image: {opts.version}\n"

        yaml_content = (
            textwrap.dedent(f"""\
                nodes:
                  - count: {opts.nodes}
                    version: {opts.version}
                    services: [{services}]
            """)
            + node_docker_block
            + textwrap.dedent(f"""\
                docker:
                    kv-memory: {opts.kv_memory_mb}
                    index-memory: {opts.index_memory_mb}
            """)
        )

        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False, prefix="cbdino-sg-"
        ) as f:
            f.write(yaml_content)
            def_file = f.name

        print(f"Cluster definition written to {def_file}:", flush=True)
        print(yaml_content, flush=True)

        allocate_cmd = ["go", "run", CBDINOCLUSTER, "allocate", "--def-file", def_file]
        if opts.purpose:
            allocate_cmd += ["--purpose", opts.purpose]

        try:
            result = run(allocate_cmd, stdout=subprocess.PIPE, text=True)
        finally:
            os.remove(def_file)
        cluster_id = result.stdout.strip()
        print(f"Cluster ID: {cluster_id}", flush=True)

        with open(state_path, "w") as f:
            f.write(cluster_id + "\n")
        print(f"Wrote cluster ID to {state_path}", flush=True)

    connstr_cmd = ["go", "run", CBDINOCLUSTER, "connstr", cluster_id]
    connstr_cmd += ["--tls"] if opts.tls else ["--no-tls"]
    connstr_result = run(
        connstr_cmd,
        stdout=subprocess.PIPE,
        text=True,
    )
    connstr = connstr_result.stdout.strip()
    print(f"Connection string: {connstr}", flush=True)

    # Connection strings can contain characters the shell would interpret (e.g. ?/& in query
    # params), so quote the values to keep the output safe to source.
    env_lines = [
        f"export SG_TEST_COUCHBASE_SERVER_URL={shlex.quote(connstr)}",
        f"export CBS_CLUSTER_ID={shlex.quote(cluster_id)}",
    ]

    print("\nExport for tests:")
    for line in env_lines:
        print(f"  {line}", flush=True)

    with open(opts.env_file, "w") as f:
        f.write("\n".join(env_lines) + "\n")
    print(f"\nWrote connection details to {opts.env_file}", flush=True)


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as e:
        print(f"Error: command failed with exit code {e.returncode}", file=sys.stderr)
        sys.exit(e.returncode)
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
