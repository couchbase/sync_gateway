#!/usr/bin/env python3
# Copyright 2026-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included
# in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
# in that file, in accordance with the Business Source License, use of this
# software will be governed by the Apache License, Version 2.0, included in
# the file licenses/APL2.txt.

# /// script
# requires-python = ">=3.10"
# ///

"""Build the Couchbase Lite test server that testing/cbltestclient drives.

The test server is a real Couchbase Lite C application from couchbaselabs/couchbase-lite-tests
that exposes its database and replicator over HTTP.  Sync Gateway's tests talk to it instead of
linking libcblite, so they need one built and installed where testing/cbltestclient looks.

No prebuilt test server is published publicly - latestbuilds.service.couchbase.com is only
reachable from the Couchbase network - so this clones the test repository and runs its CMake
build, which downloads the public Enterprise Edition Couchbase Lite package.  That costs a few
minutes and a large download, which is why it lives here rather than inside `go test`.
"""

import argparse
import os
import platform
import shutil
import subprocess
import sys
from pathlib import Path

# The default must match cbltestclient.DefaultCBLVersion.
DEFAULT_CBL_VERSION = "4.1.2"

TESTS_REPO_URL = "https://github.com/couchbaselabs/couchbase-lite-tests.git"
# Tracking main's tip rather than a fixed commit picks up test server fixes as they land.  The
# resolved commit goes in the stamp file, so a moved tip still rebuilds.
TESTS_REPO_REF = "main"

# Written next to the installed server to record what it was built from, so a rebuild can be
# skipped when nothing has changed.
STAMP_FILE_NAME = ".stamp"


def log(message: str) -> None:
    print(f"==> {message}", file=sys.stderr, flush=True)


def run(command: list[str], cwd: Path | None = None) -> None:
    log(" ".join(str(part) for part in command))
    # Build output goes to stderr with the log, keeping stdout for what --print-ref prints.
    subprocess.run(command, cwd=cwd, check=True, stdout=sys.stderr)


def goos() -> str:
    """Return the GOOS the current interpreter is running on, to match Go's cache layout."""
    system = platform.system()
    if system == "Darwin":
        return "darwin"
    if system == "Linux":
        return "linux"
    raise RuntimeError(f"Unsupported platform {system}")


def goarch() -> str:
    """Return the GOARCH the current interpreter is running on, to match Go's cache layout."""
    machine = platform.machine().lower()
    if machine in ("x86_64", "amd64"):
        return "amd64"
    if machine in ("arm64", "aarch64"):
        return "arm64"
    raise RuntimeError(f"Unsupported architecture {machine}")


def default_install_dir(version: str) -> Path:
    """Return the directory testing/cbltestclient looks in, matching cbltestclient.CacheDir."""
    root = os.environ.get("SG_TEST_CBL_TEST_SERVER_DIR")
    if root:
        base = Path(root)
    elif goos() == "darwin":
        base = Path.home() / "Library" / "Caches" / "sync_gateway" / "cbl-test-server"
    else:
        base = (
            Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
            / "sync_gateway"
            / "cbl-test-server"
        )
    return base / version / f"{goos()}-{goarch()}"


def stamp_value(version: str, commit: str) -> str:
    return f"{version} {commit}"


def git(args: list[str], cwd: Path) -> None:
    """Run a git command with the LFS smudge filter off.

    The datasets are stored in Git LFS and the test server build does not need them, so skipping
    the filter avoids pulling tens of megabytes of .cblite2 archives.
    """
    run(
        ["git", "-c", "filter.lfs.smudge=", "-c", "filter.lfs.required=false", *args],
        cwd=cwd,
    )


def resolve_ref(ref: str) -> str | None:
    """Return the commit ref points at, or None if the remote cannot be reached.

    This is what makes a moving ref safe to track: the commit goes into the stamp file, so a tip
    that has moved rebuilds and one that has not does nothing.  Being unable to resolve it is not
    fatal - an offline machine with a server already installed should keep using it.
    """
    try:
        result = subprocess.run(
            ["git", "ls-remote", TESTS_REPO_URL, ref],
            check=True,
            capture_output=True,
            text=True,
        )
    except (subprocess.CalledProcessError, OSError) as error:
        log(f"Could not resolve {ref} in {TESTS_REPO_URL}: {error}")
        return None
    if not result.stdout.strip():
        raise RuntimeError(f"{ref} does not exist in {TESTS_REPO_URL}")
    return result.stdout.split()[0]


def checkout_repo(work_dir: Path, ref: str) -> Path:
    """Return the couchbase-lite-tests checkout to build from, fetching ref if needed."""
    checkout = work_dir / "couchbase-lite-tests"
    if not (checkout / ".git").exists():
        checkout.mkdir(parents=True, exist_ok=True)
        run(["git", "init", "--quiet"], cwd=checkout)
        run(["git", "remote", "add", "origin", TESTS_REPO_URL], cwd=checkout)
    git(["fetch", "--depth", "1", "origin", ref], cwd=checkout)
    git(["checkout", "--quiet", "FETCH_HEAD"], cwd=checkout)
    return checkout


def download_cbl(server_dir: Path, version: str) -> None:
    """Download the public Enterprise Edition Couchbase Lite package the test server links against.

    Build number 0 tells the download script to take the public release from packages.couchbase.com
    rather than an internal CI build that only the Couchbase network can reach.  Enterprise is not
    optional: Sync Gateway only tests against Enterprise Edition.
    """
    # download_cbl.sh copies the unpacked package into servers/c/lib without creating it first, and
    # the directory is not tracked, so a fresh checkout fails without this.
    (server_dir / "lib").mkdir(parents=True, exist_ok=True)

    # download_cbl.sh names macOS "macos" rather than Go's "darwin", and silently downloads
    # nothing for a platform name it does not know.
    platform_name = "macos" if goos() == "darwin" else goos()
    run(
        [
            str(server_dir / "scripts" / "download_cbl.sh"),
            platform_name,
            "enterprise",
            version,
            "0",
        ],
        cwd=server_dir,
    )


def cbl_library_files(server_dir: Path) -> list[Path]:
    """Return the Couchbase Lite shared libraries that have to sit next to the executable."""
    lib_dir = server_dir / "lib" / "libcblite"
    if goos() == "darwin":
        return sorted((lib_dir / "lib").glob("libcblite*.dylib"))
    # The Linux package puts the library under a per-architecture triplet directory.
    return sorted(lib_dir.glob("lib/*/libcblite.so*"))


def build(checkout: Path, version: str) -> Path:
    """Build the C test server and return the directory holding the built executable.

    This drives CMake directly rather than through servers/c/scripts/build_*.sh, which take
    different arguments per platform.
    """
    server_dir = checkout / "servers" / "c"
    download_cbl(server_dir, version)

    build_dir = server_dir / "build"
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True)

    run(
        ["cmake", f"-DCBL_VERSION={version}", "-DCMAKE_BUILD_TYPE=Release", ".."],
        cwd=build_dir,
    )
    run(
        [
            "cmake",
            "--build",
            ".",
            "--target",
            "install",
            "--parallel",
            str(os.cpu_count() or 1),
        ],
        cwd=build_dir,
    )

    bin_dir = build_dir / "out" / "bin"
    libraries = cbl_library_files(server_dir)
    if not libraries:
        raise RuntimeError(
            f"No Couchbase Lite shared library found under {server_dir / 'lib' / 'libcblite'}"
        )
    for library in libraries:
        destination = bin_dir / library.name
        # CMake's install step may already have placed the library here, and copying a symlink over
        # an existing entry fails rather than replacing it
        destination.unlink(missing_ok=True)
        # copy rather than resolve the symlinks in the package: the executable finds the versioned
        # library through the unversioned name, so both have to be present
        shutil.copy2(library, destination, follow_symlinks=False)
    return bin_dir


def install(
    built_bin_dir: Path, assets_dir: Path, install_dir: Path, version: str, commit: str
) -> None:
    """Install the built server into the layout testing/cbltestclient expects.

    The test server resolves its assets as "<executable dir>/../assets", so the executable goes in
    a bin subdirectory with the assets beside it rather than inside it.
    """
    if install_dir.exists():
        shutil.rmtree(install_dir)
    (install_dir / "bin").mkdir(parents=True)
    shutil.copytree(built_bin_dir, install_dir / "bin", dirs_exist_ok=True)
    shutil.copytree(assets_dir, install_dir / "assets", dirs_exist_ok=True)
    (install_dir / STAMP_FILE_NAME).write_text(stamp_value(version, commit))


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--cbl-version",
        default=DEFAULT_CBL_VERSION,
        help="Couchbase Lite version to build against",
    )
    parser.add_argument(
        "--ref",
        default=os.environ.get("SG_TEST_CBL_TESTS_REF", TESTS_REPO_REF),
        help=f"Branch, tag or commit of couchbase-lite-tests to build (default: {TESTS_REPO_REF})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rebuild even if the installed server is already up to date",
    )
    parser.add_argument(
        "--print-ref",
        action="store_true",
        help="Print the couchbase-lite-tests ref that would be built, and do nothing else",
    )
    args = parser.parse_args()

    # Lets a caller key a cache on what the ref resolves to without this script owning that
    # decision, which is what CI does.
    if args.print_ref:
        print(args.ref)
        return

    install_dir = default_install_dir(args.cbl_version)
    executable = install_dir / "bin" / "testserver"

    stamp = install_dir / STAMP_FILE_NAME
    installed = executable.exists() and stamp.exists()

    commit = resolve_ref(args.ref)
    if commit is None:
        if not installed:
            raise RuntimeError(
                f"Cannot reach {TESTS_REPO_URL} to resolve {args.ref}, and no test server is "
                f"installed at {executable}"
            )
        log(f"Keeping the installed test server at {executable}")
        return

    up_to_date = (
        not args.force
        and installed
        and stamp.read_text() == stamp_value(args.cbl_version, commit)
    )
    if up_to_date:
        log(
            f"Couchbase Lite {args.cbl_version} test server already installed at {executable} "
            f"({args.ref} is {commit[:12]})"
        )
    else:
        work_dir = install_dir.parent / "src"
        work_dir.mkdir(parents=True, exist_ok=True)
        checkout = checkout_repo(work_dir, args.ref)
        built_bin_dir = build(checkout, args.cbl_version)
        install(
            built_bin_dir,
            checkout / "servers" / "c" / "assets",
            install_dir,
            args.cbl_version,
            commit,
        )
        log(f"Installed Couchbase Lite {args.cbl_version} test server at {executable}")


if __name__ == "__main__":
    main()
