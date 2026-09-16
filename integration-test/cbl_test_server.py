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
# Pinned so that a change to the test server's API or command line is something we opt into.  The
# --port flag this script relies on is not on main yet, so a local checkout has to be passed with
# --repo until it lands; update this and drop that workaround once it has.
TESTS_REPO_COMMIT = "675ea8d2dab8f8300e8b08714778d42b759cf34e"

# Written next to the installed server to record what it was built from, so a rebuild can be
# skipped when nothing has changed.
STAMP_FILE_NAME = ".stamp"


def log(message: str) -> None:
    print(f"==> {message}", file=sys.stderr, flush=True)


def run(command: list[str], cwd: Path | None = None) -> None:
    log(" ".join(str(part) for part in command))
    subprocess.run(command, cwd=cwd, check=True)


def goos() -> str:
    """Return the GOOS the current interpreter is running on, to match Go's cache layout."""
    system = platform.system()
    if system == "Darwin":
        return "darwin"
    if system == "Windows":
        return "windows"
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
    elif goos() == "windows":
        base = (
            Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
            / "sync_gateway"
            / "cbl-test-server"
        )
    else:
        base = (
            Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache"))
            / "sync_gateway"
            / "cbl-test-server"
        )
    return base / version / f"{goos()}-{goarch()}"


def stamp_value(version: str, commit: str) -> str:
    return f"{version} {commit}"


def checkout_repo(work_dir: Path, repo: Path | None) -> Path:
    """Return the couchbase-lite-tests checkout to build from, cloning the pinned commit if needed."""
    if repo is not None:
        log(f"Using existing couchbase-lite-tests checkout at {repo}")
        return repo

    checkout = work_dir / "couchbase-lite-tests"
    if not (checkout / ".git").exists():
        checkout.mkdir(parents=True, exist_ok=True)
        run(["git", "init", "--quiet"], cwd=checkout)
        run(["git", "remote", "add", "origin", TESTS_REPO_URL], cwd=checkout)
    # The datasets are stored in Git LFS and the test server build does not need them, so skip the
    # smudge filter rather than pulling tens of megabytes of .cblite2 archives.
    run(
        [
            "git",
            "-c",
            "filter.lfs.smudge=",
            "-c",
            "filter.lfs.required=false",
            "fetch",
            "--depth",
            "1",
            "origin",
            TESTS_REPO_COMMIT,
        ],
        cwd=checkout,
    )
    run(
        [
            "git",
            "-c",
            "filter.lfs.smudge=",
            "-c",
            "filter.lfs.required=false",
            "checkout",
            "--quiet",
            "FETCH_HEAD",
        ],
        cwd=checkout,
    )
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

    scripts = server_dir / "scripts"
    if goos() == "windows":
        run(
            [
                "powershell",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(scripts / "download_cbl.ps1"),
                "enterprise",
                version,
                "0",
            ],
            cwd=server_dir,
        )
    else:
        run(
            [str(scripts / "download_cbl.sh"), goos(), "enterprise", version, "0"],
            cwd=server_dir,
        )


def cbl_library_files(server_dir: Path) -> list[Path]:
    """Return the Couchbase Lite shared libraries that have to sit next to the executable."""
    lib_dir = server_dir / "lib" / "libcblite"
    if goos() == "windows":
        return sorted((lib_dir / "bin").glob("cblite.dll"))
    if goos() == "darwin":
        return sorted((lib_dir / "lib").glob("libcblite*.dylib"))
    # The Linux package puts the library under a per-architecture triplet directory.
    return sorted(lib_dir.glob("lib/*/libcblite.so*"))


def build(checkout: Path, version: str) -> Path:
    """Build the C test server and return the directory holding the built executable.

    This drives CMake directly rather than through servers/c/scripts/build_*.sh: those take
    different arguments per platform, and each ends by copying an assets directory from a path
    that does not exist, failing the whole script after a successful build.
    """
    server_dir = checkout / "servers" / "c"
    download_cbl(server_dir, version)

    build_dir = server_dir / "build"
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True)

    configure = ["cmake", f"-DCBL_VERSION={version}", "-DCMAKE_BUILD_TYPE=Release"]
    if goos() == "windows":
        configure += ["-G", "Visual Studio 17 2022", "-A", "x64"]
    run(configure + [".."], cwd=build_dir)

    build_command = [
        "cmake",
        "--build",
        ".",
        "--target",
        "install",
        "--parallel",
        str(os.cpu_count() or 1),
    ]
    if goos() == "windows":
        build_command += ["--config", "Release"]
    run(build_command, cwd=build_dir)

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
        "--out",
        type=Path,
        help="Directory to install into (default: the cache directory testing/cbltestclient reads)",
    )
    parser.add_argument(
        "--repo",
        type=Path,
        default=os.environ.get("SG_TEST_CBL_TESTS_REPO"),
        help="Build from an existing couchbase-lite-tests checkout instead of cloning the pinned commit",
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        help="Where to clone couchbase-lite-tests (default: alongside the install directory)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rebuild even if the installed server is already up to date",
    )
    parser.add_argument(
        "--print-path",
        action="store_true",
        help="Print the installed executable's path on stdout",
    )
    args = parser.parse_args()

    install_dir = args.out or default_install_dir(args.cbl_version)
    executable = (
        install_dir
        / "bin"
        / ("testserver.exe" if goos() == "windows" else "testserver")
    )

    commit = "local" if args.repo else TESTS_REPO_COMMIT
    stamp = install_dir / STAMP_FILE_NAME
    # A local checkout is rebuilt every time: its commit is whatever is checked out right now, so a
    # stamp saying "local" tells us nothing about whether it is current.
    up_to_date = (
        not args.force
        and not args.repo
        and executable.exists()
        and stamp.exists()
        and stamp.read_text() == stamp_value(args.cbl_version, commit)
    )
    if up_to_date:
        log(
            f"Couchbase Lite {args.cbl_version} test server already installed at {executable}"
        )
    else:
        work_dir = args.work_dir or install_dir.parent / "src"
        work_dir.mkdir(parents=True, exist_ok=True)
        checkout = checkout_repo(work_dir, args.repo)
        built_bin_dir = build(checkout, args.cbl_version)
        install(
            built_bin_dir,
            checkout / "servers" / "c" / "assets",
            install_dir,
            args.cbl_version,
            commit,
        )
        log(f"Installed Couchbase Lite {args.cbl_version} test server at {executable}")

    if args.print_path:
        print(executable)


if __name__ == "__main__":
    main()
