#!/usr/bin/env python

# Copyright 2016-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import shutil
import subprocess
import os
import xml.etree.ElementTree as ET
import sys
import urllib2
import argparse

"""

This will reset your local development environment to the sync gateway commit hash
specified in the argument.

WARNING: make sure all of your commits are pushed up to github before running this!  It may rollback local
git repositories if you snap to a sync gateway version that points to commits earlier than you have locally,
and then you will have to look into the git reflog to try to recover them, which is a pain.  To be safe,
back up your entire sync_gateway directory before running this.

Before running this, you should run:

    $ repo status > repo_status.txt
    $ repo info > repo_info.txt

in case there is a bug, these will be useful to have.

Usage:

    ./snap-manifest --sg-commit <commit-hash>

    or

    ./snap-manifest --sg-commit <commit-hash> --local-manifest /path/to/sg/repo/manifest/default.xml

This script will:

1. Download the manifest/default.xml file from the Sync Gateway github repo, or use the local file passed in
2. Modify the manifest to set the Sync Gateway commit passed in
3. Write the modified manifest to ./repo/manifest.xml
4. Run repo sync -d

"""

def repo_sync():
    """
    Run "repo sync", which will do all of the heavy lifting to get the 
    dependencies "snapped" / "syncd" to the versions specified in the manifest xml
    """
    print("Running repo sync -d")
    subprocess.call(['repo', 'sync', '-d'])  # TODO: does this need a subshell for any reason?


def update_sg_version(manifest_xml_content, commit):
    # modify xml according to parameters
    root = manifest_xml_content.getroot()
    for element in root:
        if element.get("name") == "sync_gateway":
            element.set("revision", commit)
    return manifest_xml_content

def get_manifest_xml_from_sg_github(commit):
    manifest_url="https://raw.githubusercontent.com/couchbase/sync_gateway/{}/manifest/default.xml".format(commit)
    print("Fetching manifest from: {}".format(manifest_url))
    response = urllib2.urlopen(manifest_url)
    return ET.ElementTree(file=response)

def prepare_repo_dir():
    """
    $ cd .repo
    $ rm manifest.xml
    $ cd manifests
    $ git reset --hard
    $ cd ../..
    """
    initial_directory = os.getcwd()
    os.chdir(".repo")
    if os.path.exists("manifest.xml"):
        os.remove("manifest.xml")
    os.chdir("manifests")
    subprocess.call(['git', 'reset', '--hard'])
    os.chdir(initial_directory)


def get_local_manifest_xml(manifest_file):
    if not os.path.exists(manifest_file):
        raise Exception("Manifest file does not exist: {}".format(manifest_file))
    return ET.ElementTree(file=open(manifest_file))

if __name__=="__main__":

    """

    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--sg-commit", help="The Sync Gateway commit or branch name to snap to.  Will pull from github unless --local-manifest is specified", required=True)
    parser.add_argument("--local-manifest", help="The path to a local manifest to snap to.  In this case will pull locally rather than going out to github", required=False)
    args = parser.parse_args()

    # Deletes existing .repo/manifest.xml and runs "git reset --hard" in .repo/manifests
    prepare_repo_dir()

    manifest_xml_content = "error"
    print("args.local_manifest: {}".format(args.local_manifest))
    if args.local_manifest != None and args.local_manifest != "":
        manifest_xml_content = get_local_manifest_xml(args.local_manifest)
    else:
        manifest_xml_content = get_manifest_xml_from_sg_github(args.sg_commit)

    versioned_manifest_xml_content = update_sg_version(manifest_xml_content, args.sg_commit)

    # Write to dest file
    dest_path = ".repo/manifest.xml"
    print("Write manifest to {}".format(dest_path))
    destfile = open(dest_path, 'w')
    versioned_manifest_xml_content.write(destfile)
    destfile.close()

    # Run repo sync
    repo_sync()
