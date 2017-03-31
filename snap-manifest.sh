#!/usr/bin/env python

import shutil
import subprocess
import os
import xml.etree.ElementTree as ET
import sys
import urllib2

"""

This will reset your local development environment to the sync gateway commit hash
specified in the argument.

WARNING: make sure all of your commits are pushed up to github before running this!  It may rollback local
git repositories if you snap to a sync gateway version that points to commits earlier than you have locally,
and then you will have to look into the git reflog to try to recover them, which is a pain.  To be safe,
back up your entire sync_gateway directory before running this.

Before running this, you should run:

    $ repo status

and make sure it returns:

    nothing to commit (working directory clean)

Usage:

    $ ./snap-manifest.sh sync-gateway-commit-hash

This will:

1. Download the manifest/default.xml file from the Sync Gateway github repo
2. Modify the manifest to set the Sync Gateway commit passed in
3. Write the modified manifest to ./repo/manifest.xml
4. Run repo sync

"""

def repo_sync():
    """
    Run "repo sync", which will do all of the heavy lifting to get the 
    dependencies "snapped" / "syncd" to the versions specified in the manifest xml
    """
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
    os.remove("manifest.xml")
    os.chdir("manifests")
    subprocess.call(['git', 'reset', '--hard'])
    os.chdir(initial_directory)


if __name__=="__main__":

    if len(sys.argv) <= 1:
        raise Exception("Usage: ./snap-manifest.sh sync-gateway-commit-hash")

    prepare_repo_dir()

    commit = sys.argv[1]
    manifest_xml_content = get_manifest_xml_from_sg_github(commit)
    versioned_manifest_xml_content = update_sg_version(manifest_xml_content, commit)

    # Write to dest file
    dest_path = ".repo/manifest.xml"
    destfile = open(dest_path, 'w')
    versioned_manifest_xml_content.write(destfile)
    destfile.close()

    # Run repo sync
    repo_sync()
