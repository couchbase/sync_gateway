#!/usr/bin/env python

import shutil
import subprocess
import os


# This script is  related to buiilding sync gateway from source via the `repo` tool,
# which is a tool to deal with multiple git repos based on a manifest XML file which
# pins dependencies to certain versions.
#
# This script has the ability to "snap" all of your dependencies into place,
# according to the versions specified in your manifest file

def discover_product():
    """
    Open ./repo/manifest.xml and figure out what the project name is:

    <project name="sync-gateway-accel"..>

    or 

    <project name="sync_gateway"..>

    """
    
    # TODO
    return "sync_gateway"


def repo_sync():
    """
    Run "repo sync", which will do all of the heavy lifting to get the 
    dependencies "snapped" / "syncd" to the versions specified in the manifest xml
    """
    subprocess.call(['repo', 'sync'])  # TODO: does this need a subshell for any reason?

def copy_modified_manifest(product_repo_commit, source_manifest_path, dest_path):
    """

    """

    # TODO: modify the XML to set the product version
    print "repo commit: {}".format(product_repo_commit)
    
    shutil.copy(source_manifest_path, dest_path)

def discover_product_repo_commit(source_manifest_path):

    """
    Go to that directory and 
    """
    cur_dir = os.getcwd()
    git_revision_hash = "n/a"
    
    try:
        source_manifest_dir = os.path.dirname(source_manifest_path)
        os.chdir(source_manifest_dir)
        git_revision_hash = get_git_revision_hash()
    finally:
        os.chdir(cur_dir)

    return git_revision_hash

def get_git_revision_hash():
    return subprocess.check_output(['git', 'rev-parse', 'HEAD'])
    
    
if __name__=="__main__":

    # TODO: args parsing

    manifest_locations = {
        "sync_gateway": "godeps/src/github.com/couchbase/sync_gateway/manifest/default.xml",
        "sync-gateway-accel": "godeps/src/github.com/couchbaselabs/sync-gateway-accel/manifest/default.xml",
    }
    
    product = discover_product()

    # Find the manifest.xml in the cloned repo (sync gateway or sg accel)    
    source_manifest_path = manifest_locations[product]

    product_repo_commit = discover_product_repo_commit(source_manifest_path)
    
    # Copy it to the ./repo/manifest.xml file
    copy_modified_manifest(
        product_repo_commit,
        source_manifest_path,
        ".repo/manifest.xml"
    )

    # Run repo sync
    # DISALBE repo_sync()
