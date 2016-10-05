#!/usr/bin/env python

import shutil
import subprocess
import os
import xml.etree.ElementTree as ET

# This script is  related to buiilding sync gateway from source via the `repo` tool,
# which is a tool to deal with multiple git repos based on a manifest XML file which
# pins dependencies to certain versions.
#
# This script has the ability to "snap" all of your dependencies into place,
# according to the versions specified in your manifest file

def discover_product_from_repo_manifest():
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

def copy_modified_manifest(product, product_repo_commit, source_manifest_path, dest_path):
    """
    This will copy the manifest from source_manifest_path to dest_path, but
    modify it along the way to update:

    <project name="sync_gateway"/>

    to

    <project name="sync_gateway" revision="82493418e" />
 
    To stamp it with the particular revision corresponding to the current repo commit
    """

    sourcefile = open(source_manifest_path)
    tree = ET.ElementTree(file=sourcefile)

    # modify xml according to parameters
    root = tree.getroot()
    for element in root:
        if element.get("name") == product:
            element.set("revision", product_repo_commit)
           
    # write modified xml to stdout
    destfile = open(dest_path, 'w')
    tree.write(destfile)
    
    destfile.close()
    sourcefile.close()

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

    return git_revision_hash.strip()

def get_git_revision_hash():
    return subprocess.check_output(['git', 'rev-parse', 'HEAD'])
    
    
if __name__=="__main__":

    manifest_locations = {
        "sync_gateway": "godeps/src/github.com/couchbase/sync_gateway/manifest/default.xml",
        "sync-gateway-accel": "godeps/src/github.com/couchbaselabs/sync-gateway-accel/manifest/default.xml",
    }

    # Discover the product based on what's in .repo/manifest.xml
    product = discover_product_from_repo_manifest()

    # Find the manifest.xml in the cloned repo (sync gateway or sg accel)    
    source_manifest_path = manifest_locations[product]

    product_repo_commit = discover_product_repo_commit(source_manifest_path)
    
    # Copy it to the ./repo/manifest.xml file
    copy_modified_manifest(
        product,
        product_repo_commit,
        source_manifest_path,
        ".repo/manifest.xml"
    )

    # Run repo sync
    repo_sync()
