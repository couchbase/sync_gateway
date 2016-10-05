#!/usr/bin/env python

import shutil
import subprocess

# This "snaps" all of your dependencies into place, according to the versions specified in your manifest file

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

if __name__=="__main__":

    # TODO: args parsing

    manifest_locations = {
        "sync_gateway": "godeps/src/github.com/couchbase/sync_gateway/manifest/default.xml",
        "sync-gateway-accel": "godeps/src/github.com/couchbaselabs/sync-gateway-accel/manifest/default.xml",
    }
    
    product = discover_product()

    # Find the manifest.xml in the cloned repo (sync gateway or sg accel)    
    source_manifest_path = manifest_locations[product]

    # Copy it to the ./repo/manifest.xml file
    shutil.copy(source_manifest_path, ".repo")

    # Run repo sync
    repo_sync()
