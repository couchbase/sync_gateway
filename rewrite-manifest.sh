#!/usr/bin/env python

# Copyright 2016-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

# This script updates the manifest created from a 'repo init -u <url>'
# command with a *different* manifest, presumably before 'repo sync'
# has been run.
#
# This is called from bootstrap.sh, but can also be used standalone.
#
# The purpose is to build from a manifest on a feature branch, for example
# as part of validating a github pull request.
#
# Here are the actions performed:
#
# 1. Fetches manifest from the given url
# 2. Updates the given project revision to match the given commit
# 3. Emits modified manifest to stdout
#
# Usage:
#
#     rewrite-manifest --manifest-url http://yourwebsite.co/manifest.xml --project-name your-project --set-revision new-sha > .repo/manifest.xml
#
# If your existing manifest contained a project entry like:
#
#     <project name="your-project" path="somepath" remote="someremote"/>
#
# the above command would modify it to be:
#
#     <project name="your-project" path="somepath" revision="new-sha" remote="someremote"/>
#
# and emit it to stdout, which you can redirect to overwrite your .repo/manifest.xml file

import optparse
import xml.etree.ElementTree as ET
try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen
import sys

def parse_args():
    """
    Parse command line args and return a tuple
    """
    parser = optparse.OptionParser()
    parser.add_option('-u', '--manifest-url', help='Manifest URL')
    parser.add_option('-p', '--project-name', help="Project name to modify revision")
    parser.add_option('-s', '--set-revision', help="SHA hash of revision to modify project specified via --project-name")
    (opts, args) = parser.parse_args()
    return (parser, opts.manifest_url, opts.project_name, opts.set_revision)

def validate_args(parser, manifest_url, project_name, set_revision):
    """
    Make sure all required args are passed, or else print usage
    """
    if manifest_url is None:
        parser.print_help()
        exit(-1)
    if project_name is None:
        parser.print_help()
        exit(-1)
    if set_revision is None:
        parser.print_help()
        exit(-1)

if __name__=="__main__":

   # get arguments
   (parser, manifest_url, project_name, set_revision) = parse_args()

   # validate arguments
   validate_args(parser, manifest_url, project_name, set_revision)

   # fetch manifest content and parse xml
   tree = ET.ElementTree(file=urlopen(manifest_url))

   # modify xml according to parameters
   root = tree.getroot()
   for element in root:
       if element.get("name") == project_name:
           element.set("revision", set_revision)

   # write modified xml to stdout
   if sys.version_info >= (3, 0):
       tree.write(sys.stdout.buffer)
   else:
       tree.write(sys.stdout)
   sys.stdout.write("\n") # trailing newline
