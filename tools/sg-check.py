#!/usr/bin/python
#
# sg-check.py - Performs various consistency checks against a running
#               Sync Gateway's public REST API. It will also provide
#               some useful summary statics, including channel counts
#               for those using the `channel(doc.channels)` style.
#
# Author:
#     Zachary Gramana  <zack@couchbase.com>
#
# Copyright (c) 2016 Couchbase, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the License at
#
# http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions
# and limitations under the License.
#
import ijson # `pip install ijson` if you cannot load this module.
import sys
from urllib2 import urlopen

previous = 0
current = 0
skips = 0

if len(sys.argv) == 1:
    print("You must pass either a URL to a SG database, e.g. http://foo.com/db, or a path to a directory containing 'all_docs.json' and 'changes.json'")
    sys.exit(1)

useHttp = sys.argv[1].startswith('http')

if useHttp:
    sgRoot = sys.argv[1]
    alldocsPath = sgRoot + '/_all_docs?include_docs=true&revs=true&update_seq=true'
    allDocs = urlopen(alldocsPath)
else:
    jsonRoot = sys.argv[1]
    alldocsPath = jsonRoot + "/all_docs.json"
    allDocs = open(alldocsPath) 

print('Getting ' + alldocsPath)

parser = ijson.parse(allDocs)
all_seqs = []
all_channels = { 'unassigned': 0 }
update_seq = ''

inDoc = False
hasChannels = False

for prefix, event, value in parser:
    if (prefix, event) == ('rows.item.update_seq', 'number'):
        all_seqs.append(value)
    if (prefix, event) == ('rows.item.doc', 'start_map'):
        inDoc = True
        hasChannels = False
    if (prefix, event) == ('rows.item.doc', 'end_map'):
        inDoc = False
        if not hasChannels:
            all_channels['unassigned'] += 1        
    elif prefix.endswith('rows.item.doc.channels.item'):
        hasChannels = True
        if value in all_channels:
            all_channels[value] += 1
        else:
            all_channels[value] = 1
    elif prefix == 'update_seq':
        update_seq = str(value)

all_seqs.sort()

if useHttp:
    httpRoot = sys.argv[1]
    changesPath = httpRoot + '/_changes?feed=longpoll&heartbeat=300000&style=all_docs&since=&include_docs=true'
    jsonFile = urlopen(changesPath)
else:
    jsonRoot = sys.argv[1]
    changesPath = jsonRoot + "/changes.json"
    jsonFile = open(changesPath)

print('Getting ' + changesPath)

parser = ijson.parse(jsonFile)
changes_seqs = []
changes_channels = { 'unassigned': 0 }
last_seq = ''
deletes = []
user_docs = []

for prefix, event, value in parser:
    if (prefix, event) == ('results.item.seq', 'number'):
        changes_seqs.append(value)
        current = value
    if (prefix, event) == ('results.item.id', 'string'):
        if str(value).startswith('_user/'):
            user_docs.append(current)
    if (prefix, event) == ('results.item.deleted', 'boolean'):
        deletes.append(current)
    if (prefix, event) == ('results.item.doc', 'start_map'):
        inDoc = True
        hasChannels = False
    if (prefix, event) == ('results.item.doc', 'end_map'):
        inDoc = False
        if not hasChannels:
            changes_channels['unassigned'] += 1        
    elif prefix.endswith('results.item.doc.channels.item'):
        hasChannels = True
        if value in changes_channels:
            changes_channels[value] += 1
        else:
            changes_channels[value] = 1
    elif prefix == 'last_seq':
        last_seq = str(value)

changes_seqs.sort()
deletes.sort()

print('\r\n_all_docs returned ' + str(len(all_seqs)) + ' rows') 
print('_changes  returned ' + str(len(all_seqs)) + ' results') 

for value in all_seqs:
    if not value in changes_seqs:
        print('all_docs seq #'  + str(value) + 'not found in _changes')

for value in changes_seqs:
    if not value in deletes and not value in user_docs and not value in all_seqs:
        print('_changes seq #' + str(value) + ' not found in all_docs')

print('\r\nupdate_seq: ' + update_seq)
print('last_seq:   ' + last_seq)

print('\r\nall_docs channel counts:')
for key in sorted(all_channels.keys()):
    if (key == "unassigned"):
        print('')
    print(' ' + key + ': ' + str(all_channels[key]))

delCount = len(deletes)

print('\r\nchanges channel counts:')
for key in sorted(changes_channels.keys()):
    if (key == "unassigned"):
        print('\r\n ' + key + ': ' + str(changes_channels[key] - delCount)) #+ '(' + str() + ')')
    else:
        print(' ' + key + ': ' + str(changes_channels[key]))

print('\r\ntotal user doc sequences: ' + str(len(user_docs)))
print('total tombstones: ' + str(delCount) + '\r\n')
