# BaseCouch

Gluing CouchDB to Couchbase Server

This is a prototype implementation of an adapter that can allow Couchbase Server 2 to act as a replication endpoint for CouchDB and compatible libraries like TouchDB and PouchDB. It does this by running an HTTP listener that speaks enough of CouchDB's REST API to serve as a passive endpoint of replication, and using a Couchbase bucket as the persistent storage of all the documents.

## Current Status

As of October 4 2012, BaseCouch:

* Supports being pushed to (but not yet pull)
* Doesn't support conflicts; it will panic if asked to store a revision that branches the tree

## Schema

Unfortunately there isn't a simple one-to-one mapping between CouchDB and Couchbase documents, since Couchbase lacks support for multiple revisions.

### Database

A CouchDB database is represented by a Couchbase document whose ID is `cdb:`_name_ where _name_ is the name of the database. The contents look like:

    { "name": "database-name", "docPrefix": "doc:database-name/ABCDEF:" }

The `docPrefix` property is the prefix string applied to CouchDB document IDs to store them in Couchbase. It always consists of `doc:` followed by the database name, a `/`, a random UUID, and a ":". (The UUID ensures that if a database is deleted but some documents are left behind, and later a database is created with the same name, the orphaned documents won't appear in it.)

### Document

A CouchDB document is represented by a Couchbase document whose ID is the database's `docPrefix` followed by the CouchDB document ID. The contents look like:

    { "history": {"revs": [...], "parents": [...]},
      "current": { ...document body... } }

`current` contains the current revision's body, including the special `_id` and `_rev` properties, and a `_deleted` property if the document's been deleted.

`history` is the revision tree, encoded as two parallel arrays: `revs` is an array of revision IDs, and `parents` is an array of integers. For each revision in `revs`, the corresponding element of `parents` is the index of its parent revision (or -1 if the revision has no parent.)

There it not yet any storage of the non-current revisions; their bodies are lost when the document is updated. This works alright for purposes of replication as long as there aren't any conflicts. I'll be remedying this in the near future.