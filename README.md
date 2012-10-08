# BaseCouch

Gluing CouchDB to Couchbase Server

This is a prototype implementation of an adapter that can allow Couchbase Server 2 to act as a replication endpoint for CouchDB and compatible libraries like TouchDB and PouchDB. It does this by running an HTTP listener that speaks enough of CouchDB's REST API to serve as a passive endpoint of replication, and using a Couchbase bucket as the persistent storage of all the documents.

## Current Status

As of October 8 2012, BaseCouch:

* Supports both push and pull, although pull hasn't been tested as much.
* Doesn't support attachments.

## Schema

Unfortunately there isn't a simple one-to-one mapping between CouchDB and Couchbase documents, since Couchbase lacks support for multiple revisions.

### Database

A CouchDB database is represented by a Couchbase document whose ID is `cdb:`_name_ where _name_ is the name of the database. The contents look like:

    { "name": "database-name", "docPrefix": "doc:database-name/ABCDEF:" }

The `docPrefix` property is the prefix string applied to CouchDB document IDs to store them in Couchbase. It always consists of `doc:` followed by the database name, a `/`, a random UUID, and a ":". (The UUID ensures that if a database is deleted but some documents are left behind, and later a database is created with the same name, the orphaned documents won't appear in it.)

There's also a document named `cdb:`name`:nextsequence` which is used as an atomic counter for generating sequence IDs.

### Document

A CouchDB document is represented by a Couchbase document whose ID is the database's `docPrefix` followed by the CouchDB document ID. The contents look like:

    { "id": "docid", "rev": "1-currentrevid",
      "sequence": 1234
      "history": {"revs": [...], "parents": [...], "keys": [...], "deleted": [...]} }

`id` and `rev` are the IDs of the document and of its current (winning) revision. If the current revision is a deletion, there is also a `deleted` property whose value is `true`.

`sequence` is the sequence number assigned to the latest change made to this document. This is only used for generating the view for the `_changes` feed.

`history` is the revision tree, encoded as parallel arrays: `revs` is an array of revision IDs, and `parents` is an array of integers. For each revision in `revs`, the corresponding element of `parents` is the index of its parent revision (or -1 if the revision has no parent.) The `keys` array gives the key under which that revision's contents are stored in Couchbase. The optional `deleted` array is not parallel with the others; it's just an array of indexes of revisions that are deletions.

### Revision

A CouchDB document is represented by a Couchbase document whose ID starts with `rev:`. The rest of the ID is the revision key as found in the `keys` array of a document's `history` object. This key is in practice a hex SHA-1 digest of the revision's JSON.

The contents of a revision document are simply the contents of that revision. For maximum reuse, the `_id` and `_rev` properties are not included, although `_deleted` is.

Note that revisions are a content-addressable store, in that the document key is derived from the contents. This allows revisions with the same contents to be stored only once, saving space. This is especially important for use cases like Syncpoint, where a document may be replicated into large numbers of databases. As long as all the databases are in the same bucket, each revision of the document will only be stored once.
