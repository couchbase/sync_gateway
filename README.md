# BaseCouch

Gluing CouchDB to Couchbase Server

This is an **experimental prototype** adapter that can allow Couchbase Server 2 to act as a replication endpoint for CouchDB and compatible libraries like TouchDB and PouchDB. It does this by running an HTTP listener that speaks enough of CouchDB's REST API to serve as a passive endpoint of replication, and using a Couchbase bucket as the persistent storage of all the documents.

* [Watch me give a brief presentation about BaseCouch](https://plus.google.com/117619707007719365626/posts/1Vuz3b8crXm) at our sprint demo session from 10/25/12. 
* ...or just [view or download the slides](https://speakerdeck.com/snej/basecouch).

## Current Status

As of October 25 2012, BaseCouch:

* Supports both push and pull.
* Supports revision trees and conflicts.
* Supports attachments (including MIME multipart PUT/GET).

Limitations:

* Document IDs longer than about 180 characters will overflow Couchbase's key size limit and cause an HTTP error.
* Explicit garbage collection is required to free up space, via a REST call to `/_vacuum`. This is not yet scheduled automatically, so you'll have to call it yourself.
* There is no `_compact` implementation yet, so obsolete revisions are never deleted.
* No access control: it's admin party 24/7!
* Performance is probably not that great. This is an unoptimized proof of concept.

## License

Apache 2 license, like all Couchbase stuff.

## How To Run It

### Setup

0. Install and start [Couchbase Server 2.0](http://www.couchbase.com) on localhost.
1. Create a bucket named `couchdb` in the default pool.
1. Install [Go](http://golang.org).
2. `go get -u github.com/couchbaselabs/basecouch`

### Startup

3. `cd` to the first directory in your `$GOPATH`, i.e. the location you set up to store downloaded Go packages.
4. `cd src/github.com/couchbaselabs/basecouch`
5. `go run util/main.go`

You now have a sort of mock-CouchDB listening on port 4984. It definitely won't do everything CouchDB does, but you can tell another CouchDB-compatible server to replicate with it.

If you want to run Couchbase on a different host, or use a different name for the bucket, or listen on a different port, you can do that with command-line options to `main.go`. Use the `--help` flag to see a list of options.

## Schema

Unfortunately there isn't a simple one-to-one mapping between CouchDB and Couchbase documents, since Couchbase lacks support for multiple revisions.

### Database

All CouchDB databases live in a single bucket. A CouchDB database is represented by a Couchbase document whose ID is `cdb:`_name_ where _name_ is the name of the database. The contents look like:

    { "name": "database-name", "docPrefix": "doc:database-name/ABCDEF:" }

The `docPrefix` property is the prefix string applied to CouchDB document IDs to store them in Couchbase. It always consists of `doc:` followed by the database name, a `/`, a random UUID, and a ":". (The UUID ensures that if a database is deleted but some documents are left behind, and later a database is created with the same name, the orphaned documents won't appear in it.)

There's also a document named `seq:`_name_ which is used as an atomic counter for generating sequence IDs for that database.

### Document

A CouchDB document is represented by a Couchbase document whose ID is the database's `docPrefix` followed by the CouchDB document ID. The contents look like:

    { "id": "docid", "rev": "1-currentrevid",
      "sequence": 1234
      "history": {"revs": [...], "parents": [...], "keys": [...], "deleted": [...]} }

`id` and `rev` are the IDs of the document and of its current (winning) revision. If the current revision is a deletion, there is also a `deleted` property whose value is `true`.

`sequence` is the sequence number assigned to the latest change made to this document. This is only used for generating the view for the `_changes` feed.

`history` is the revision tree, encoded as parallel arrays: `revs` is an array of revision IDs, and `parents` is an array of integers. For each revision in `revs`, the corresponding element of `parents` is the index of its parent revision (or -1 if the revision has no parent.) The `keys` array gives the key under which that revision's contents are stored in Couchbase. The optional `deleted` array is not parallel with the others; it's just an array of indexes of revisions that are deletions.

### Revision

A CouchDB document is represented by a Couchbase document whose ID starts with `rev:`. The rest of the ID is the revision key as found in the `keys` array of a document's `history` object. This key is in practice a base64'd SHA-1 digest of the revision's JSON.

The contents of a revision document are simply the contents of that revision. For maximum reuse, the `_id` and `_rev` properties are not included, although `_deleted` and `_attachments` are.

Note that revisions are a content-addressable store, in that the document key is derived from the contents. This allows revisions with the same contents to be stored only once, saving space. This is especially important for use cases like Syncpoint, where a document may be replicated into large numbers of databases. As long as all the databases are in the same bucket, each revision of the document will only be stored once. However, an explicit garbage collection is required to locate and delete revisions that are no longer referred to by any document.

### Attachment

Revisions store attachment metadata in stubbed-out form, with a `"stub":true` property and no data. The `digest` property in the metadata is used to look up an attachment body.

An attachment's body is stored in a Couchbase document whose ID is `att:` followed by the attachment metadata's `digest` property. (Attachments are therefore another content-addressable store, with a different namespace, and have the same benefit that a specific file will only ever be stored once no matter how many documents it's attached to.)

An attachment document's body is _not_ JSON. It's simply the raw binary contents. The metadata of an attachment, such as name and MIME type, lives in the `_attachments` property of a revision that refers to it.

## Crazy Ideas

This schema seems like an efficient way to implement a server for mobile clients, a la Syncpoint, where every user has their own database on the server that contains a subset of the entire data set. If the user data sets overlap, which seems common if they're all drawn from a central data store, the content-addressable sharing of revisions and attachments between documents means a lot less space usage, and less data to copy during replication. The main thing that needs to be implemented for this is a local-only replicator, i.e. a command to say "replicate database A to database B". Since both A and B are in the same bucket, this would simply involve copying rev-tree changes from one document to another.

If you replace the calls to the Couchbase Server API with equivalent calls to CouchStore, you have the beginnings of a lightweight local CouchDB-compatible database, something like TouchDB. This shouldn't be hard, because the basic key-value get/put operations are identical, and the usage of views for finding all documents and for by-sequence lookups is already available directly in CouchStore via key range enumeration and the by-sequence index. (The hardest part would be implementing the rest [sic] of the CouchDB API, especially views. You'd probably need to create a separate CouchStore database for every view to serve as its index.)
