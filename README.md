# Couchbase Sync Gateway

Gluing TouchDB (and CouchDB) to Couchbase Server


## About

This is an **experimental prototype** adapter that can allow Couchbase Server 2 to act as a replication endpoint for TouchDB, CouchDB and other compatible libraries like PouchDB. It does this by running an HTTP listener process that speaks enough of CouchDB's REST API to serve as a passive endpoint of replication, and using a Couchbase bucket as the persistent storage of all the documents.

### Limitations

* Sync Gateway currently supports only a _single_ database. Its name defaults to the name of the underlying bucket, but can be changed using the `dbname` command-line flag. Any attempt to use the CouchDB REST API to access or create other databases, or delete the existing one, will fail with an appropriate HTTP error.
* Document IDs longer than about 180 characters will overflow Couchbase's key size limit and cause an HTTP error.
* Explicit garbage collection is required to free up space, via a REST call to `/_vacuum`. This is not yet scheduled automatically, so you'll have to call it yourself.
* Performance is probably not that great. This is an unoptimized proof of concept.

### License

Apache 2 license, like all Couchbase stuff.



## How To Run It

### Setup

0. Install and start [Couchbase Server 2.0](http://www.couchbase.com) on localhost.
1. Create a bucket named `sync_gateway` in the default pool.
2. Install [Go](http://golang.org). Make sure you have version 1.0.3 or later.
3. Install [Mercurial](http://mercurial.selenic.com/downloads/).
4. `go get -u github.com/couchbaselabs/sync_gateway`

### Startup

The Sync Gateway launcher tool is `bin/sync_gateway` in the first directory in your GOPATH. If you've already added this directory to your PATH, you can just enter `sync_gateway` from a shell to run it.

You now have a sort of mock-CouchDB listening on port 4984. It definitely won't do everything CouchDB does, but you can tell another CouchDB-compatible database (including TouchDB) to replicate with it.

If you want to run Couchbase on a different host, or use a different name for the bucket, or listen on a different port, you can do that with command-line options. Use the `--help` flag to see a list of options.


## Channels

Channels are the intermediaries between documents and users. Every document belongs to a set of channels, and every user has a set of channels s/he is allowed to access. Additionally, a replication from Sync Gateway specifies what channels it wants to replicate; documents not in any of these channels will be ignored (even if the user has access to them.)

Thus, channels have three purposes:

1. Authorizing users to see documents;
2. Partitioning the data set;
3. Constraining the amount of data synced to (mobile) clients.

There is no need to register or preassign channels. Channels come into existence as documents are assigned to them. Channels with no documents assigned are considered empty.

Valid channel names consist of Unicode letter and digit characters, as well as "_", "-" and ".". The empty string is not allowed. The special meta-channel name "*" denotes all channels. Channel names are compared literally, so they are case- and diacritical-sensitive.

### Mapping documents to channels

There are currently two ways to assign documents to channels. Both of these operate implicitly: there's not a separate action that assigns a doc to a channel, rather the contents of the document determine what channels its in.

#### Explicit property

The default (simple and limited) way is to add a `channels` property to a document. Its value is an array of strings. The strings are the names of channels that this document will be available through. A document with no `channels` property will not appear in any channels.

#### Mapping function

The more flexible way is to define a channelmap function. This is a JavaScript function, similar to a "map", that takes a document body as input and can decide based on that what channels it should go into. Like a regular map function, it may not reference any external state and it must return the same results every time it's called on the same input.

The channelmap function goes in a design document with ID `_design/channels`, in a property named `channelmap`.

To add the current document to a channel, the function should call the special function `sync` which takes one or more channel names (or arrays of channel names) as arguments. For convenience, `sync` ignores `null` or `undefined` argument values.

Defining a channelmap overrides the default channel mapping mechanism; that is, the `channels` property will be ignored.

The default mechanism is equivalent to the following simple channelmap:

    function (doc) { sync(doc.channels); }

### Replicating channels to CouchDB or TouchDB

The basics are simple: When pulling from Sync Gateway using the CouchDB API, configure the replication to use a filter named `sync_gateway/bychannel`, and a filter parameter `channels` whose value is a comma-separated list of channels to subscribe to. The replication will now only pull documents tagged with those channels.

#### Removal from channels

There is a tricky edge case of a document being "removed" from a channel without being deleted, i.e. when a new revision is not added to one or more channels that the previous revision was in. Subscribers (downstream databases pulling from this db) should know about this change, but it's not exactly the same as a deletion. CouchDB's existing filtered-replication mechanism does not address this, which has made things difficult for some clients.

Sync Gateway's `_changes` feed includes one more revision of a document after it stops matching a channel. It adds a `removed` property to the entry where this happens. (No client yet recognizes this property, though.) The value of `removed` is an array of strings, each string naming a channel this revision no longer appears in.

The effect on the client will be that after a replication it sees the next revision of the document, the one that causes it to no longer match the channel(s). It won't get any further revisions until the next one that makes the document match again.

This could seem weird ("why am I downloading documents I don't need?") but it ensures that any views running in the client will correctly no longer include the document, instead of including an obsolete revision. If the app code uses views to filter instead of just assuming that all docs in its local db must be relevant, it should be fine.

## Authentication & Authorization

Sync Gateway supports user accounts that are allowed to access only a subset of channels.

### Accounts

Accounts are managed through a parallel REST interface that runs on port 4985 (by default, but this can be customized via the `authaddr` command-line argument). This interface is privileged and for internal use only; instead, we assume you have some other server-side mechanism for users to manage accounts, which will call through to this API.

The URL for a user account is simply "/user/_name_" where _name_ is the username. The typical GET, PUT and DELETE methods apply. The contents of the resource are a JSON object with the properties:

* "name": The user name (same as in the URL path). Names must consist of alphanumeric ASCII characters or underscores.
* "admin_channels": An array of strings -- the channels that the user is granted access to by the administrator. The name "*" means "all channels". An empty array or missing property denies access to all channels.
* "all_channels": Like "admin_channels" but also includes channels the user is given access to by other documents via a channel-mapper function. (This is a derived property and changes to it will be ignored.)
* "password": In a PUT or POST request, put the user's password here. It will not be returned by a GET.
* "passwordhash": Securely hashed version of the password. This will be returned from a GET. If you want to update a user without changing the password, leave this alone when sending the modified JSON object back through PUT.
* "disabled": Normally missing; if set to `true`, disables access for that account.
* "email": The user's email address. Optional, but BrowserID login (q.v.) needs it.

You can create a new user either with a PUT to its URL, or by POST to `/user/`.

There is a special account named `GUEST` that applies to unauthenticated requests. Any request to the public API that does not have an `Authorization` header is treated as the `GUEST` user. The default `channels` property of the guest user is `["*"]`, which gives access to all channels. In other words, it's the equivalent of CouchDB's "admin party". If you want any channels to be read-protected, you'll need to change this first.

To disable all guest access, set the guest user's `disabled` property:

    curl -X PUT localhost:4985/user/GUEST --data '{"disabled":true, "channels":[]}'

### Authentication

[Like CouchDB](http://wiki.apache.org/couchdb/Session_API), Sync Gateway allows clients to authenticate using either HTTP Basic Auth or cookie-based sessions.

#### BrowserID

Sync Gateway also supports [Mozilla's BrowserID (aka Persona) protocol](https://developer.mozilla.org/en-US/docs/persona) that allows users to log in using their email addresses.

To enable it, you need to add a `-site` command-line argument when starting the server, to tell it what its canonical URL is; for example:

    sync_gateway -site=http://example.com/

(This is necessary because the BrowserID protocol requires both client and server to agree on this name, and there's no reliable way to derive it on the server, especially if it's behind a proxy.)

Once that's set up, you need to set the `email` property of user accounts, so that the server can determine the account based on the email address.

Clients log in the same way they would to a CouchDB server running the [BrowserID plugin](https://github.com/iriscouch/browserid_couchdb): by POSTing to `/_browserid`, with a JSON body containing an `assertion` property whose value is the signed assertion received from the identity provider. Just as with a `_session` login, the response will set a session cookie.

#### Indirect Authentication

An app server can also create a session for a user by POSTing to `/_session` on the privileged port-4985 REST interface. The request body should be a JSON document with two properties: `name` (the user name) and `ttl` time-to-live measured in seconds. The response will be a JSON document with properties `cookie_name` (the name of the session cookie the client should send), `session_id` (the value of the session cookie) and `expires` (the time the session expires).

This allows the app server to optionally do its own authentication: the client sends credentials to it, and then it uses this API to generate a login session and send the cookie data back to the client, which can then send it in requests to Sync Gateway.

### Authorization

The `all_channels` property of a user account determines what channels that user may access.

Any GET request to a document not assigned to one or more of the user's available channels will fail with a 403.

Accessing a `_changes` feed with any channel names that are not available to the user will fail with a 403.

There is not yet any _write_ protection; this is TBD. It's going to be conceptually trickier because there are definitely use cases where you create or update documents that are tagged with channels you don't have access to. We may just need a separate validation function as in CouchDB.

There is currently an edge case where after a user is granted access to a new channel, their client will not automatically sync with pre-existing documents in that channel (that they didn't have access to before.) The workaround is for the client to do a one-shot sync from only that new channel, which having no checkpoint will start over from the beginning and fetch all the old documents.

#### Programmatic Authorization

It is possible for documents to grant users access to channels. A typical example is a document representing a shared resource (like a chat room or photo gallery), which has a property like `members` that lists the users who should have access to that resource. If the documents belonging to that resource are all tagged with a specific channel, then a channel-map function can be used to detect the membership property and assign access to the users listed in it:

	function(doc) {
		if (doc.type == "chatroom") {
			access(doc.channel_id, doc.members)
		}
	}

In this example, a chat room is represented by a document with a "type" property "chatroom". The "channel_id" property names the associated channel (with which the actual chat messages will be tagged), and the "members" property lists the users who have access.


## Schema

Unfortunately there isn't a simple one-to-one mapping between CouchDB and Couchbase documents, since Couchbase lacks support for multiple revisions.

### Documents

A CouchDB document is represented by a Couchbase document whose ID is `doc:` followed by the CouchDB document ID. The contents look like:

    { "id": "docid", "rev": "1-currentrevid",
      "sequence": 1234
      "history": {"revs": [...], "parents": [...], "keys": [...], "deleted": [...]} }

`id` and `rev` are the IDs of the document and of its current (winning) revision. If the current revision is a deletion, there is also a `deleted` property whose value is `true`.

`sequence` is the sequence number assigned to the latest change made to this document. This is only used for generating the view for the `_changes` feed.

`history` is the revision tree, encoded as parallel arrays: `revs` is an array of revision IDs, and `parents` is an array of integers. For each revision in `revs`, the corresponding element of `parents` is the index of its parent revision (or -1 if the revision has no parent.) The `keys` array gives the key under which that revision's contents are stored in Couchbase. The optional `deleted` array is not parallel with the others; it's just an array of indexes of revisions that are deletions.

### Local Documents

Local documents have an ID prefix of `ldoc:`. The contents are the same as the CouchDB properties, including the specisl `_rev` property that stores the current revision ID.

### Revisions

The contents of a CouchDB document revision are stored in a Couchbase document whose ID starts with `rev:`. The rest of the ID is the revision key as found in the `keys` array of a document's `history` object. This key is in practice a base64'd SHA-1 digest of the revision's JSON.

The contents of a revision document are simply the contents of that revision. For maximum reuse, the `_id` and `_rev` properties are not included, although `_deleted` and `_attachments` are.

Note that revisions are a _content-addressable store_, in that the document key is derived from the contents. This allows revisions with the same contents to be stored only once, saving space. This is especially important for use cases like Syncpoint, where a document may be replicated into large numbers of databases. As long as all the databases are in the same bucket, each revision of the document will only be stored once. However, an explicit garbage collection is required to locate and delete revisions that are no longer referred to by any document.

### Attachments

Revisions store attachment metadata in stubbed-out form, with a `"stub":true` property and no data. The `digest` property in the metadata is used to look up an attachment body.

An attachment's body is stored in a Couchbase document whose ID is `att:` followed by the attachment metadata's `digest` property. (Attachments are therefore another content-addressable store, with a different namespace, and have the same benefit that a specific file will only ever be stored once no matter how many documents it's attached to.)

An attachment document's body is _not_ JSON. It's simply the raw binary contents. The metadata of an attachment, such as name and MIME type, lives in the `_attachments` property of a revision that refers to it.
