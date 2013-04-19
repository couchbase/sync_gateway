# Couchbase Sync Gateway

Gluing [Couchbase Lite][COUCHBASE_LITE] (and [TouchDB][TOUCHDB] / [CouchDB][COUCHDB]) to [Couchbase Server][COUCHBASE_SERVER]


## About

This is an adapter that can allow [Couchbase Server 2][COUCHBASE_SERVER]* to act as a replication endpoint for [Couchbase Lite][COUCHBASE_LITE], [TouchDB][TOUCHDB], [CouchDB][COUCHDB], and other compatible libraries like PouchDB. It does this by running an HTTP listener process that speaks enough of CouchDB's REST API to serve as a passive endpoint of replication, and using a Couchbase bucket as the persistent storage of all the documents.

It also provides a mechanism called _channels_ that makes it feasible to share a database between a large number of users, with each user given access only to a subset of the database. This is a frequent use case for mobile apps, and one that doesn't work very well with CouchDB.

<small>\* _It can actually run without Couchbase Server, using a simple built-in database called [Walrus][WALRUS]. This is useful for testing or for very lightweight use. More details below._</small>

### Limitations

* Document IDs longer than about 180 characters will overflow Couchbase's key size limit and cause an HTTP error.
* Only a subset of the CouchDB REST API is supported: this is intentional. The gateway is _not_ a CouchDB replacement, rather a compatible sync endpoint.
* Explicit garbage collection is required to free up space, via a REST call to `/_vacuum`. This is not yet scheduled automatically, so you'll have to call it yourself.
* Performance is probably not that great. We'll be optimizing more later.

### License

Apache 2 license, like all Couchbase stuff.



# How To Run It

## Quick installation

The Sync Gateway is known to work on Mac OS X, Windows and Ubuntu Linux. It has no external dependencies other than the Go language.

1. Install [Go](http://golang.org). Make sure you have version 1.1b1 or later. You can download and run a [binary installer](http://code.google.com/p/go/downloads/list), or tell your favorite package manager like apt-get or brew or MacPorts to install "golang". (Just check that it's going to install 1.1 and not 1.0.3.)
2. Go needs [Git](http://git-scm.com) and [Mercurial](http://mercurial.selenic.com/downloads/) for for downloading packages. You may already have these installed. Enter `git --version` and `hg --version` in a shell to check. If not, go get 'em.
3. We recommend setting a `GOPATH` so 3rd party packages don't get installed into your system Go installation. If you haven't done this yet, make an empty directory someplace (like maybe `~/lib/Go`), then edit your .bash_profile or other shell startup script to set and export a `$GOPATH` variable pointing to it. (While you're at it, you might want to add its `bin` subdirectory to your `$PATH`.) Don't forget to start a new shell so the variable takes effect.
4. Now you can install the gateway simply by running `go get -u github.com/couchbaselabs/sync_gateway`

## Quick startup

The Sync Gateway launcher tool is `bin/sync_gateway` in the directory pointed to by your GOPATH (see item 3 above.) If you didn't set a GOPATH, it'll be somewhere inside your system Go installation directory (like `/usr/local/go`, or `/usr/local/Cellar/go` if you used Homebrew.)

If you've already added this `bin` directory to your shell PATH, you can invoke it from a shell as `sync_gateway`, otherwise you'll need to give its full path. Either way, start the gateway from a shell using the following arguments:

    sync_gateway -url walrus:

That's it! You now have a sort of mock-CouchDB listening on port 4984 (and with an admin API on port 4985.) It has a single database called "sync_gateway". It definitely won't do everything CouchDB does, but you can tell another CouchDB-compatible database (including TouchDB) to replicate with it.

(To use a different database name, use the `-dbname` flag: e.g. `-dbname mydb`.)

The gateway is using a simple in-memory database called [Walrus][WALRUS] instead of Couchbase Server. This is very convenient for quick testing. Walrus is _so_ simple, in fact, that by default it doesn't persist its data to disk at all, so every time the sync_gateway process exits it loses all of its data! Great for unit testing, not great for any actual use. You can make your database persistent by changing the `-url` value to a path to an already-existing filesystem directory:

    mkdir /data
    sync_gateway -url /data

The gateway will now periodically save its state to a file `/data/sync_gateway.walrus`. This is by no means highly scalable, but it will work for casual use.

## Connecting with a real Couchbase server

Using a real Couchbase server, once you've got one set up, is as easy as changing the URL:

0. Install and start [Couchbase Server 2.0][COUCHBASE_SERVER] or later.
1. Create a bucket named `sync_gateway` in the default pool.
2. Start the sync gateway with an argument `-url` followed by the HTTP URL of the Couchbase server:

```
    sync_gateway -url http://localhost:8091
```

If you want to use a different name for the bucket, or listen on a different port, you can do that with command-line options. Use the `--help` flag to see a list of options.

## Configuration files

Instead of entering the settings on the command-line, you can store them in a JSON file and then just provide the path to that file as a command-line argument. As a bonus, the file lets you run multiple databases.

Here's an example configuration file that starts a server with the default settings:

    {
        "interface": ":4984",
        "adminInterface": ":4985",
        "log": ["CRUD", "REST"],
        "databases": {
            "sync_gateway": {
                "server": "http://localhost:8091",
                "bucket": "sync_gateway"
            }
        }
    }

If you want to run multiple databases you can either add more entries to the `databases` property in the config file, or you can put each of them in its own config file (just like above) and list each of the config files on the command line.

# Channels

Channels are the intermediaries between documents and users. Every document belongs to a set of channels, and every user has a set of channels s/he is allowed to access. Additionally, a replication from Sync Gateway specifies what channels it wants to replicate; documents not in any of these channels will be ignored (even if the user has access to them.)

Thus, channels have three purposes:

1. Authorizing users to see documents;
2. Partitioning the data set;
3. Constraining the amount of data synced to (mobile) clients.

There is no need to register or preassign channels. Channels come into existence as documents are assigned to them. Channels with no documents assigned are considered empty.

Valid channel names consist of Unicode letter and digit characters, as well as "_", "-" and ".". The empty string is not allowed. The special meta-channel name "*" denotes all channels. Channel names are compared literally, so they are case- and diacritical-sensitive.

## Mapping documents to channels

There are currently two ways to assign documents to channels. Both of these operate implicitly: there's not a separate action that assigns a doc to a channel, rather the _contents_ of the document determine what channels its in.

### Explicit property

The default (simple and limited) way is to add a `channels` property to a document. Its value is an array of strings. The strings are the names of channels that this document will be available through. A document with no `channels` property will not appear in any channels.

### Sync function

The more flexible way is to define a **sync function**. This is a JavaScript function, similar to a CouchDB validation function, that takes a document body as input and can decide based on that what channels it should go into. Like a regular CouchDB function, it may not reference any external state and it must return the same results every time it's called on the same input.

The sync function is specified in the config file for your database. Each sync function applies to one database.

To add the current document to a channel, the function should call the special function `channel` which takes one or more channel names (or arrays of channel names) as arguments. For convenience, `channel` ignores `null` or `undefined` argument values.

Defining a sync function overrides the default channel mapping mechanism; that is, the document's `channels` property will be ignored. The default mechanism is equivalent to the following simple sync function:

    function (doc) { channel(doc.channels); }

## Replicating channels to Couchbase Lite, CouchDB or TouchDB

The basics are simple: When pulling from Sync Gateway using the CouchDB API, configure the replication to use a filter named `sync_gateway/bychannel`, and a filter parameter `channels` whose value is a comma-separated list of channels to fetch. The replication will now only pull documents tagged with those channels.

(Yes, this sounds just like CouchDB filtered replication. The difference is that the implementation is much, much more efficient because it uses a B-tree index to find the matching documents, rather than simply calling a JavaScript filter function on every single document.)

### Removal from channels

There is a tricky edge case of a document being "removed" from a channel without being deleted, i.e. when a new revision is not added to one or more channels that the previous revision was in. Subscribers (downstream databases pulling from this db) should know about this change, but it's not exactly the same as a deletion. CouchDB's existing filtered-replication mechanism does not address this, which has made things difficult for some clients.

Sync Gateway's `_changes` feed includes one more revision of a document after it stops matching a channel. It adds a `removed` property to the entry where this happens. (No client yet recognizes this property, though.) The value of `removed` is an array of strings, each string naming a channel this revision no longer appears in.

The effect on the client will be that after a replication it sees the next revision of the document, the one that causes it to no longer match the channel(s). It won't get any further revisions until the next one that makes the document match again.

This could seem weird ("why am I downloading documents I don't need?") but it ensures that any views running in the client will correctly no longer include the document, instead of including an obsolete revision. If the app code uses views to filter instead of just assuming that all docs in its local db must be relevant, it should be fine.

Note that in cases where the user's access to a channel is revoked, this will not remove documents from the user's device which are part of the revoked channels but have already been synced.

# Authentication & Authorization

Sync Gateway supports user accounts that are allowed to access only a subset of channels (and hence documents).

## Accounts

Accounts are managed through a parallel REST interface that runs on port 4985 (by default, but this can be customized via the `authaddr` command-line argument). This interface is privileged and for internal use only; instead, we assume you have some other server-side mechanism for users to manage accounts, which will call through to this API.

The URL for a user account is simply "/_database_/user/_name_" where _name_ is the username and _database_ is the configured name of the database. The typical GET, PUT and DELETE methods apply. The contents of the resource are a JSON object with the properties:

* "name": The user name (same as in the URL path). Names must consist of alphanumeric ASCII characters or underscores.
* "admin_channels": An array of strings -- the channels that the user is granted access to by the administrator. The name "*" means "all channels". An empty array or missing property denies access to all channels.
* "all_channels": Like "admin_channels" but also includes channels the user is given access to by other documents via a sync function. (This is a derived property and changes to it will be ignored.)
* "roles": An optional array of strings -- the roles (q.v.) the user belongs to.
* "password": In a PUT or POST request, put the user's password here. It will not be returned by a GET.
* "passwordhash": Securely hashed version of the password. This will be returned from a GET. If you want to update a user without changing the password, leave this alone when sending the modified JSON object back through PUT.
* "disabled": Normally missing; if set to `true`, disables access for that account.
* "email": The user's email address. Optional, but Persona login (q.v.) needs it.

You can create a new user either with a PUT to its URL, or by POST to `/$DB/user/`.

There is a special account named `GUEST` that applies to unauthenticated requests. Any request to the public API that does not have an `Authorization` header is treated as the `GUEST` user. The default `admin_channels` property of the guest user is `["*"]`, which gives access to all channels. In other words, it's the equivalent of CouchDB's "admin party". If you want any channels to be read-protected, you'll need to change this first.

To disable all guest access, set the guest user's `disabled` property:

    curl -X PUT localhost:4985/$DB/user/GUEST --data '{"disabled":true, "channels":[]}'

## Roles

A user account can be assigned to zero or more _roles_. Roles are simply named collections of channels; a user inherits the channel access of all roles it belongs to. This is very much like CouchDB; or like Unix groups, except that roles do not form a hierarchy.

Roles are accessed through the admin REST API much like users are, through URLs of the form "/_database_/role/_name_". Role resources have a subset of the properties that users do: `name`, `admin_channels`, `all_channels`.

Roles have a separate namespace from users, so it's legal to have a user and a role with the same name.

## Authentication

[Like CouchDB](http://wiki.apache.org/couchdb/Session_API), Sync Gateway allows clients to authenticate using either HTTP Basic Auth or cookie-based sessions. The only difference is that we've moved the session URL from `/_session` to `/dbname/_session`, because in the Sync Gateway user accounts are per-database, not global to the server.

### Persona

Sync Gateway also supports [Mozilla's Persona (aka BrowserID) protocol](https://developer.mozilla.org/en-US/docs/persona) that allows users to log in using their email addresses.

To enable it, you need to add a top-level `persona` property to your server config file. Its value should be an object with an `origin` property containing your server's canonical root URL, like so:

    {
        "persona": { "origin": "http://example.com/" },
        ...

Alternatively, you can use a `-personaOrigin` command-line flag whose value is the origin URL.

(This is necessary because the Persona protocol requires both client and server to agree on the server's ID, and there's no reliable way to derive the URL on the server, especially if it's behind a proxy.)

Once that's set up, you need to set the `email` property of user accounts, so that the server can determine the account based on the email address.

Clients log in by POSTing to `/dbname/_persona`, with a JSON body containing an `assertion` property whose value is the signed assertion received from the identity provider. Just as with a `_session` login, the response will set a session cookie.

### Indirect Authentication

An app server can also create a session for a user by POSTing to `/dbname/_session` on the privileged port-4985 REST interface. The request body should be a JSON document with two properties: `name` (the user name) and `ttl` time-to-live measured in seconds. The response will be a JSON document with properties `cookie_name` (the name of the session cookie the client should send), `session_id` (the value of the session cookie) and `expires` (the time the session expires).

This allows the app server to optionally do its own authentication: the client sends credentials to it, and then it uses this API to generate a login session and send the cookie data back to the client, which can then send it in requests to Sync Gateway.

## Authorization

The `all_channels` property of a user account determines what channels that user may access.

Any GET request to a document not assigned to one or more of the user's available channels will fail with a 403.

Accessing a `_changes` feed with any channel names that are not available to the user will fail with a 403.

Write protection is done by document validation functions, as in CouchDB: the function can look at the current user and determine whether that user should be able to make the change; if not, it throws an exception with a 403 Forbidden status code.

There is currently an edge case where after a user is granted access to a new channel, their client will not automatically sync with pre-existing documents in that channel (that they didn't have access to before.) The workaround is for the client to do a one-shot sync from only that new channel, which having no checkpoint will start over from the beginning and fetch all the old documents.

### Programmatic Authorization

It is possible for documents to grant users access to channels; this is done by writing a sync function that recognizes such documents and calls a special `access()` function to grant access.

`access() takes two parameters: the first is a user name or array of user names; the second is a channel name or array of channel names. For convenience, null values are ignored (treated as empty arrays.)

A typical example is a document representing a shared resource (like a chat room or photo gallery), which has a property like `members` that lists the users who should have access to that resource. If the documents belonging to that resource are all tagged with a specific channel, then a sync function can be used to detect the membership property and assign access to the users listed in it:

	function(doc) {
		if (doc.type == "chatroom") {
			access(doc.members, doc.channel_id)
		}
	}

In this example, a chat room is represented by a document with a "type" property "chatroom". The "channel_id" property names the associated channel (with which the actual chat messages will be tagged), and the "members" property lists the users who have access.

`access()` can also operate on roles: if a username string begins with `role:` then the remainder of the string is interpreted as a role name. (There's no ambiguity here, since ":" is an illegal character in a user or role name.)

### Authorizing Document Updates

As mentioned earlier, sync functions can also authorize document updates. Just like a CouchDB validation function, a sync function can reject the document by throwing an exception:

    throw({forbidden: "error message"})

A 403 Forbidden status and the given error string will be returned to the client.

To validate a document you often need to know which user is changing it, and sometimes you need to compare the old and new revisions. For those reasons the sync function actually takes up to three parameters, like a CouchDB validation function. To get access to the old revision and the user, declare it like this:

    function(doc, oldDoc, user) { ... }

`oldDoc` is the old revision of the document (or empty if this is a new document.) `user` is an object with properties `name` (the username), `roles` (an array of the names of the user's roles), and `channels` (an array of all channels the user has access to.)

[COUCHBASE_LITE]: https://github.com/couchbase/couchbase-lite-ios
[TOUCHDB]: https://github.com/couchbaselabs/TouchDB-iOS
[COUCHDB]: http://couchdb.apache.org
[COUCHBASE_SERVER]: http://www.couchbase.com/couchbase-server/overview
[WALRUS]:https://github.com/couchbaselabs/walrus
