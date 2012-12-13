# ChannelSync

Jens Alfke

This is a fork of BaseCouch (see the master branch) that implements experimental support for channel-based syncing.

## One database only

Unlike BaseCouch this server supports only a _single_ database in its bucket. The name defaults to "channelsync", but can be changed using the `dbname` command-line flag. Any attempt to access or create other databases will fail with an appropriate HTTP error.

(Basically, the multiplexing of multiple databases in a single Couchbase bucket was an attempt to share content efficiently between large numbers of subscribers, something that we now think we can do more efficiently using channels.)

## Mapping documents to channels

There are currently two ways to map documents to channels.

### Explicit property

The default (simple and limited) way is to add a `channels` property to a document. Its value is an array of strings. The strings are the names of channels that this document will be available through. A document with no `channels` property will not appear in any channels.

### Mapping function

The more flexible way is to define a channelmap function. This is a JavaScript function, similar to a "map", that takes a document body as input and can decide based on that what channels it should go into. Like a regular map function, it may not reference any external state and it must return the same results every time it's called on the same input.

The channelmap function goes in a design document with ID `_design/channels`, in a property named `channelmap`.

To add the current document to a channel, the function should call the special function `sync` which takes one or more channel names (or arrays of channel names) as arguments. For convenience, `sync` ignores `null` or `undefined` argument values.

Defining a channelmap overrides the default channel mapping mechanism; that is, the `channels` property will be ignored.

The default mechanism is equivalent to the following simple channelmap:

    function (doc) { sync(doc.channels); }

## Replicating channels to CouchDB or TouchDB

The basics are simple: When pulling from ChannelSync using the CouchDB API, configure the replication to use a filter named `channelsync/bychannel`, and a filter parameter `channels` whose value is a comma-separated list of channels to subscribe to. The replication will now only pull documents tagged with those channels.

### Removal from channels

There is a tricky edge case of a document being "removed" from a channel without being deleted, i.e. when a new revision is not added to one or more channels that the previous revision was in. Subscribers (downstream databases pulling from this db) should know about this change, but it's not exactly the same as a deletion. CouchDB's existing filtered-replication mechanism does not address this, which has made things difficult for some clients.

ChannelSync's `_changes` feed includes one more revision of a document after it stops matching a channel. It adds a `removed` property to the entry where this happens. (No client yet recognizes this property, though.) The value of `removed` is an array of strings, each string naming a channel this revision no longer appears in.

The effect on the client will be that after a replication it sees the next revision of the document, the one that causes it to no longer match the channel(s). It won't get any further revisions until the next one that makes the document match again.

This could seem weird ("why am I downloading documents I don't need?") but it ensures that any views running in the client will correctly no longer include the document, instead of including an obsolete revision. If the app code uses views to filter instead of just assuming that all docs in its local db must be relevant, it should be fine.

## Authentication & Authorization

ChannelSync supports user accounts that are allowed to access only a subset of channels.

### Accounts

Accounts are managed through a parallel REST interface that runs on port 4985 (by default, but this can be customized via the `authaddr` command-line argument). This interface is privileged and for internal use only; instead, we assume you have some other server-side mechanism for users to manage accounts, which will call through to this API.

The URL for a user account is simply "/_user_" where _user_ is the username. The typical GET, PUT and DELETE methods apply. The contents of the resource are a JSON object with the properties:

* "name": The user name (same as in the URL path). Names must consist of alphanumeric ASCII characters or underscores.
* "channels": An array of channel name strings. The name "*" means "all channels". An empty array or missing property denies access to all channels. A missing `channels` property prevents the user from authenticating at all.
* "password": In a PUT or POST request, put the user's password here. It will not be returned by a GET.
* "passwordhash": Securely hashed version of the password. This will be returned from a GET. If you want to update a user without changing the password, leave this alone when sending the modified JSON object back through PUT.

You can create a new user either with a PUT to its URL, or by POST to `/`.

There is a special account named `GUEST` that applies to unauthenticated requests. Any request to the public API that does not have an `Authorization` header is treated as the `GUEST` user. The default `channels` property of the guest user is `["*"]`, which gives access to all channels. In other words, it's the equivalent of CouchDB's "admin party". If you want any channels to be read-protected, you'll need to change this first.

### Authorization

The `channels` property of a user account determines what channels that user may access.

Any GET request to a document not assigned to one or more of the user's available channels will fail with a 403.

Accessing a `_changes` feed with any channel names that are not available to the user will fail with a 403.

There is not yet any _write_ protection; this is TBD. It's going to be conceptually trickier because there are definitely use cases where you create or update documents that are tagged with channels you don't have access to. We may just need a separate validation function as in CouchDB.

There is currently an edge case where after a user is granted access to a new channel, their client will not automatically sync with pre-existing documents in that channel (that they didn't have access to before.) The workaround is for the client to do a one-shot sync from only that new channel, which having no checkpoint will start over from the beginning and fetch all the old documents.
