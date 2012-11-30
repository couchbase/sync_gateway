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