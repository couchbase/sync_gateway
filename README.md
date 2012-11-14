# ChannelSync

Jens Alfke

This is a fork of BaseCouch (see the master branch) that implements experimental support for channel-based syncing.

* Add a "channel" property to each document, whose value is an array of strings. The strings are the names of channels that this document will be available through.
* When syncing from the database using the CouchDB API, specify a filter named "bychannel" and a filter parameter "channels" whose value is a comma-separated list of channels to subscribe to.
* The replication will now only include documents tagged with those channels.
* There is a tricky edge case of a document being "removed" from a channel without being deleted, i.e. being updated with a "channels" property that omits one or more channels it used to list. Subscribers (downstream databases pulling from this db) should know about this change, but it's not exactly the same as a deletion. Currently the changes feed adds a "removed" property to an entry where this happens. No client yet recognizes this property, though.

WARNING: Unlike BaseCouch this server supports only a _single_ database in its bucket. The name is hardwired to "db". Any attempt to access or create other databases will fail with an appropriate HTTP error. (Basically, the multiplexing of multiple databases in a single Couchbase bucket was an attempt to share content efficiently between large numbers of subscribers, something that we now think we can do more efficiently using channels.)
