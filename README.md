# Couchbase Sync Gateway

Gluing [Couchbase Lite][COUCHBASE_LITE] to [Couchbase Server][COUCHBASE_SERVER]

The Sync Gateway manages HTTP-based data access for mobile clients. It handles access control and data routing, so that a single large Couchbase Server cluster can manage data for multiple users and complex applications.

<img src="http://jchris.ic.ht/files/slides/mobile-arch.png" width="600px"/>

This README was getting too long, so here it is broken out into chapters.

* [Introduction-to-Sync-Gateway](https://github.com/couchbaselabs/sync_gateway/wiki/Introduction-to-Sync-Gateway)
* [Installing-and-Upgrading](https://github.com/couchbaselabs/sync_gateway/wiki/Installing-and-Upgrading)
* [Administration-Basics](https://github.com/couchbaselabs/sync_gateway/wiki/Administration-Basics)
* [Channels-Access-Control-and-Data-Routing-w-Sync-Function](https://github.com/couchbaselabs/sync_gateway/wiki/Channels-Access-Control-and-Data-Routing-w-Sync-Function)
* [Authentication](https://github.com/couchbaselabs/sync_gateway/wiki/Authentication)
* [Couchbase-Server-Smart-Client-SDKs](https://github.com/couchbaselabs/sync_gateway/wiki/Couchbase-Server-Smart-Client-SDKs)
* [Admin-REST-API](https://github.com/couchbaselabs/sync_gateway/wiki/Admin-REST-API)
* [Sync-REST-API](https://github.com/couchbaselabs/sync_gateway/wiki/Sync-REST-API)
* [Changes-Worker-Pattern](https://github.com/couchbaselabs/sync_gateway/wiki/Changes-Worker-Pattern)
* [State-Machine-Document-Pattern](https://github.com/couchbaselabs/sync_gateway/wiki/State-Machine-Document-Pattern)
* [Scaling-Sync-Gateway](https://github.com/couchbaselabs/sync_gateway/wiki/Scaling-Sync-Gateway)
* [Troubleshooting](https://github.com/couchbaselabs/sync_gateway/wiki/Troubleshooting)


### License

Apache 2 license, like all Couchbase stuff.

## Tutorials and Other Resources

There is a [broad overview of mobile and Couchbase Lite here, with pointers to most of the docs](https://github.com/couchbase/mobile)

If you're having trouble, feel free to ask for help on the [mailing list][MAILING_LIST]. If you're pretty sure you've found a bug, please [file a bug report][ISSUE_TRACKER].

[COUCHBASE_LITE]: https://github.com/couchbase/couchbase-lite-ios
[TOUCHDB]: https://github.com/couchbaselabs/TouchDB-iOS
[COUCHDB]: http://couchdb.apache.org
[COUCHDB_API]: http://wiki.apache.org/couchdb/Complete_HTTP_API_Reference
[COUCHBASE_SERVER]: http://www.couchbase.com/couchbase-server/overview
[WALRUS]: https://github.com/couchbaselabs/walrus
[HTTPIE]: http://httpie.org
[MAILING_LIST]: https://groups.google.com/forum/?fromgroups#!forum/mobile-couchbase
[ISSUE_TRACKER]: https://github.com/couchbaselabs/sync_gateway/issues?state=open
[MAC_STABLE_BUILD]: http://cbfs-ext.hq.couchbase.com/mobile/SyncGateway/SyncGateway-Mac.zip
