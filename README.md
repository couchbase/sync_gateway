# Sync Gateway

[![Sync Gateway Documentation](https://img.shields.io/badge/documentation-current-blue.svg)][SG_DOCS]
[![GoDoc](https://godoc.org/github.com/couchbase/sync_gateway?status.svg)](https://godoc.org/github.com/couchbase/sync_gateway)
[![Go Report Card](https://goreportcard.com/badge/github.com/couchbase/sync_gateway)](https://goreportcard.com/report/github.com/couchbase/sync_gateway)
[![Codebeat badge](https://codebeat.co/badges/a8fb8053-742a-425b-8e8c-96f1c5bdbd26)](https://codebeat.co/projects/github-com-couchbase-sync_gateway)
[![Code Coverage](https://img.shields.io/coveralls/github/couchbase/sync_gateway.svg)](https://coveralls.io/github/couchbase/sync_gateway)
[![License](https://img.shields.io/github/license/couchbase/sync_gateway.svg)](https://github.com/couchbase/sync_gateway/blob/master/LICENSE)

Sync Gateway is a horizontally scalable web server that securely manages the access control and
synchronization of data between [Couchbase Lite][CB_LITE] and [Couchbase Server][CB_SERVER].

## Download

Download Sync Gateway and other Couchbase packages for Linux, Windows and macOS at [Couchbase Downloads][CB_DOWNLOAD].

## Build from source

### Pre-requisites

To build Sync Gateway from source, you must have the following installed:

* Go 1.8 or later, with your `$GOPATH` set to a valid directory.

**Install Go**

See [Installing Go](https://golang.org/doc/install)

### Build instructions

See the [Extended Build Instructions](docs/BUILD.md) to build with dependency pinning via the `repo` multi-repository tool.

## Resources

- [Sync Gateway Documentation][SG_DOCS]
- [Sync Gateway Issue Tracker][SG_ISSUES]
- Couchbase Products:
    - [Sync Gateway][CB_GATEWAY]
    - [Lite][CB_LITE]
    - [Mobile][CB_MOBILE]
    - [Server][CB_SERVER]
    - [Developer SDKs][CB_SDK]
- [Couchbase Downloads][CB_DOWNLOAD]
- [Couchbase Discussion Forum][CB_FORUM]
- [Couchbase Mobile Mailing List][MAILING_LIST]

## License

[Apache License 2.0](https://github.com/couchbase/sync_gateway/blob/master/LICENSE)

[CB_MOBILE]: https://www.couchbase.com/products/mobile
[CB_GATEWAY]: https://www.couchbase.com/products/sync-gateway
[CB_LITE]: https://www.couchbase.com/products/lite
[CB_SERVER]: https://www.couchbase.com/products/server
[CB_SDK]: https://www.couchbase.com/products/developer-sdk
[CB_DOWNLOAD]: https://www.couchbase.com/downloads
[CB_FORUM]: http://forums.couchbase.com
[SG_REPO]: https://github.com/couchbase/sync_gateway
[SG_DOCS]: https://developer.couchbase.com/documentation/mobile/current/guides/sync-gateway/index.html
[SG_ISSUES]: https://github.com/couchbase/sync_gateway/issues?q=is%3Aissue+is%3Aopen
[MAILING_LIST]: https://groups.google.com/forum/?fromgroups#!forum/mobile-couchbase
