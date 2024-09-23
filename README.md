# Sync Gateway

[![Sync Gateway Documentation](https://img.shields.io/badge/documentation-current-blue.svg)][SG_DOCS]
[![GoDoc](https://godoc.org/github.com/couchbase/sync_gateway?status.svg)](https://godoc.org/github.com/couchbase/sync_gateway)
[![Go Report Card](https://goreportcard.com/badge/github.com/couchbase/sync_gateway)](https://goreportcard.com/report/github.com/couchbase/sync_gateway)
[![Code Coverage](https://jenkins.sgwdev.com/buildStatus/icon?job=MasterIntegration&subject=coverage&status=${lineCoverage}&color=${colorLineCoverage})](https://jenkins.sgwdev.com/job/MasterIntegration/lastBuild/coverage/)
[![License](https://img.shields.io/badge/license-BSL%201.1-lightgrey)](https://github.com/couchbase/sync_gateway/blob/main/LICENSE)

Sync Gateway is a horizontally scalable web server that securely manages the access control and
synchronization of data between [Couchbase Lite][CB_LITE] and [Couchbase Server][CB_SERVER].

## Couchbase Capella DBaaS

Couchbase's cloud database platform is the easiest and fastest way to begin with Couchbase and eliminate ongoing database management efforts.
Try for free at [Couchbase Capella][CB_CAPELLA].

## Self-Managed and On-Prem

Download Sync Gateway and other Couchbase packages for Linux, Windows and macOS at [Couchbase Downloads][CB_DOWNLOAD].

## Build from source

### Pre-requisites

To build Sync Gateway from source, you must have the following installed:

* Go 1.21 or later ([Installing Go](https://golang.org/doc/install))
* Building the Enterprise Edition requires access to private code, and cannot be built by third-parties.

### Build Instructions 

```shell
$ go build
```

## Resources

- [Sync Gateway Documentation][SG_DOCS]
- [Couchbase Forums][CB_FORUM]
- Couchbase Products:
    - [Sync Gateway][CB_GATEWAY]
    - [Lite][CB_LITE]
    - [Mobile][CB_MOBILE]
    - [Server][CB_SERVER]
    - [Developer SDKs][CB_SDK]
- [Couchbase Downloads][CB_DOWNLOAD]
- [Sync Gateway Issue Tracker][SG_ISSUES]

## License

[Business Source License 1.1](https://github.com/couchbase/sync_gateway/blob/main/LICENSE)

[CB_MOBILE]: https://www.couchbase.com/products/mobile
[CB_GATEWAY]: https://www.couchbase.com/products/sync-gateway
[CB_LITE]: https://www.couchbase.com/products/lite
[CB_SERVER]: https://www.couchbase.com/products/server
[CB_SDK]: https://www.couchbase.com/products/developer-sdk
[CB_DOWNLOAD]: https://www.couchbase.com/downloads
[CB_FORUM]: http://forums.couchbase.com
[SG_DOCS]: https://docs.couchbase.com/sync-gateway/current/introduction.html
[SG_ISSUES]: https://github.com/couchbase/sync_gateway/issues?q=is%3Aissue+is%3Aopen
[CB_CAPELLA]: https://cloud.couchbase.com/sign-up?ref=github-sgw
