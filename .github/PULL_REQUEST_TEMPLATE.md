CBG-0000

Describe your PR here...
- Use bullet points if there's more than one thing changed

## Pre-review checklist (remove once done)
- Removed debug logging (fmt.Print, log.Print, ...)
- Logging sensitive data? Make sure it's tagged (e.g. `base.UD(docID)`, `base.UD(username)`)

## Dependencies (if appliccable)
- [ ] Link upstream PRs
- [ ] Bump manifest once merged

## [Integration Tests](http://uberjenkins.sc.couchbase.com:8080/job/sync-gateway-integration/build?delay=0sec)
- [ ] `xattrs=true ` http://uberjenkins.sc.couchbase.com:8080/job/sync-gateway-integration/???/
- [ ] `xattrs=false` http://uberjenkins.sc.couchbase.com:8080/job/sync-gateway-integration/???/
