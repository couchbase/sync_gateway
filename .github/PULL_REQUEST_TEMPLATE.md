CBG-0000

Describe your PR here...
- Use bullet points if there's more than one thing changed

## Pre-review checklist (remove once done)
- Removed debug logging (`fmt.Print`, `log.Print`, ...)
- Logging sensitive data? Make sure it's tagged (e.g. `base.UD(docID)`, `base.MD(dbName)`)

## Dependencies (if applicable)
- [ ] Link upstream PRs
- [ ] Bump manifest once merged

## [Integration Tests](https://jenkins.sgwdev.com/job/SyncGateway-Integration/build?delay=0sec)
- [ ] `xattrs=true` https://jenkins.sgwdev.com/job/SyncGateway-Integration/000/
