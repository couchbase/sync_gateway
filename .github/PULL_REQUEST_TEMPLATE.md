CBG-0000

Describe your PR here...
- Use bullet points if there's more than one thing changed

## Pre-review checklist
- [ ] Removed debug logging (`fmt.Print`, `log.Print`, ...)
- [ ] Logging sensitive data? Make sure it's tagged (e.g. `base.UD(docID)`, `base.MD(dbName)`)
- [ ] Updated relevant information in the API specifications (such as endpoint descriptions, schemas, ...) in `docs/api`

## Dependencies (if applicable)
- [ ] Link upstream PRs
- [ ] Update Go module dependencies when merged

## [Integration Tests](https://jenkins.sgwdev.com/job/SyncGateway-Integration/build?delay=0sec)
- [ ] `GSI=true,xattrs=true` https://jenkins.sgwdev.com/job/SyncGateway-Integration/000/
