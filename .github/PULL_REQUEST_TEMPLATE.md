CBG-0000

Describe your PR here...
- Use bullet points if there's more than one thing changed

## Dependencies (if applicable)
- [ ] Link upstream PRs
- [ ] Update Go module dependencies when merged

## [Integration Tests](https://jenkins.sgwdev.com/job/SyncGateway-Integration/build?delay=0sec)
- [ ] `xattrs=true` https://jenkins.sgwdev.com/job/SyncGateway-Integration/000/

## Reviewer checklist
- [ ] Check all debug logging (`fmt.Print`, `log.Print`, ...) is removed
- [ ] Check logged sensitive data is tagged (e.g. `base.UD(docID)`, `base.MD(dbName)`)
- [ ] Check all relevant information in the API specifications (such as endpoint descriptions, schemas, ...) has been updated in `docs/api`