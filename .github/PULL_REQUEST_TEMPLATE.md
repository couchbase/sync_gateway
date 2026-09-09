CBG-0000

Describe your PR here...
- Use bullet points if there's more than one thing changed

## Pre-review checklist
- [ ] Logging sensitive data? Make sure it's tagged (e.g. `base.UD(docID)`, `base.MD(dbName)`)
- [ ] Updated relevant information in the API specifications (such as endpoint descriptions, schemas, ...) in `docs/api`

## Dependencies (if applicable)
- [ ] Link upstream PRs
- [ ] Update Go module dependencies when merged

## [Integration Tests](https://jenkins.sgwdev.com/job/SyncGatewayIntegration-Pipeline/build?delay=0sec)
- [ ] https://jenkins.sgwdev.com/job/SyncGatewayIntegration-Pipeline/0000/

<!--
Automated review runs on every non-draft PR raised from this repo (forks are not reviewed automatically).
  - Re-run it, or ask a follow-up, by commenting `@droid <question>`
  - Suppress it with the `droid-skip` label, or `WIP`/`DO NOT MERGE` in the title
-->
