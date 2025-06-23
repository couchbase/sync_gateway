When performing a code review, if there are any changes to the REST APIs (e.g. REST handler code, query parameters, structs returned via handler functions), ensure the OpenAPI specifications are updated accordingly in the `docs/api` directory.

When performing a code review, if there is any dev-time logging using `log.Printf`, `fmt.Printf`, or similar, ensure it is removed or replaced with appropriate Sync Gateway logging (e.g. `base.Infof`, `base.Warnf`, `base.Debugf`).

When performing a code review, if a log message includes User Data, ensure the value is wrapped with the `base.UD()` helper function to enable redaction. User Data includes: Document IDs, JSON document contents (keys and values), usernames, email addresses, or other personally identifiable information (PII).

When performing a code review, be mindful of performance implications, such as mutex contention, race conditions, and other concurrency-related issues.

When performing a code review, ensure code comments explain the *intent* or *reasoning* behind an implementation, rather than just restating what the code does.

When performing a code review, ensure `for` loops have sufficient exit conditions and are not prone to infinite loops. Prefer expressing the exit condition in the loop declaration itself, rather than relying on `break` statements within the loop body.
