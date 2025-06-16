When performing a code review, if there are any changes to the REST APIs (e.g. rest handler code, query parameters, structs that get returned via a handler function, etc.), ensure the OpenAPI specifications are updated accordingly in the `docs/api` directory.

When performing a code review, if there is any dev-time logging in the new code using `log.Printf`, `fmt.Printf` or similar, make sure this is removed or replaced with Sync Gateway logging (e.g. `base.Infof`, `base.Warnf`, `base.Debugf`, etc.)

When performing a code review, if there's a log message that includes User Data, ensure the value is tagged with the `base.UD()` helper function to ensure these get redacted. User Data is considered to be one of the following: Document IDs, contents from a JSON document (keys and values), Usernames, email addresses, or any other PII.

When performing a code review, be considerate of performance, mutex contention, race conditions, and other concurrency bugs.

When performing a code review, ensure that code comments are not simply repeating the code, but rather explaining the intent of the code or the reasoning behind a particular implementation choice.

When performing a code review, evaluate whether do while or unconditional for loops have sufficient exit conditions, and that they are not prone to infinite loops.