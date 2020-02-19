Couchbase Sync Gateway

This is a self-contained installation of Couchbase Sync Gateway. You can start Sync Gateway by specifying a configuration file through command-line. The configuration file determines the runtime behavior of Sync Gateway, including server configuration and the database or set of databases with which a Sync Gateway instance can interact.

$ ./sync_gateway examples/basic-couchbase-bucket.json

You can stop Sync Gateway with Control-C. There is no specific shutdown procedure and it is safe to stop it at any time.

This application can be run from any location on any writeable volume. You can choose to move it to your Application
directory, but this is not required. However, do not move the application while it's running.

You can find more detailed documentation at https://docs.couchbase.com/sync-gateway/current/getting-started.html