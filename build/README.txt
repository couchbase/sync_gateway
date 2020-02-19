Couchbase Sync Gateway

This is a self-contained installation of Couchbase Sync Gateway. You start Sync Gateway by running sync_gateway with the -url option. The argument for the -url option is the HTTP URL of the Couchbase Server to which you want Sync Gateway to connect. If you do not include any additional command-line options, the default values are used.

The following command starts Sync Gateway on port 4984, connects to a limited in-memory database called "walrus" that lives on Sync Gateway, and starts the admin server on port 4985.

$ ./sync_gateway

Command-line options:

  -adminInterface=":4985": Address to bind admin interface to
  -bucket="sync_gateway": Name of bucket
  -dbname="": Name of Couchbase Server database (defaults to name of bucket)
  -interface=":4984": Address to bind to
  -log="": Log keywords, comma separated
  -pool="default": Name of pool
  -pretty=false: Pretty-print JSON responses
  -url="walrus:": Address of Couchbase server
  -verbose=false: Log more info about requests

You can stop Sync Gateway with Control-C. There is no specific shutdown procedure and it is safe to stop it at any time.

This application can be run from any location on any writeable volume.
You can choose to move it to your Application directory, but this is not required. However, do not move the application while it's running.

Note: Walrus is a Go library that provides a tiny implementation of the Bucket API from the sg-bucket package and the
only purpose of Walrus is to be able to run/test Sync Gateway during development without needing to spin up a full
Couchbase Server instance.

Run Couchbase Sync Gateway against Couchbase Server

You can run Sync Gateway against Couchbase Server by specifying the connection details of Couchbase server in a
[database configuration file](https://docs.couchbase.com/sync-gateway/current/getting-started.html).

Once database configuration file is created, issue the below command to start Sync Gateway from the command line:

$ ./sync_gateway examples/config-server.json

If Sync Gateway is running in a service replace the configuration file and restart the service.