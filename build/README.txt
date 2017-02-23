Couchbase Sync Gateway 1.4

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
