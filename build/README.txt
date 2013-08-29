Couchbase Sync Gateway Community Edition 1.0-Beta

This is a self-contained installation of Couchbase Sync Gateway. You start Sync Gateway by running sync_gateway with the -url option. The argument for the -url option is the HTTP URL of the Couchbase server to which you want Sync Gateway to connect. If you do not include any additional command-line options, the default values are used. For -url, this is "walrus", a simple, in-memory database.

The following command starts Sync Gateway on port 4984, connects to the default bucket named sync_gateway in the Couchbase Serving running on localhost, and starts the admin server on port 4985.

$ ./sync_gateway -url http://localhost:8091

More information about the available command-line options are as follows:
  
  Usage of ./sync_gateway:
  -adminInterface=":4985": Address to bind admin interface to
  -bucket="sync_gateway": Name of bucket
  -dbname="": Name of CouchDB database (defaults to name of bucket)
  -interface=":4984": Address to bind to
  -log="": Log keywords, comma separated
  -personaOrigin="": Base URL that clients use to connect to the server
  -pool="default": Name of pool
  -pretty=false: Pretty-print JSON responses
  -url="walrus:": Address of Couchbase server
  -verbose=false: Log more info about requests

You can stop Sync Gateway by typing Control-C. There is no specific shutdown procedure and it is safe to stop it at any time.

This application may be run from any location on any writeable volume.
You may choose to move it to /Applications, but this is not required. However, do not move the application while it's running.