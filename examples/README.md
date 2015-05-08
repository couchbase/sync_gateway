
Here are a list of example Sync Gateway configurations:

File  | Description
------------- | -------------
basic-walrus-bucket.json  | Start with this example.  It is a minimal config that uses the Walrus in memory bucket as a backing store.
basic-walrus-persisted-bucket.json  | Uses the Walrus in memory bucket, with regular snapshots persisted to disk in the current directory.
basic-couchbase-bucket.json  | Uses a Couchbase Server bucket as a backing store.
basic-sync-function.json  | Uses a custom Sync Function.
users-roles.json  | Statically define users and roles.  (They can also be defined via the REST API)
read-write-timeouts.json  | Demonstrates how to set timeouts on reads/writes.
cors.json  | Enable CORS support.
config-server.json  | Use an external configuration server to support dynamic configuration, such as the ability to add databases on the fly.
democlusterconfig.json | This the configuration used by the demo cluster Sync Gateway instance, which example apps such as TodoLite and GrocerySync connect to by default.

## Disabling logging

By default these examples have all log levels enabled.  The idea behind this is to make debugging easier.

If the logs become too noisy, please replace the log section with:

```
"log": ["HTTP+"]
```

More log levels are documented in the [Sync Gateway documentation](http://developer.couchbase.com/mobile/develop/guides/sync-gateway/)

## How these examples were formatted

```
$ npm install -g underscore-cli
$ cat basic-walrus-bucket.json | underscore pretty --color
```
			
									

