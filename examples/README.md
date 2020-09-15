
Here are a list of example Sync Gateway configurations:

File  | Description
------------- | -------------
basic-couchbase-bucket.json  | Uses a Couchbase Server bucket as a backing store.
basic-sync-function.json  | Uses a custom Sync Function.
config-server.json  | Use an external configuration server to support dynamic configuration, such as the ability to add databases on the fly.
config-shared-bucket-filter.json | Same as below but with a filter applied to the imports.
config-shared-bucket.json | Uses import docs and shared bucket access allowing external clients to share the bucket with sync gateway.
cors.json  | Enable CORS support.
democlusterconfig.json | This the configuration used by the demo cluster Sync Gateway instance, which example apps such as TodoLite and GrocerySync connect to by default.
ee-cache-config.json | Makes use of the cache settings available in the enterprise edition.
ee_basic-delta-sync.json | Enabled delta sync available in the enterprise edition.
events_webhook.json | Adds a webhook event, called when documents are altered and satisfies the filter.
logging-with-redaction.json | Shows log redaction setting.
logging-with-rotation.json | Shows log rotation option usage.
openid-connect.json | Utilizes the openID connect function to authenticate users.
read-write-timeouts.json  | Demonstrates how to set timeouts on reads/writes.
replications-in-config.json | Shows Inter-Sync Gateway replication use.
serviceconfig.json | The default config used for service startup. Should be avoided when building a suitable config as Walrus is not supported.
users-roles.json  | Statically define users and roles.  (They can also be defined via the REST API)

## Disabling logging

By default these examples have all log levels enabled.  The idea behind this is to make debugging easier.

If the logs become too noisy, please replace the log_keys section with:

```
"log_keys": ["HTTP+"]
```

or in some cases the debug level is set for the console and this may be changed to something lower:

```
"log_level": "info"
```

More log levels are documented in the [Sync Gateway documentation](https://docs.couchbase.com/sync-gateway/current/config-properties.html)

## How these examples were formatted

```
$ npm install -g underscore-cli
$ cat basic-couchbase-bucket.json | underscore pretty --color
```
			
									

