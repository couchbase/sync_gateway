## Example Startup Configurations
Here are a list of example Sync Gateway Startup configurations found in `examples/startup_config`:

File  | Description
------------- | -------------
basic.json | Start a connection with Couchbase Server.
cors.json | Enable CORS support.
logging-with-rotation.json | Shows log rotation option usage.
logging-without-redaction.json | Shows disabling log redaction.
../ssl/ssl.json | Require TLS certificates to connect to Sync Gateway (by both the REST API and Couchbase Lite).
x509.json | Start a secure connection with Couchbase Server.

## Example Database Configurations
Here are a list of example Sync Gateway Database configurations found in `examples/database_config`:

File  | Description
------------- | -------------
ee-basic-delta-sync.json | Enabled delta sync available in the enterprise edition.
ee-cache-config.json | Makes use of the cache settings available in the enterprise edition.
events-webhook.json | Adds a webhook event, called when documents are altered and satisfies the filter.
import-filter.json | Imports docs with an import filter.
openid-connect.json | Utilizes the openID connect function to authenticate users.
sync-function.json | Uses a custom Sync Function.

## Using the example configurations
From Sync Gateway 3.0, the configuration file used to start Sync Gateway does not contain database information. Instead, database information is passed through using the REST API endpoints. This means example startup configurations and database configurations can be mixed and matched with each other.

All the example startup configurations and endpoint URLs have default values for username and password (which is `username` and `password` respectively) and for the bucket name (which is `default`). These may need to be changed to match your Couchbase Server and bucket setup before Sync Gateway can successfully start.

The example startup configurations contain console logging set to the `info` level meaning the console will print out information about what Sync Gateway is doing as well as anything on above levels (such as warning and errors). This can be changed to make Sync Gateway less verbose. See the [Sync Gateway documentation](https://docs.couchbase.com/sync-gateway/current/authentication-users.html) on how to change this.

To start Sync Gateway with a startup configuration, run the binary with the configuration file as an argument. For example, to run the `basic.json` example configuration run:
```
./bin/sync_gateway examples/startup_config/basic.json
```

A database with it's configuration can be added using the `PUT /{db}/` endpoint. For example, to add an import filter to Sync Gateway on Unix based systems, run:
```
curl -X PUT username:password@localhost:4985/db/ -H "Content-Type: application/json" --data-binary "@examples/database_config/import-filter.json"
```

None of the database configuration files enable the guest account or create a user for use with the Public REST API. To enable the guest account so the Public REST API can be interacted with unauthenticated, run:
```
curl -X PUT username:password@localhost:4985/db/_config -H "Content-Type: application/json" --data-binary '{"guest": {"disabled":false}}'
```
More information on user authentication can be found in the [Sync Gateway documentation](https://docs.couchbase.com/sync-gateway/current/authentication-users.html).

## Legacy Configurations
**Use of legacy configurations are not recommended nor supported.** Here are a list of example Sync Gateway legacy configurations found in `examples/legacy_config`:

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
../serviceconfig.json | The default config used for service startup. Should be avoided when building a suitable config as Walrus is not supported.
ssl.json | Require TLS certificates to connect to Sync Gateway (by both the REST API and Couchbase Lite).
users-roles.json  | Statically define users and roles.  (They can also be defined via the REST API)
