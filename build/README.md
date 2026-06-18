# Couchbase Sync Gateway

This is a self-contained installation of Couchbase Sync Gateway. You can start Sync Gateway by running `sync_gateway` and specifying a configuration file. Sample configuration files can be found in the `examples` folder. Modify the example configuration files to include your Couchbase Server address and credentials.

Detailed getting started information can be found at <https://docs.couchbase.com/sync-gateway/current/getting-started.html>

```shell
./sync_gateway examples/basic-couchbase-bucket.json
```

You can stop Sync Gateway with Control-C. There is no specific shutdown procedure and it is safe to stop it at any time.

This application can be run from any location on any writeable volume. You can choose to move it to your Application directory, but this is not required. However, do not move the application while it's running.

## Contributing

### Pre-commit hooks

Run once per checkout — this is all most contributors need to do:

```shell
uvx pre-commit install
```

After that, the hooks run automatically whenever you `git commit`. `uvx` ships with [uv](https://docs.astral.sh/uv/getting-started/installation/).

#### Running hooks manually

If you need to run the hooks outside of a commit:

```shell
uvx pre-commit run            # staged files only
uvx pre-commit run --all-files  # entire repo
```
