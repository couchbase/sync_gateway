# ruleguard rules

A directory containing [ruleguard](https://github.com/quasilyte/go-ruleguard) rules run by [go-critic](https://github.com/go-critic/go-critic) via [golangci-lint](https://golangci-lint.run/).

## Manual usage:

```sh
$ ruleguard -rules ruleguard/*.go ./...
db/crud.go:2577:4: logwrappederr: cannot use error wrapping verb %w outside of fmt.Errorf() - use %s or %v instead? (rules-logwrappederr.go:64)

$ golangci-lint --config .golangci.yml run'
db/crud.go:2577:4: ruleguard: cannot use error wrapping verb %w outside of fmt.Errorf() - use %s or %v instead? (gocritic)
			base.WarnfCtx(ctx, "CheckProposedRev(%q) --> %T %w", base.UD(docid), err, err)
			^
```