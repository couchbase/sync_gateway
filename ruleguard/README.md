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

## Layout

These files are part of the main `github.com/couchbase/sync_gateway` module, so pre-commit,
golangci-lint, govulncheck and Dependabot all reach them.

Every `.go` file here is behind the `ruleguard` build tag, so none of it enters a normal build or
test run — golangci-lint sets the tag (see `run.build-tags`) to lint the rule files. To compile the
directory by hand use `go build -tags ruleguard ./ruleguard/...`; `go test` needs `-vet=off` as well,
because the fixtures below misuse `%w` on purpose and `go vet`'s printf check objects.

- `rules-*.go` — the rules themselves, loaded by gocritic via the `gocritic.settings.ruleguard`
  block in `.golangci.yml` / `.golangci-strict.yml`.
- `*_test.go` — fixtures, not Go tests. They deliberately contain rule violations, which is why
  golangci-lint excludes `ruleguard:` findings for them. Each one records the number of valid and
  invalid usages it should produce when the rules are run against it.
- `internal/test_pkg/base` — a stand-in for `base`, so the fixtures can call the logging wrappers
  the rules match on without importing the real package.
