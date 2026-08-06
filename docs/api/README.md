# Sync Gateway OpenAPI Specs

This directory contains the OpenAPI specs for the Sync Gateway REST API.

The recommended tool to work with these specs is [Redocly](https://redoc.ly/).

The specs are split across `paths/`, `components/`, and per-audience roots (`admin.yaml`,
`public.yaml`, `metric.yaml`, `diagnostic.yaml`, and their `-capella` variants). Which roots are
linted, and the decorators applied to each, are declared in `.redocly.yaml` at the repository root.

Changing a REST handler, a query parameter, or a response schema means updating these specs.

## Linting

Validates every API root defined in `.redocly.yaml`:

```sh
$ npm ci
$ npx redocly lint --config=.redocly.yaml --format=stylish
```

Redocly is pinned in `package.json` and `package-lock.json`. `npm ci` installs that exact version.

This is wired into both the `redocly-lint` pre-commit hook (which fires on any change under
`docs/api/` or to `.redocly.yaml`) and the `openapi` CI workflow, so it usually runs without you
invoking it directly. The hook runs `npm ci` for you, so it needs no setup. `yamllint` also runs
over this directory in both places.

## Preview

```sh
$ npx redocly preview-docs --config=.redocly.yaml
```
