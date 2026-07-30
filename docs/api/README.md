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
$ npx --yes @redocly/cli@2 lint --config=.redocly.yaml --format=stylish
```

This is wired into both the `redocly-lint` pre-commit hook (which fires on any change under
`docs/api/` or to `.redocly.yaml`) and the `openapi` CI workflow, so it usually runs without you
invoking it directly. `yamllint` also runs over this directory in both places.

## Preview

```sh
$ npx --yes @redocly/cli@2 preview-docs --config=.redocly.yaml
```
