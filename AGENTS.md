# AGENTS.md

## Scope

- Work **only inside this repository** (the current workspace root).
- Do not read, create, or modify files outside the current directory.
- Do not edit `vendor/` by hand; manage dependencies through `go.mod`/`go.sum`.

## Project

`kafkaquarius` is a Go CLI for Kafka message search, filtering, migration, backup,
and restore. Filters/transforms are CEL expressions (see `examples/*.txt`).

- Module: `kafkaquarius` (`go.mod`, Go 1.25)
- Entrypoint: `cmd/main.go`
- Packages: `internal/app.go`, `internal/cel`, `internal/config`,
  `internal/consumer`, `internal/domain`
- The VS Code launch config expects `examples/config.yaml`.

## Go environment

Go tooling is configured in `.vscode/settings.json`:

- `go.gopath` and `terminal.integrated.env.linux.GOPATH` set the Go workspace path.
  It is machine-specific, so never hardcode it in files or commands. Instead, read
  the value from `.vscode/settings.json` and use it for the current shell:

  ```bash
  export GOPATH="$(sed -n 's/.*"GOPATH": "\([^"]*\)".*/\1/p' .vscode/settings.json)"
  ```

- Run all Go commands (`make`, `go build`, `go test`, `go vet`, `go run`) in a
  shell where `GOPATH` is set this way, from the repository root.
- VS Code launch config `main.go: debug` runs `cmd/main.go` with
  `--config=${workspaceFolder}/examples/config.yaml` from the workspace root.

Do not change these settings unless explicitly asked.

## Build and dependencies

Use the `Makefile` instead of ad-hoc build commands:

```bash
make deps    # go mod download
make build   # same as: make go.build
make clean   # same as: make go.clean
```

`make build` runs `go build` for `linux/amd64` with CGO static flags and writes
`build/bin/kafkaquarius-current-linux`. Override `pkgver`, `builddir`, or
`mainpath` on the command line if needed. An optional `env.file=<path>` argument
loads variables from a file.

## Tests and checks

There is no `make test` target; run Go tooling directly:

```bash
go test ./...
go vet ./...
gofmt -l cmd internal
```

Use `gofmt -l cmd internal` rather than `gofmt -l .`: the repository contains
`.go/` (module cache) and `vendor/`, which must not be reformatted or scanned.

Add or update `_test.go` files for changed code. Tests live next to the code
(e.g. `internal/cel/cel_test.go`). Fix all test, vet, and formatting issues before
finishing.

## Docker

```bash
make docker.up    # build and run compose environment
make docker.up background=yes
make docker.down
```

## Local run

```bash
go run ./cmd/main.go --config=examples/config.yaml
```

Other run configurations are in `.run/` (JetBrains) and `.vscode/launch.json`.

## Conventions

- Keep the existing package layout and naming (`internal/<area>`).
- Follow standard Go style; run `gofmt` on changed files.
- Do not add comments unless they explain non-obvious behavior.
- Do not commit generated binaries, logs, or build artifacts (`build/bin`,
  `*.test`, `full.log`) — they are git-ignored.
