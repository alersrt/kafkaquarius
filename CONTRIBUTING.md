Contribution Guide
==================

## Requirements

- If you don't use docker-wrapped commands, make sure that tools you use have the same version as in docker-wrapped
  commands. It's latest version, mainly.

## Operations

Take a look at [`Makefile`][1] for commands usage details.

### Dependencies

To preload project dependencies use docker-wrapped command from [`Makefile`][1]:

```bash
make deps
```

### Building

To build/rebuild binaries use command from [`Makefile`][1]:

```bash
make build
```

[1]: Makefile