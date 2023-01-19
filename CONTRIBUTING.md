# Contributing

## How to submit changes or bug reports
Submit an issue or bug with reproduction steps in the issue tag

## Guidelines for contributions
Tests must still pass or have good explanation for change. Only additions to the tests and improvements to test coverage are encouraged.

## Setting up a development environment
`go get` or `go install` puts sqlvet in `$GOPATH` or `$HOME/go`. You're able to fork and the repo into another directory to make changes. `go test ./...` works pretty well for testing changes.

## Development Workflow Requirements
The `const version` needs to be updated with changes before new releases are tagged and published.

## Information on how to report security vulnerabilities
Please go directly to the repository owner with information on a vulnerability or it's fix so it can be patched and released asap.
