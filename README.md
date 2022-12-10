# Sqlvet

Sqlvet performs static analysis on raw SQL queries in your Go code base to
surface potential runtime errors at build time.

Feature highlights:

* Check for SQL syntax error
* Identify unsafe queries that could potentially lead to SQL injections
* For INSERT statements, make sure column count matches value count
* Validate table names
* Validate column names

TODO:

* Validate query function argument count and types
* Type check value list in UPDATE query
* rename to psqlvet for delineation (unlike forked repo, no plan to support mysql)


## Usage

### Installation

```
$ go get github.com/samiam2013/sqlvet
```

### Zero conf

SqlVet should work out of the box for any Go project using go modules:

```
$ sqlvet .
[!] No schema specified, will run without table and column validation.
Checked 10 SQL queries.
🎉 Everything is awesome!
```

Note: unreachable code will be skipped.


### Schema validation

To enable more in-depth analysis, create a `sqlvet.toml` config file at the
root of your project and specify the path to a database schema file:

```
$ cat ./sqlvet.toml
schema_path = "schema/full_schema.sql"

$ sqlvet .
Loaded DB schema from schema/full_schema.sql
        table alembic_version with 1 columns
        table incident with 13 columns
        table usr with 4 columns
Exec @ ./pkg/incident.go:75:19
        UPDATE incident SET oops = $1 WHERE id = $2

        ERROR: column `oops` is not defined in table `incident`

Checked 10 SQL queries.
Identified 1 errors.
```

### Customer query functions and libraries

By default, sqlvet checks all calls to query function in `database/sql`,
   `github.com/jmoiron/sqlx`, `github.com/jinzhu/gorm` and `go-gorp/gorp`
   libraries. You can however configure it to white-list arbitrary query
   functions like below:

```toml
[[sqlfunc_matchers]]
  pkg_path = "github.com/mattermost/gorp"
  [[sqlfunc_matchers.rules]]
    query_arg_name = "query"
    query_arg_pos  = 0
  [[sqlfunc_matchers.rules]]
    query_arg_name = "sql"
    query_arg_pos  = 0
```

The above config tells sqlvet to analyze any function/method from
`github.com/mattermost/gorp` package that has the first parameter named either
`query` or `sql`.

You can also match query functions by names:

```toml
[[sqlfunc_matchers]]
  pkg_path = "github.com/jmoiron/sqlx"
  [[sqlfunc_matchers.rules]]
    func_name = "NamedExecContext"
    query_arg_pos  = 1
```

The above config tells sqlvet to analyze the second parameter of any
function/method named `NamedExecContext` in `github.com/jmoiron/sqlx` package.


### Ignore false positives

To skip a false positive, annotate the relevant line with `sqlvet: ignore`
comment:

```go
func foo() {
    Db.Query(fmt.Sprintf("SELECT %s", "1")) // sqlvet: ignore
}
```


## Acknowledgements

Sqlvet was inspired by [safesql](https://github.com/stripe/safesql) and
[sqlc](https://github.com/kyleconroy/sqlc).
