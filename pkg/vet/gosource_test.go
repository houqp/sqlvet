package vet_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/houqp/gtest"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/houqp/sqlvet/pkg/schema"
	"github.com/houqp/sqlvet/pkg/vet"
)

type GoSourceTmpDir struct{}

func (s GoSourceTmpDir) Construct(t *testing.T, fixtures struct{}) (string, string) {
	dir, err := ioutil.TempDir("", "gosource-tmpdir")
	assert.NoError(t, err)

	modpath := filepath.Join(dir, "go.mod")
	err = os.WriteFile(modpath, []byte(`
module github.com/houqp/sqlvettest
	`), 0644)
	assert.NoError(t, err)

	return dir, dir
}

func (s GoSourceTmpDir) Destruct(t *testing.T, dir string) {
	os.RemoveAll(dir)
}

func init() {
	gtest.MustRegisterFixture("GoSourceTmpDir", &GoSourceTmpDir{}, gtest.ScopeSubTest)
}

type GoSourceTests struct{}

func (s *GoSourceTests) Setup(t *testing.T) {
	log.SetLevel(log.TraceLevel)
}
func (s *GoSourceTests) Teardown(t *testing.T)   {}
func (s *GoSourceTests) BeforeEach(t *testing.T) {}
func (s *GoSourceTests) AfterEach(t *testing.T)  {}

func (s *GoSourceTests) SubTestInvalidSyntax(t *testing.T, fixtures struct {
	TmpDir string `fixture:"GoSourceTmpDir"`
}) {
	dir := fixtures.TmpDir

	fpath := filepath.Join(dir, "main.go")
	err := os.WriteFile(fpath, []byte(`
package main

func main() {
	return 1
}
`), 0644)
	assert.NoError(t, err)

	_, err = vet.CheckDir(vet.VetContext{}, dir, "", nil)
	assert.Error(t, err)
}

func (s *GoSourceTests) SubTestSkipNoneDbQueryCall(t *testing.T, fixtures struct {
	TmpDir string `fixture:"GoSourceTmpDir"`
}) {
	dir := fixtures.TmpDir

	source := []byte(`
package main

type Parameter struct {}

func (Parameter) Query(s string) error {
	return nil
}

func NewParam() *Parameter {
	return &Parameter{}
}

func main() {
	f := Parameter{}
	f.Query("user_id")

	func() {
		func() {
			// access from outside of closure body scope
			f.Query("user_id")

			// from function call
			// scoped within closure
			flocal := NewParam()
			flocal.Query("book_id")
		}()
	}()
}
	`)

	fpath := filepath.Join(dir, "main.go")
	err := os.WriteFile(fpath, source, 0644)
	assert.NoError(t, err)

	queries, err := vet.CheckDir(vet.VetContext{}, dir, "", nil)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(queries))
}

func (s *GoSourceTests) SubTestPkgDatabaseSql(t *testing.T, fixtures struct {
	TmpDir string `fixture:"GoSourceTmpDir"`
}) {
	dir := fixtures.TmpDir

	source := []byte(`
package main

import (
	"context"
	"database/sql"
	"fmt"
)

func main() {
	db, _ := sql.Open("mysql", "user:password@tcp(127.0.0.1:3306)/hello")
	db.Query("SELECT 1")

	// sqlvet: ignore
	db.Query("SELECT 2")

	db.Query("SELECT 3") //sqlvet: ignore

	db.Query(
		"SELECT 4",
	) //sqlvet:ignore

	db.Query("SELECT 5")
	//   sqlvet:ignore

	// context aware methods
	ctx := context.Background()
	db.QueryRowContext(ctx, "SELECT 5")

	tx, _ := db.Begin()
	tx.ExecContext(ctx, "SELECT 6")

	// unsafe string
	var userInput string
	tx.Query(fmt.Sprintf("SELECT %s", userInput))

	// string concat
	tx.Exec("SELECT " + "7")
	staticUserId := "id"
	tx.Exec("SELECT " + staticUserId + " FROM foo")
}
	`)

	fpath := filepath.Join(dir, "main.go")
	err := os.WriteFile(fpath, source, 0644)
	assert.NoError(t, err)

	queries, err := vet.CheckDir(vet.VetContext{}, dir, "", nil)
	if err != nil {
		t.Fatalf("Failed to load package: %s", err.Error())
		return
	}
	assert.Equal(t, 6, len(queries))
	sort.Slice(queries, func(i, j int) bool {
		return queries[i].Position.Offset < queries[j].Position.Offset
	})

	assert.NoError(t, queries[0].Err)
	assert.Equal(t, "SELECT 1", queries[0].Query)

	assert.NoError(t, queries[1].Err)
	assert.Equal(t, "SELECT 5", queries[1].Query)

	assert.NoError(t, queries[2].Err)
	assert.Equal(t, "SELECT 6", queries[2].Query)

	// unsafe string
	assert.Error(t, queries[3].Err)

	// string concat
	assert.NoError(t, queries[4].Err)
	assert.Equal(t, "SELECT 7", queries[4].Query)
	assert.NoError(t, queries[5].Err)
	assert.Equal(t, "SELECT id FROM foo", queries[5].Query)
}

func (s *GoSourceTests) SubTestPkgDatabaseSqlxWithCustomWrapperAndRebindForDB(t *testing.T, fixtures struct {
	TmpDir string `fixture:"GoSourceTmpDir"`
}) {
	dir := fixtures.TmpDir

	source := []byte(`
package main

import (
	"context"
	"github.com/jmoiron/sqlx"
	"database/sql"
)

type CustomWrapper interface {
	sqlx.QueryerContext
	sqlx.ExecerContext

	SelectContext(ctx context.Context, dest any, query string, args ...any) error
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error)
	BindNamed(query string, arg any) (string, []any, error)
	Rebind(query string) string
}

func main() {
	var ctx = context.Background()
	const queryTmpl = "SELECT * FROM test_schema.test_table WHERE id IN (?);"
	var query, data, err = sqlx.In(queryTmpl, getArgs()) // sqlvet: ignore
	if err != nil {
		panic(err)
	}

	var db CustomWrapper = &sqlx.DB{}
	var entities []any
	if err := db.GetContext(ctx, &entities, db.Rebind(query), data...); err != nil {
		panic(err) 
	}
}

func getArgs() []string {
	return []string{"one","two"}
}
	`)

	fpath := filepath.Join(dir, "main.go")
	err := os.WriteFile(fpath, source, 0644)
	assert.NoError(t, err)

	gomod := `
module github.com/houqp/sqlvet

go 1.24.0

require github.com/jmoiron/sqlx v1.4.0`
	fpathGoMod := filepath.Join(dir, "go.mod")
	err = os.WriteFile(fpathGoMod, []byte(gomod), 0644)
	assert.NoError(t, err)

	gosum := `
	github.com/jmoiron/sqlx v1.4.0 h1:1PLqN7S1UYp5t4SrVVnt4nUVNemrDAtxlulVe+Qgm3o=
	github.com/jmoiron/sqlx v1.4.0/go.mod h1:ZrZ7UsYB/weZdl2Bxg6jCRO9c3YHl8r3ahlKmRT4JLY=
	`
	fpathGoSum := filepath.Join(dir, "go.sum")
	err = os.WriteFile(fpathGoSum, []byte(gosum), 0644)
	assert.NoError(t, err)

	queries, err := vet.CheckDir(vet.VetContext{}, dir, "", nil)
	if err != nil {
		t.Fatalf("Failed to load package: %s", err.Error())
		return
	}
	assert.Equal(t, 0, len(queries))
}

func (s *GoSourceTests) SubTestPkgDatabaseSqlxWithCustomWrapperAndTypeConversion(t *testing.T, fixtures struct {
	TmpDir string `fixture:"GoSourceTmpDir"`
}) {
	dir := fixtures.TmpDir

	source := []byte(`
package main

import (
	"context"
	"github.com/jmoiron/sqlx"
	"database/sql"
)

type CustomWrapper interface {
	sqlx.QueryerContext
	sqlx.ExecerContext

	SelectContext(ctx context.Context, dest any, query string, args ...any) error
	GetContext(ctx context.Context, dest any, query string, args ...any) error
	NamedExecContext(ctx context.Context, query string, arg any) (sql.Result, error)
	BindNamed(query string, arg any) (string, []any, error)
	Rebind(query string) string
}

func main() {
	var ctx = context.Background()
	const query = "SELECT CASE WHEN id = 'test1' AND coalesce((jsonbField::jsonb)->>'in_field', '') <> 'test2' THEN (jsonbField::jsonb) - 'in_field_2' ELSE jsonbField::jsonb END AS jsonbField FROM test_schema.test_tbl WHERE id = $1;"

	var db CustomWrapper = &sqlx.DB{}
	var entities []any
	if err := db.SelectContext(ctx, &entities, query, "test1"); err != nil {
		panic(err)
	}
}
	`)

	fpath := filepath.Join(dir, "main.go")
	err := os.WriteFile(fpath, source, 0644)
	assert.NoError(t, err)

	gomod := `
module github.com/houqp/sqlvet

go 1.24.0

require github.com/jmoiron/sqlx v1.4.0`
	fpathGoMod := filepath.Join(dir, "go.mod")
	err = os.WriteFile(fpathGoMod, []byte(gomod), 0644)
	assert.NoError(t, err)

	gosum := `
	github.com/jmoiron/sqlx v1.4.0 h1:1PLqN7S1UYp5t4SrVVnt4nUVNemrDAtxlulVe+Qgm3o=
	github.com/jmoiron/sqlx v1.4.0/go.mod h1:ZrZ7UsYB/weZdl2Bxg6jCRO9c3YHl8r3ahlKmRT4JLY=
	`
	fpathGoSum := filepath.Join(dir, "go.sum")
	err = os.WriteFile(fpathGoSum, []byte(gosum), 0644)
	assert.NoError(t, err)

	schemaCtx := vet.NewContext(map[string]schema.Table{
		"test_tbl": {
			Name: "test_tbl",
			Columns: map[string]schema.Column{
				"id":         {Name: "id", Type: "text"},
				"jsonbfield": {Name: "jsonbfield", Type: "jsonb"},
			},
		},
	})

	queries, err := vet.CheckDir(schemaCtx, dir, "", nil)
	if err != nil {
		t.Fatalf("Failed to load package: %s", err.Error())
		return
	}

	// Expect 2 queries (one for DB, one for Tx implementation)
	// Both should validate successfully with :: operators preserved
	assert.Equal(t, 2, len(queries))
	for _, q := range queries {
		assert.NoError(t, q.Err)
		assert.Contains(t, q.Query, "::")
	}
}

// run sqlvet from parent dir
func (s *GoSourceTests) SubTestCheckRelativeDir(t *testing.T, fixtures struct {
	TmpDir string `fixture:"GoSourceTmpDir"`
}) {
	dir := fixtures.TmpDir

	source := []byte(`
package main

func main() {
}
	`)

	fpath := filepath.Join(dir, "main.go")
	err := os.WriteFile(fpath, source, 0644)
	assert.NoError(t, err)

	cwd, err := os.Getwd()
	assert.NoError(t, err)
	parentDir := filepath.Dir(dir)
	os.Chdir(parentDir)
	defer os.Chdir(cwd)

	queries, err := vet.CheckDir(vet.VetContext{}, filepath.Base(dir), "", nil)
	if err != nil {
		t.Fatalf("Failed to load package: %s", err.Error())
		return
	}
	assert.Equal(t, 0, len(queries))
}

func TestGoSource(t *testing.T) {
	gtest.RunSubTests(t, &GoSourceTests{})
}

func (s *GoSourceTests) SubTestQueryParam(t *testing.T, fixtures struct {
	TmpDir string `fixture:"GoSourceTmpDir"`
}) {
	dir := fixtures.TmpDir

	source := []byte(`
package main

import (
	"context"
	"database/sql"
)

func main() {
	db, _ := sql.Open("mysql", "user:password@tcp(127.0.0.1:3306)/hello")

	db.Query("SELECT 2 FROM foo WHERE id=$1", 1)

	db.Exec("UPDATE foo SET id = $1", 10)

	ctx := context.Background()
	tx, _ := db.Begin()
	tx.ExecContext(ctx, "INSERT INTO foo (id, value) VALUES ($1, $2)", 1, "hello")

	db.Query("SELECT 2 FROM foo WHERE id=$1 OR value=$1", 1)
}
	`)

	fpath := filepath.Join(dir, "main.go")
	err := os.WriteFile(fpath, source, 0644)
	assert.NoError(t, err)

	queries, err := vet.CheckDir(vet.VetContext{}, dir, "", nil)
	if err != nil {
		t.Fatalf("Failed to load package: %s", err.Error())
		return
	}
	assert.Equal(t, 4, len(queries))
	sort.Slice(queries, func(i, j int) bool {
		return queries[i].Position.Offset < queries[j].Position.Offset
	})

	assert.NoError(t, queries[0].Err)
	assert.Equal(t, "SELECT 2 FROM foo WHERE id=$1", queries[0].Query)
	assert.Equal(t, 1, queries[0].ParameterArgCount)

	assert.NoError(t, queries[1].Err)
	assert.Equal(t, "UPDATE foo SET id = $1", queries[1].Query)
	assert.Equal(t, 1, queries[1].ParameterArgCount)

	assert.NoError(t, queries[2].Err)
	assert.Equal(t, "INSERT INTO foo (id, value) VALUES ($1, $2)", queries[2].Query)
	assert.Equal(t, 2, queries[2].ParameterArgCount)

	assert.NoError(t, queries[3].Err)
	assert.Equal(t, "SELECT 2 FROM foo WHERE id=$1 OR value=$1", queries[3].Query)
	assert.Equal(t, 1, queries[3].ParameterArgCount)
}

func (s *GoSourceTests) SubTestBuildFlags(t *testing.T, fixtures struct {
	TmpDir string `fixture:"GoSourceTmpDir"`
}) {
	dir := fixtures.TmpDir

	source := []byte(`
//+build myBuildTag

package main

import (
	"fmt"
)

func main() {
	fmt.Printf("Hello World\n")
}
`)

	fpath := filepath.Join(dir, "main.go")
	err := os.WriteFile(fpath, source, 0644)
	assert.NoError(t, err)

	_, err = vet.CheckDir(vet.VetContext{}, dir, "", nil)
	assert.NoError(t, err)

	_, err = vet.CheckDir(vet.VetContext{}, dir, "-tags myBuildTag", nil)
	assert.NoError(t, err)
}
