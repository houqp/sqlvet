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

	"github.com/houqp/sqlvet/pkg/vet"
)

type GoSourceTmpDir struct{}

func (s GoSourceTmpDir) Construct(t *testing.T, fixtures struct{}) (string, string) {
	dir, err := ioutil.TempDir("", "gosource-tmpdir")
	assert.NoError(t, err)

	modpath := filepath.Join(dir, "go.mod")
	err = ioutil.WriteFile(modpath, []byte(`
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
	err := ioutil.WriteFile(fpath, []byte(`
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
	err := ioutil.WriteFile(fpath, source, 0644)
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
	err := ioutil.WriteFile(fpath, source, 0644)
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
	err := ioutil.WriteFile(fpath, source, 0644)
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
	err := ioutil.WriteFile(fpath, source, 0644)
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
	err := ioutil.WriteFile(fpath, source, 0644)
	assert.NoError(t, err)

	_, err = vet.CheckDir(vet.VetContext{}, dir, "", nil)
	assert.Error(t, err)

	_, err = vet.CheckDir(vet.VetContext{}, dir, "-tags myBuildTag", nil)
	assert.NoError(t, err)
}
