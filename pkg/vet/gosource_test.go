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
	"golang.org/x/tools/go/analysis/analysistest"
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

func (s *GoSourceTests) runAnalyzer(t *testing.T, dir, packageName string) []*vet.QuerySite {
	var result []*vet.QuerySite
	analyzer := vet.NewAnalyzer(vet.VetContext{}, nil, func(r *vet.QuerySite) {
		result = append(result, r)
	})
	analysistest.Run(t, dir, analyzer, packageName)
	return result
}

func (s *GoSourceTests) SubTestSkipNoneDbQueryCall(t *testing.T, fixtures struct {
	TmpDir string `fixture:"GoSourceTmpDir"`
}) {

	dir, cleanup, err := analysistest.WriteFiles(map[string]string{
		"main/main.go": `package main

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
	`})
	assert.NoError(t, err)
	defer cleanup()

	_ = s.runAnalyzer(t, dir, "main")
}

func (s *GoSourceTests) SubTestPkgDatabaseSql(t *testing.T, fixtures struct {
	TmpDir string `fixture:"GoSourceTmpDir"`
}) {
	dir := fixtures.TmpDir

	dir, cleanup, err := analysistest.WriteFiles(map[string]string{
		"main/main.go": `package main

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

  // const string
	tx, _ = db.Begin()
  const query = "SELECT 7"
	tx.ExecContext(ctx, query)
}
	`})
	assert.NoError(t, err)
	defer cleanup()

	queries := s.runAnalyzer(t, dir, "main")

	assert.Equal(t, 5, len(queries))
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

	assert.NoError(t, queries[4].Err)
	assert.Equal(t, "SELECT 7", queries[4].Query)
}

func (s *GoSourceTests) SubTestQueryParam(t *testing.T, fixtures struct {
	TmpDir string `fixture:"GoSourceTmpDir"`
}) {
	const source = `
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
	`

	dir, cleanup, err := analysistest.WriteFiles(map[string]string{"main/main.go": source})
	assert.NoError(t, err)
	defer cleanup()
	queries := s.runAnalyzer(t, dir, "main")

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

func TestGoSource(t *testing.T) {
	gtest.RunSubTests(t, &GoSourceTests{})
}
