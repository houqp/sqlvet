package vet_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/houqp/sqlvet/pkg/schema"
	"github.com/houqp/sqlvet/pkg/vet"
)

var mockDbSchema = &schema.Db{
	Tables: map[string]schema.Table{
		"foo": {
			Name: "foo",
			Columns: map[string]schema.Column{
				"id": {
					Name: "id",
					Type: "int",
				},
				"value": {
					Name: "value",
					Type: "varchar",
				},
			},
		},
		"bar": {
			Name: "bar",
			Columns: map[string]schema.Column{
				"id": {
					Name: "id",
					Type: "int",
				},
				"count": {
					Name: "count",
					Type: "int",
				},
			},
		},
	},
}

var mockCtx = vet.VetContext{Schema: mockDbSchema}

func TestInsert(t *testing.T) {
	testCases := []struct {
		Name  string
		Query string
	}{
		{
			"insert",
			`INSERT INTO foo (id) VALUES (1)`,
		},
		{
			"insert with select",
			`INSERT INTO foo (id)
			SELECT bar.id
			FROM bar
			WHERE bar.id = 1`,
		},
		{
			"insert with select and const",
			`INSERT INTO foo (id, value)
			SELECT bar.id, 'test'
			FROM bar
			WHERE bar.id = 1`,
		},
		{
			"insert with subquery",
			`INSERT INTO foo (id, value)
			VALUES (
				(
					SELECT id
					FROM bar
					WHERE bar.id = 2
				),
				'test'
			)`,
		},
		{
			"insert with return",
			`INSERT INTO foo (id) VALUES (1) RETURNING value`,
		},
	}

	for _, tcase := range testCases {
		t.Run(tcase.Name, func(t *testing.T) {
			qparams, err := vet.ValidateSqlQuery(mockCtx, tcase.Query)
			if err != nil {
				vet.DebugQuery(tcase.Query)
			}
			assert.NoError(t, err)
			assert.Equal(t, 0, len(qparams))
		})
	}
}

func TestInvalidInsert(t *testing.T) {
	testCases := []struct {
		Name  string
		Query string
		Err   error
	}{
		{
			"invalid syntax",
			`INSERT INTO foo (id,) VALUES ($1)`,
			errors.New("syntax error at or near \")\""),
		},
		{
			"invalid table",
			`INSERT INTO foononexist (id) VALUES ($1)`,
			errors.New("invalid table name: foononexist"),
		},
		{
			"invalid column",
			`INSERT INTO foo (id, date, value) VALUES ($1, $2, $3)`,
			errors.New("column `date` is not defined in table `foo`"),
		},
		{
			"not enough values",
			`INSERT INTO foo (id, value) VALUES ($1)`,
			errors.New("Column count 2 doesn't match value count 1."),
		},
		{
			"too many values",
			`INSERT INTO foo (id, value) VALUES ($1, $2, $3)`,
			errors.New("Column count 2 doesn't match value count 3."),
		},
		{
			"invalid column in value list",
			`INSERT INTO foo (id) VALUES (oops)`,
			errors.New("column `oops` is not defined in table `foo`"),
		},
		{
			"invalid column in value list as expression",
			`INSERT INTO foo (id) VALUES (oops+1)`,
			errors.New("column `oops` is not defined in table `foo`"),
		},
		{
			"invalid table from select",
			`INSERT INTO foo (id, value)
			SELECT id, count
			FROM barr
			WHERE bar.id=2`,
			errors.New("invalid table name: barr"),
		},
		{
			"invalid table from select target",
			`INSERT INTO foo (id, value)
			SELECT bar.id
			FROM foo
			WHERE foo.id=1`,
			errors.New("table `bar` not available for query"),
		},
		{
			"invalid column from select",
			`INSERT INTO foo (id, value)
			SELECT bar.id, bar.value
			FROM bar
			WHERE bar.id=2`,
			errors.New("column `value` is not defined in table `bar`"),
		},
		{
			"invalid column from subselect in select",
			`INSERT INTO foo (id, value)
			SELECT bar.id, (SELECT 'test' FROM bar WHERE ida = 1)
			FROM bar
			WHERE bar.id=2`,
			fmt.Errorf(
				"Invalid SELECT query in value list: %w",
				errors.New("column `ida` is not defined in table `bar`")),
		},
		{
			"invalid table from select join",
			`INSERT INTO foo (id, value)
			SELECT bar.id, bar.count
			FROM bar
			JOIN barrr b2 ON b2.uid=bar.id
			WHERE bar.id=2`,
			errors.New("invalid table name: barrr"),
		},
		{
			"invalid column from select join",
			`INSERT INTO foo (id, value)
			SELECT bar.id, bar.count
			FROM bar
			JOIN bar b2 ON b2.uid=bar.id
			WHERE bar.id=2`,
			errors.New("column `uid` is not defined in table `b2`"),
		},
		{
			"invalid column from subquery",
			`INSERT INTO foo (id, value)
			VALUES (
				(
					SELECT ida
					FROM bar
					WHERE bar.id = 2
				),
				'test'
			)`,
			fmt.Errorf(
				"Invalid value list: %w",
				errors.New("column `ida` is not defined in table `bar`")),
		},
		{
			"insert with invalud column return",
			`INSERT INTO foo (id) VALUES (1) RETURNING uid`,
			errors.New("column `uid` is not defined in table `foo`"),
		},
	}

	for _, tcase := range testCases {
		t.Run(tcase.Name, func(t *testing.T) {
			qparams, err := vet.ValidateSqlQuery(mockCtx, tcase.Query)
			if err == nil {
				vet.DebugQuery(tcase.Query)
			}
			assert.Equal(t, tcase.Err, err)
			assert.Equal(t, 0, len(qparams))
		})
	}
}

func TestInvalidSelect(t *testing.T) {
	testCases := []struct {
		Name  string
		Query string
		Err   error
	}{
		{
			"invalid syntax",
			`SELECT id, FROM foo`,
			errors.New("syntax error at or near \"FROM\""),
		},
		{
			"invalid table",
			`SELECT id FROM foononexist`,
			errors.New("invalid table name: foononexist"),
		},
		{
			"invalid table in target list",
			`SELECT foononexist.id FROM foo`,
			errors.New("table `foononexist` not available for query"),
		},
		{
			"invalid target column",
			`SELECT id, date, value FROM foo`,
			errors.New("column `date` is not defined in table `foo`"),
		},
		{
			"invalid column in where clause",
			`SELECT id, value FROM foo WHERE date=NOW() AND 'a'='a'`,
			errors.New("column `date` is not defined in table `foo`"),
		},
		{
			"invalid column in where null test",
			`SELECT id, value FROM foo WHERE date IS NULL`,
			errors.New("column `date` is not defined in table `foo`"),
		},
		{
			"invalid table in join",
			`SELECT id, value FROM foo JOIN barr ON foo.id=barr.id`,
			errors.New("invalid table name: barr"),
		},
		{
			"invalid column in join",
			`SELECT bar.id, value FROM foo LEFT JOIN bar ON foo.id=bar.uid WHERE foo.id=1`,
			errors.New("column `uid` is not defined in table `bar`"),
		},
		{
			"invalid column in select with multiple joins",
			`SELECT id
			FROM foo
			LEFT JOIN bar b1 ON b1.id = foo.id
			LEFT JOIN bar b ON b.date = foo.id
			LEFT JOIN foo f ON f.id = foo.id
			LEFT JOIN foo f2 ON f2.id = foo.id
			WHERE value IS NULL`,
			errors.New("column `date` is not defined in table `b`"),
		},
		{
			"invalid column in order by",
			`SELECT id, value FROM foo ORDER BY oops`,
			errors.New("column `oops` is not defined in table `foo`"),
		},
		{
			"invalid column in multiple order by",
			`SELECT id, value FROM foo ORDER BY id, oops`,
			errors.New("column `oops` is not defined in table `foo`"),
		},
		{
			"invalid column in having",
			`SELECT MAX(id), value FROM foo GROUP BY value HAVING MAX(uid) > 1`,
			errors.New("column `uid` is not defined in table `foo`"),
		},
		{
			"invalid column in having with AND",
			`SELECT MAX(id), value FROM foo GROUP BY value HAVING MAX(oops) > 1 AND MAX(id) < 10`,
			errors.New("column `oops` is not defined in table `foo`"),
		},
	}

	for _, tcase := range testCases {
		t.Run(tcase.Name, func(t *testing.T) {
			qparams, err := vet.ValidateSqlQuery(mockCtx, tcase.Query)
			if err == nil {
				vet.DebugQuery(tcase.Query)
			}
			assert.Equal(t, tcase.Err, err)
			assert.Equal(t, 0, len(qparams))
		})
	}
}

// FIXME support ::regclass:
// `SELECT COUNT(0)
// FROM   pg_attribute
// WHERE  attrelid = $1::regclass
// AND    attname = $2
// AND    NOT attisdropped`,

func TestSelect(t *testing.T) {
	testCases := []struct {
		Name  string
		Query string
	}{
		{
			"ping",
			`SELECT 1`,
		},
		{
			"ping with table",
			`SELECT 1 FROM foo`,
		},
		{
			"select one column",
			`SELECT id FROM foo`,
		},
		{
			"select one column with table prefix",
			`SELECT foo.id FROM foo`,
		},
		{
			"select all columns",
			`SELECT * FROM foo`,
		},
		{
			"select with where",
			`SELECT id FROM foo WHERE value='bar' AND id=1`,
		},
		{
			"select with null test",
			`SELECT id FROM foo WHERE value IS NULL`,
		},
		{
			"select with multiple joins",
			`SELECT id
			FROM foo
			LEFT JOIN bar b ON b.id = foo.id
			LEFT JOIN foo f ON f.id = foo.id
			WHERE value IS NULL`,
		},
	}

	for _, tcase := range testCases {
		t.Run(tcase.Name, func(t *testing.T) {
			qparams, err := vet.ValidateSqlQuery(mockCtx, tcase.Query)
			if err != nil {
				vet.DebugQuery(tcase.Query)
			}
			assert.NoError(t, err)
			assert.Equal(t, 0, len(qparams))
		})
	}
}

func TestUpdate(t *testing.T) {
	testCases := []struct {
		Name  string
		Query string
	}{
		{
			"update all to null",
			`UPDATE foo SET value=NULL`,
		},
		{
			"update id for all",
			`UPDATE foo SET id=1`,
		},
		{
			"update with where",
			`UPDATE foo SET value='bar' WHERE id > 1 AND value IS NULL`,
		},
		{
			"update with boolean in where",
			`UPDATE foo SET value='bar' WHERE True`,
		},
		{
			"update with list in where",
			`UPDATE foo SET value='bar' WHERE id IN (1, 2, 3)`,
		},
		{
			"update with from",
			`UPDATE foo SET value=count FROM bar WHERE bar.id=1`,
		},
		{
			"update with returning",
			`UPDATE foo SET id=1 RETURNING value`,
		},
	}

	for _, tcase := range testCases {
		t.Run(tcase.Name, func(t *testing.T) {
			qparams, err := vet.ValidateSqlQuery(mockCtx, tcase.Query)
			if err != nil {
				vet.DebugQuery(tcase.Query)
			}
			assert.NoError(t, err)
			assert.Equal(t, 0, len(qparams))
		})
	}
}

func TestInvalidUpdate(t *testing.T) {
	testCases := []struct {
		Name  string
		Query string
		Err   error
	}{
		{
			"invalid syntax",
			`UPDATE foo, SET id=1`,
			errors.New("syntax error at or near \",\""),
		},
		{
			"invalid table",
			`UPDATE foononexist SET id=1`,
			errors.New("invalid table name: foononexist"),
		},
		{
			"invalid column",
			`UPDATE foo SET date=NOW()`,
			errors.New("column `date` is not defined in table `foo`"),
		},
		{
			"invalid column in where clause",
			`UPDATE foo SET value='bar' WHERE date=NOW() OR 1=1`,
			errors.New("column `date` is not defined in table `foo`"),
		},
		{
			"invalid table in from clause",
			`UPDATE foo SET value=count FROM foononexist WHERE foononexist.id=1`,
			errors.New("invalid table name: foononexist"),
		},
		{
			"invalid column in from clause",
			`UPDATE foo SET value=valuecount FROM bar WHERE bar.id=1`,
			errors.New("column `valuecount` is not defined in any of the table available for query"),
		},
		{
			"invalid column in returning",
			`UPDATE foo SET id=1 RETURNING date`,
			errors.New("column `date` is not defined in table `foo`"),
		},
	}

	for _, tcase := range testCases {
		t.Run(tcase.Name, func(t *testing.T) {
			qparams, err := vet.ValidateSqlQuery(mockCtx, tcase.Query)
			assert.Equal(t, tcase.Err, err)
			assert.Equal(t, 0, len(qparams))
		})
	}
}

func TestDelete(t *testing.T) {
	testCases := []struct {
		Name  string
		Query string
	}{
		{
			"delete all",
			`DELETE FROM foo`,
		},
		{
			"delete with where",
			`DELETE FROM foo WHERE id=1 AND value='bar'`,
		},
		{
			"delete with where and subquery",
			`DELETE FROM foo WHERE id = (SELECT id FROM foo WHERE id=2 LIMIT 1)`,
		},
		{
			"delete with returning",
			`DELETE FROM foo RETURNING id`,
		},
	}

	for _, tcase := range testCases {
		t.Run(tcase.Name, func(t *testing.T) {
			qparams, err := vet.ValidateSqlQuery(mockCtx, tcase.Query)
			if err != nil {
				vet.DebugQuery(tcase.Query)
			}
			assert.NoError(t, err)
			assert.Equal(t, 0, len(qparams))
		})
	}
}

func TestInvalidDelete(t *testing.T) {
	testCases := []struct {
		Name  string
		Query string
		Err   error
	}{
		{
			"invalid syntax",
			`DELETE FROM foo, WHERE id=1`,
			errors.New("syntax error at or near \",\""),
		},
		{
			"invalid table",
			`DELETE FROM foononexist WHERE id=1`,
			errors.New("invalid table name: foononexist"),
		},
		{
			"invalid column",
			`DELETE FROM foo WHERE date=NOW()`,
			errors.New("column `date` is not defined in table `foo`"),
		},
		{
			"invalid column",
			`DELETE FROM foo WHERE bar=NOW() AND id=1`,
			errors.New("column `bar` is not defined in table `foo`"),
		},
		{
			"invalid column",
			`DELETE FROM foo WHERE bar=NOW() OR id=1`,
			errors.New("column `bar` is not defined in table `foo`"),
		},
		{
			"invalid column",
			`DELETE FROM foo WHERE bar=NOW() AND 1=1 OR id=1`,
			errors.New("column `bar` is not defined in table `foo`"),
		},
		{
			"invalid column in where subquery",
			`DELETE FROM foo WHERE id = (SELECT id FROM foo WHERE date=NOW())`,
			fmt.Errorf(
				"Invalid WHERE clause: %w",
				errors.New("column `date` is not defined in table `foo`")),
		},
		{
			"invalid table in where subquery",
			`DELETE FROM foo WHERE id = (SELECT id FROM foononexist WHERE id=1)`,
			fmt.Errorf(
				"Invalid WHERE clause: %w",
				errors.New("invalid table name: foononexist")),
		},
		{
			"invalid column in return clause",
			`DELETE FROM foo RETURNING uid`,
			errors.New("column `uid` is not defined in table `foo`"),
		},
	}

	for _, tcase := range testCases {
		t.Run(tcase.Name, func(t *testing.T) {
			qparams, err := vet.ValidateSqlQuery(mockCtx, tcase.Query)
			assert.Equal(t, tcase.Err, err)
			assert.Equal(t, 0, len(qparams))
		})
	}
}

func TestQueryParams(t *testing.T) {
	testCases := []struct {
		Name   string
		Query  string
		Params []vet.QueryParam
	}{
		{
			"select",
			"SELECT id FROM foo WHERE value=$1",
			[]vet.QueryParam{
				{1},
			},
		},
		{
			"update",
			"UPDATE foo SET value=$1 WHERE id=$2",
			[]vet.QueryParam{
				{1},
				{2},
			},
		},
		{
			"insert",
			"INSERT INTO foo (id, value) VALUES ($1, $2)",
			[]vet.QueryParam{
				{1},
				{2},
			},
		},
		{
			"delete",
			"DELETE FROM foo WHERE id=$1",
			[]vet.QueryParam{
				{1},
			},
		},
	}

	for _, tcase := range testCases {
		t.Run(tcase.Name, func(t *testing.T) {
			qparams, err := vet.ValidateSqlQuery(mockCtx, tcase.Query)
			if err != nil {
				vet.DebugQuery(tcase.Query)
			}
			assert.NoError(t, err)
			assert.Equal(t, tcase.Params, qparams)
		})
	}
}
