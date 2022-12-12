package parseutil

import "testing"

func TestCompileQuery(t *testing.T) {
	table := []struct {
		Q, R, D, T, N string
		V             []string
	}{
		// basic test for named parameters, invalid char ',' terminating
		{
			Q: `INSERT INTO foo (a,b,c,d) VALUES (:name, :age, :first, :last)`,
			R: `INSERT INTO foo (a,b,c,d) VALUES (?, ?, ?, ?)`,
			D: `INSERT INTO foo (a,b,c,d) VALUES ($1, $2, $3, $4)`,
			T: `INSERT INTO foo (a,b,c,d) VALUES (@p1, @p2, @p3, @p4)`,
			N: `INSERT INTO foo (a,b,c,d) VALUES (:name, :age, :first, :last)`,
			V: []string{"name", "age", "first", "last"},
		},
		// This query tests a named parameter ending the string as well as numbers
		{
			Q: `SELECT * FROM a WHERE first_name=:name1 AND last_name=:name2`,
			R: `SELECT * FROM a WHERE first_name=? AND last_name=?`,
			D: `SELECT * FROM a WHERE first_name=$1 AND last_name=$2`,
			T: `SELECT * FROM a WHERE first_name=@p1 AND last_name=@p2`,
			N: `SELECT * FROM a WHERE first_name=:name1 AND last_name=:name2`,
			V: []string{"name1", "name2"},
		},
		{
			Q: `SELECT "::foo" FROM a WHERE first_name=:name1 AND last_name=:name2`,
			R: `SELECT ":foo" FROM a WHERE first_name=? AND last_name=?`,
			D: `SELECT ":foo" FROM a WHERE first_name=$1 AND last_name=$2`,
			T: `SELECT ":foo" FROM a WHERE first_name=@p1 AND last_name=@p2`,
			N: `SELECT ":foo" FROM a WHERE first_name=:name1 AND last_name=:name2`,
			V: []string{"name1", "name2"},
		},
		{
			Q: `SELECT 'a::b::c' || first_name, '::::ABC::_::' FROM person WHERE first_name=:first_name AND last_name=:last_name`,
			R: `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=? AND last_name=?`,
			D: `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=$1 AND last_name=$2`,
			T: `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=@p1 AND last_name=@p2`,
			N: `SELECT 'a:b:c' || first_name, '::ABC:_:' FROM person WHERE first_name=:first_name AND last_name=:last_name`,
			V: []string{"first_name", "last_name"},
		},
		{
			Q: `SELECT @name := "name", :age, :first, :last`,
			R: `SELECT @name := "name", ?, ?, ?`,
			D: `SELECT @name := "name", $1, $2, $3`,
			N: `SELECT @name := "name", :age, :first, :last`,
			T: `SELECT @name := "name", @p1, @p2, @p3`,
			V: []string{"age", "first", "last"},
		},
		/* This unicode awareness test sadly fails, because of our byte-wise worldview.
		 * We could certainly iterate by Rune instead, though it's a great deal slower,
		 * it's probably the RightWay(tm)
		{
			Q: `INSERT INTO foo (a,b,c,d) VALUES (:あ, :b, :キコ, :名前)`,
			R: `INSERT INTO foo (a,b,c,d) VALUES (?, ?, ?, ?)`,
			D: `INSERT INTO foo (a,b,c,d) VALUES ($1, $2, $3, $4)`,
			N: []string{"name", "age", "first", "last"},
		},
		*/
	}

	for _, test := range table {
		qr, names, err := CompileNamedQuery([]byte(test.Q), QUESTION)
		if err != nil {
			t.Error(err)
		}
		if qr != test.R {
			t.Errorf("expected %s, got %s", test.R, qr)
		}
		if len(names) != len(test.V) {
			t.Errorf("expected %#v, got %#v", test.V, names)
		} else {
			for i, name := range names {
				if name != test.V[i] {
					t.Errorf("expected %dth name to be %s, got %s", i+1, test.V[i], name)
				}
			}
		}
		qd, _, _ := CompileNamedQuery([]byte(test.Q), DOLLAR)
		if qd != test.D {
			t.Errorf("\nexpected: `%s`\ngot:      `%s`", test.D, qd)
		}

		qt, _, _ := CompileNamedQuery([]byte(test.Q), AT)
		if qt != test.T {
			t.Errorf("\nexpected: `%s`\ngot:      `%s`", test.T, qt)
		}

		qq, _, _ := CompileNamedQuery([]byte(test.Q), NAMED)
		if qq != test.N {
			t.Errorf("\nexpected: `%s`\ngot:      `%s`\n(len: %d vs %d)", test.N, qq, len(test.N), len(qq))
		}
	}
}
