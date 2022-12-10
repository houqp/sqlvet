package schema

import (
	"os"
	"strings"

	pg_query2 "github.com/pganalyze/pg_query_go/v2"
)

// func debugNode(n nodes.Node) {
// 	b, e := n.MarshalJSON()
// 	if e != nil {
// 		fmt.Println("Node decode error:", e)
// 	} else {
// 		fmt.Println(string(b))
// 	}
// }

func (s *Db) LoadPostgres(schemaPath string) error {
	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		return err
	}

	tree, err := pg_query2.Parse(string(schemaBytes))
	if err != nil {
		return err
	}
	// fmt.Printf("tree: %+v\n", tree)

	// tree2, err := pg_query2.Parse(string(schemaBytes))
	// if err != nil {
	// 	return err
	// }
	// fmt.Printf("tree2: %+v\n", tree2)

	var stmt any
	for _, stmt = range tree.Stmts {
		raw, ok := stmt.(pg_query2.RawStmt)
		if !ok {
			continue
		}

		var rs any = raw.Stmt
		switch stmt := rs.(type) {
		case pg_query2.CreateStmt:
			tableName := stmt.Relation.Relname
			table := Table{
				Name:    tableName,
				Columns: map[string]Column{},
			}

			var colElem any
			for _, colElem = range stmt.TableElts {
				colDef, ok := colElem.(pg_query2.ColumnDef)
				if !ok {
					continue
				}

				typeParts := []string{}
				var typNode any
				for _, typNode = range colDef.TypeName.Names {
					tStr, ok := typNode.(pg_query2.String)
					if !ok {
						continue
					}
					typeParts = append(typeParts, tStr.Str)
				}

				colName := colDef.Colname
				table.Columns[colName] = Column{
					Name: colName,
					Type: strings.Join(typeParts, "."),
				}
			}

			s.Tables[tableName] = table
		}
	}

	return nil
}
