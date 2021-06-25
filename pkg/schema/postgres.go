package schema

import (
	"io/ioutil"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go"
	nodes "github.com/pganalyze/pg_query_go/nodes"
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
	schemaBytes, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		return err
	}

	// func() {
	// 	tree, _ := pg_query.ParseToJSON(string(schemaBytes))
	// 	fmt.Println("????????????", tree)
	// }()

	tree, err := pg_query.Parse(string(schemaBytes))
	if err != nil {
		return err
	}

	for _, stmt := range tree.Statements {
		raw, ok := stmt.(nodes.RawStmt)
		if !ok {
			continue
		}

		switch stmt := raw.Stmt.(type) {
		case nodes.CreateStmt:
			tableName := *stmt.Relation.Relname
			table := Table{
				Name:    tableName,
				Columns: map[string]Column{},
			}

			for _, colElem := range stmt.TableElts.Items {
				colDef, ok := colElem.(nodes.ColumnDef)
				if !ok {
					continue
				}

				typeParts := []string{}
				for _, typNode := range colDef.TypeName.Names.Items {
					tStr, ok := typNode.(nodes.String)
					if !ok {
						continue
					}
					typeParts = append(typeParts, tStr.Str)
				}

				colName := *colDef.Colname
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
