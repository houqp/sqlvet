package schema

import (
	"os"
	"strings"

	pg_query "github.com/pganalyze/pg_query_go/v2"
)

func (s *Db) LoadPostgres(schemaPath string) error {
	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		return err
	}

	tree, err := pg_query.Parse(string(schemaBytes))
	if err != nil {
		return err
	}

	for _, stmt := range tree.Stmts {
		if stmt.Stmt == nil {
			continue
		}

		switch {
		case stmt.Stmt.GetCreateStmt() != nil:
			tableName := stmt.Stmt.GetCreateStmt().Relation.Relname
			table := Table{
				Name:    tableName,
				Columns: map[string]Column{},
			}

			for _, colElem := range stmt.Stmt.GetCreateStmt().TableElts {
				if colElem.GetColumnDef() == nil {
					continue
				}
				colDef := colElem.GetColumnDef()

				typeParts := []string{}
				for _, typNode := range colDef.TypeName.Names {
					if typNode.GetString_() == nil {
						continue
					}
					tStr := typNode.GetString_()
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
