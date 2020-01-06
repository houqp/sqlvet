package vet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	pg_query "github.com/lfittl/pg_query_go"
	nodes "github.com/lfittl/pg_query_go/nodes"

	"github.com/houqp/sqlvet/pkg/schema"
)

type VetContext struct {
	Schema *schema.Db
}

type TableUsed struct {
	Name  string
	Alias string
}

type ColumnUsed struct {
	Column   string
	Table    string
	Location int
}

func DebugQuery(q string) {
	b, _ := pg_query.ParseToJSON(q)
	var pretty bytes.Buffer
	json.Indent(&pretty, []byte(b), "\t", "  ")
	fmt.Println("query: " + q)
	fmt.Println("parsed query: " + string(pretty.Bytes()))
}

func rangeVarToTableUsed(r nodes.RangeVar) TableUsed {
	t := TableUsed{
		Name: *r.Relname,
	}
	if r.Alias != nil {
		t.Alias = *r.Alias.Aliasname
	}
	return t
}

// return nil if no specific column is being referenced
func columnRefToColumnUsed(colRef nodes.ColumnRef) *ColumnUsed {
	cu := ColumnUsed{
		Location: colRef.Location,
	}

	var colField nodes.Node
	if len(colRef.Fields.Items) > 1 {
		// in the form of SELECT table.column FROM table
		cu.Table = colRef.Fields.Items[0].(nodes.String).Str
		colField = colRef.Fields.Items[1]
	} else {
		// in the form of SELECT column FROM table
		colField = colRef.Fields.Items[0]
	}

	switch refField := colField.(type) {
	case nodes.String:
		cu.Column = refField.Str
	case nodes.A_Star:
		// SELECT *
		return nil
	default:
		// FIXME: change to debug logging
		panic(fmt.Sprintf("Unsupported ref field type: %s", reflect.TypeOf(colField)))
	}

	return &cu
}

// extract used tables from FROM clause and JOIN clauses
func getUsedTablesFromSelectStmt(fromClauseList nodes.List) []TableUsed {
	usedTables := []TableUsed{}

	if len(fromClauseList.Items) <= 0 {
		// skip because no table is referenced in the query
		return usedTables
	}

	for _, fromItem := range fromClauseList.Items {
		switch fromExpr := fromItem.(type) {
		case nodes.RangeVar:
			// SELECT without JOIN
			usedTables = append(usedTables, rangeVarToTableUsed(fromExpr))
		case nodes.JoinExpr:
			// SELECT with one or more JOINs
			usedTables = append(usedTables, rangeVarToTableUsed(fromExpr.Larg.(nodes.RangeVar)))
			usedTables = append(usedTables, rangeVarToTableUsed(fromExpr.Rarg.(nodes.RangeVar)))
			// TODO: also check for table in columnRef in join condition expression
		}
	}

	return usedTables
}

func getUsedColumnsFromJoinClauses(fromClauseList nodes.List) []ColumnUsed {
	usedCols := []ColumnUsed{}

	if len(fromClauseList.Items) <= 0 {
		// skip because no table is referenced in the query, which means there
		// is no Join clause
		return usedCols
	}

	for _, fromItem := range fromClauseList.Items {
		switch fromExpr := fromItem.(type) {
		case nodes.RangeVar:
			// SELECT without JOIN
			continue
		case nodes.JoinExpr:
			// SELECT with one or more JOINs
			switch joinCond := fromExpr.Quals.(type) {
			case nodes.A_Expr:
				lcolRef, ok := joinCond.Lexpr.(nodes.ColumnRef)
				if ok {
					cu := columnRefToColumnUsed(lcolRef)
					if cu != nil {
						usedCols = append(usedCols, *cu)
					}
				}
				rcolRef, ok := joinCond.Rexpr.(nodes.ColumnRef)
				if ok {
					cu := columnRefToColumnUsed(rcolRef)
					if cu != nil {
						usedCols = append(usedCols, *cu)
					}
				}
			}
		}
	}

	return usedCols
}

func getUsedColumnsFromReturningList(returningList nodes.List) []ColumnUsed {
	usedCols := []ColumnUsed{}

	for _, node := range returningList.Items {
		target, ok := node.(nodes.ResTarget)
		if !ok {
			continue
		}

		switch targetVal := target.Val.(type) {
		case nodes.ColumnRef:
			cu := columnRefToColumnUsed(targetVal)
			if cu == nil {
				continue
			}
			usedCols = append(usedCols, *cu)
		default:
			// do nothing if no column is referenced
		}
	}

	return usedCols
}

func validateTable(ctx VetContext, tname string) error {
	if ctx.Schema == nil {
		return nil
	}
	_, ok := ctx.Schema.Tables[tname]
	if !ok {
		return fmt.Errorf("invalid table name: %s", tname)
	}
	return nil
}

func validateTableColumns(ctx VetContext, tables []TableUsed, cols []ColumnUsed) error {
	if ctx.Schema == nil {
		return nil
	}

	var ok bool
	usedTables := map[string]schema.Table{}
	for _, tu := range tables {
		usedTables[tu.Name], ok = ctx.Schema.Tables[tu.Name]
		if !ok {
			return fmt.Errorf("invalid table name: %s", tu.Name)
		}
		if tu.Alias != "" {
			usedTables[tu.Alias] = usedTables[tu.Name]
		}
	}

	for _, col := range cols {
		if col.Table != "" {
			table, ok := usedTables[col.Table]
			if !ok {
				return fmt.Errorf("table `%s` not available for query", col.Table)
			}
			_, ok = table.Columns[col.Column]
			if !ok {
				return fmt.Errorf("column `%s` is not defined in table `%s`", col.Column, col.Table)
			}
		} else {
			// no table prefix, try all tables
			found := false
			for _, table := range usedTables {
				_, ok = table.Columns[col.Column]
				if ok {
					found = true
					break
				}
			}
			if !found {
				if len(tables) == 1 {
					// to make error message more useful, if only one table is
					// referenced in the query, it's safe to assume user only
					// want to use columns from that table.
					return fmt.Errorf(
						"column `%s` is not defined in table `%s`",
						col.Column, tables[0].Name)
				} else {
					return fmt.Errorf(
						"column `%s` is not defined in any of the table available for query",
						col.Column)
				}
			}
		}
	}

	return nil
}

func validateInsertValues(ctx VetContext, cols []ColumnUsed, vals []nodes.Node) error {
	colCnt := len(cols)
	// val could be nodes.ParamRef
	valCnt := len(vals)

	if colCnt != valCnt {
		return fmt.Errorf("Column count %d doesn't match value count %d.", colCnt, valCnt)
	}

	return nil
}

func getUsedColumnsFromQualifications(ctx VetContext, clause nodes.Node) ([]ColumnUsed, error) {
	usedCols := []ColumnUsed{}

	switch expr := clause.(type) {
	case nodes.A_Expr:
		if expr.Lexpr != nil {
			cols, err := getUsedColumnsFromQualifications(ctx, expr.Lexpr)
			if err != nil {
				return nil, err
			}
			usedCols = append(usedCols, cols...)
		}
		if expr.Rexpr != nil {
			cols, err := getUsedColumnsFromQualifications(ctx, expr.Rexpr)
			if err != nil {
				return nil, err
			}
			usedCols = append(usedCols, cols...)
		}
	case nodes.BoolExpr:
		for _, arg := range expr.Args.Items {
			cols, err := getUsedColumnsFromQualifications(ctx, arg)
			if err != nil {
				return nil, err
			}
			usedCols = append(usedCols, cols...)
		}
	case nodes.NullTest:
		return getUsedColumnsFromQualifications(ctx, expr.Arg)
	case nodes.ColumnRef:
		cu := columnRefToColumnUsed(expr)
		if cu == nil {
			return usedCols, nil
		}
		usedCols = append(usedCols, *cu)
	case nodes.ParamRef:
		// WHERE id=$1
	case nodes.A_Const:
		// WHERE 1
	case nodes.FuncCall:
		// WHERE date=NOW()
	case nodes.TypeCast:
		// WHERE foo=True
		return getUsedColumnsFromQualifications(ctx, expr.Arg)
	case nodes.List:
		// WHERE id IN (1, 2, 3)
		for _, item := range expr.Items {
			cols, err := getUsedColumnsFromQualifications(ctx, item)
			if err != nil {
				return nil, err
			}
			usedCols = append(usedCols, cols...)
		}
	case nodes.SubLink:
		// WHERE id IN (SELECT id FROM foo)
		selectStmt, ok := expr.Subselect.(nodes.SelectStmt)
		if !ok {
			return nil, fmt.Errorf(
				"Unsupported subquery type: %s", reflect.TypeOf(expr.Subselect))
		} else {
			err := validateSelectStmt(ctx, selectStmt)
			if err != nil {
				return nil, err
			}
		}
	default:
		return nil, fmt.Errorf(
			"Unsupported qualification, found node of type: %v",
			reflect.TypeOf(clause),
		)
	}

	return usedCols, nil
}

// find used column names from where clause
func getUsedColumnsFromWhereClause(ctx VetContext, clause nodes.Node) ([]ColumnUsed, error) {
	cols, err := getUsedColumnsFromQualifications(ctx, clause)
	if err != nil {
		return nil, fmt.Errorf("Invalid WHERE clause: %w", err)
	}
	return cols, err
}

func validateSelectStmt(ctx VetContext, stmt nodes.SelectStmt) error {
	usedTables := getUsedTablesFromSelectStmt(stmt.FromClause)

	usedCols := []ColumnUsed{}
	for _, item := range stmt.TargetList.Items {
		target, ok := item.(nodes.ResTarget)
		if !ok {
			continue
		}

		switch targetVal := target.Val.(type) {
		case nodes.ColumnRef:
			cu := columnRefToColumnUsed(targetVal)
			if cu == nil {
				continue
			}
			usedCols = append(usedCols, *cu)
		default:
			// do nothing if no column is referenced
		}
	}

	usedCols = append(usedCols, getUsedColumnsFromJoinClauses(stmt.FromClause)...)

	if stmt.WhereClause != nil {
		whereCols, err := getUsedColumnsFromWhereClause(ctx, stmt.WhereClause)
		if err != nil {
			return err
		}
		usedCols = append(usedCols, whereCols...)
	}

	return validateTableColumns(ctx, usedTables, usedCols)
}

func validateUpdateStmt(ctx VetContext, stmt nodes.UpdateStmt) error {
	tableName := *stmt.Relation.Relname
	usedTables := []TableUsed{{Name: tableName}}
	usedTables = append(usedTables, getUsedTablesFromSelectStmt(stmt.FromClause)...)

	usedCols := []ColumnUsed{}
	for _, item := range stmt.TargetList.Items {
		target := item.(nodes.ResTarget)
		usedCols = append(usedCols, ColumnUsed{
			Table:    tableName,
			Column:   *target.Name,
			Location: target.Location,
		})

		colRef, ok := target.Val.(nodes.ColumnRef)
		if ok {
			// UPDATE table1 SET table1.foo=table2.bar FROM table2
			cu := columnRefToColumnUsed(colRef)
			if cu != nil {
				usedCols = append(usedCols, *cu)
			}
		}
	}

	if stmt.WhereClause != nil {
		whereCols, err := getUsedColumnsFromWhereClause(ctx, stmt.WhereClause)
		if err != nil {
			return err
		}
		usedCols = append(usedCols, whereCols...)
	}

	if len(stmt.ReturningList.Items) > 0 {
		usedCols = append(usedCols, getUsedColumnsFromReturningList(stmt.ReturningList)...)
	}

	return validateTableColumns(ctx, usedTables, usedCols)
}

func validateInsertStmt(ctx VetContext, stmt nodes.InsertStmt) error {
	tableName := *stmt.Relation.Relname
	usedTables := []TableUsed{{Name: tableName}}

	targetCols := []ColumnUsed{}
	for _, item := range stmt.Cols.Items {
		target := item.(nodes.ResTarget)
		targetCols = append(targetCols, ColumnUsed{
			Table:    tableName,
			Column:   *target.Name,
			Location: target.Location,
		})
	}

	values := []nodes.Node{}
	// make a copy of targetCols because we need it to do value count
	// validation separately
	usedCols := append([]ColumnUsed{}, targetCols...)

	selectStmt := stmt.SelectStmt.(nodes.SelectStmt)
	if selectStmt.ValuesLists != nil {
		/*
		 * In the form of:
		 *     INSERT INTO table (col1, col2) VALUES (val1, val2)
		 *
		 * In a "leaf" node representing a VALUES list, the above fields are all
		 * null, and instead this field is set.  Note that the elements of the
		 * sublists are just expressions, without ResTarget decoration. Also note
		 * that a list element can be DEFAULT (represented as a SetToDefault
		 * node), regardless of the context of the VALUES list. It's up to parse
		 * analysis to reject that where not valid.
		 */
		for _, node := range selectStmt.ValuesLists[0] {
			switch v := node.(type) {
			case nodes.SubLink:
				subquery, ok := v.Subselect.(nodes.SelectStmt)
				if !ok {
					return fmt.Errorf(
						"Unsupported subquery type in value list: %s", reflect.TypeOf(v.Subselect))
				}
				err := validateSelectStmt(ctx, subquery)
				if err != nil {
					return fmt.Errorf("Invalid SELECT query in value list: %w", err)
				}
			}
			values = append(values, node)
		}
	} else {
		/*
		 * Value from SELECT, in the form of:
		 *     INSERT INTO table (col1, col2) SELECT (col1, col2) FROM table
		 */
		usedTables = append(usedTables, getUsedTablesFromSelectStmt(selectStmt.FromClause)...)

		usedCols = append(
			usedCols, getUsedColumnsFromJoinClauses(selectStmt.FromClause)...)

		if selectStmt.WhereClause != nil {
			whereCols, err := getUsedColumnsFromWhereClause(ctx, selectStmt.WhereClause)
			if err != nil {
				return err
			}
			usedCols = append(usedCols, whereCols...)
		}

		for _, item := range selectStmt.TargetList.Items {
			target := item.(nodes.ResTarget)
			values = append(values, target)

			switch targetVal := target.Val.(type) {
			case nodes.ColumnRef:
				cu := columnRefToColumnUsed(targetVal)
				if cu == nil {
					continue
				}
				usedCols = append(usedCols, *cu)
			case nodes.SubLink:
				subquery, ok := targetVal.Subselect.(nodes.SelectStmt)
				if !ok {
					return fmt.Errorf(
						"Unsupported subquery type in value list: %s", reflect.TypeOf(targetVal.Subselect))
				}
				err := validateSelectStmt(ctx, subquery)
				if err != nil {
					return fmt.Errorf("Invalid SELECT query in value list: %w", err)
				}
			}
		}
	}

	if len(stmt.ReturningList.Items) > 0 {
		usedCols = append(usedCols, getUsedColumnsFromReturningList(stmt.ReturningList)...)
	}

	if err := validateTableColumns(ctx, usedTables, usedCols); err != nil {
		return err
	}

	if err := validateInsertValues(ctx, targetCols, values); err != nil {
		return err
	}

	return nil
}

func validateDeleteStmt(ctx VetContext, stmt nodes.DeleteStmt) error {
	tableName := *stmt.Relation.Relname
	if err := validateTable(ctx, tableName); err != nil {
		return err
	}

	usedCols := []ColumnUsed{}

	if stmt.WhereClause != nil {
		whereCols, err := getUsedColumnsFromWhereClause(ctx, stmt.WhereClause)
		if err != nil {
			return err
		}
		usedCols = append(usedCols, whereCols...)
	}

	if len(stmt.ReturningList.Items) > 0 {
		usedCols = append(
			usedCols, getUsedColumnsFromReturningList(stmt.ReturningList)...)
	}

	if len(usedCols) > 0 {
		usedTables := []TableUsed{{Name: tableName}}
		if err := validateTableColumns(ctx, usedTables, usedCols); err != nil {
			return err
		}
	}

	return nil
}

func ValidateSqlQuery(ctx VetContext, queryStr string) error {
	tree, err := pg_query.Parse(queryStr)
	if err != nil {
		return err
	}

	if len(tree.Statements) == 0 || len(tree.Statements) > 1 {
		return fmt.Errorf("query contained more than one statement.")
	}

	raw, ok := tree.Statements[0].(nodes.RawStmt)
	if !ok {
		return fmt.Errorf("query contained invalid statement.")
	}

	switch stmt := raw.Stmt.(type) {
	case nodes.SelectStmt:
		return validateSelectStmt(ctx, stmt)
	case nodes.UpdateStmt:
		return validateUpdateStmt(ctx, stmt)
	case nodes.InsertStmt:
		return validateInsertStmt(ctx, stmt)
	case nodes.DeleteStmt:
		return validateDeleteStmt(ctx, stmt)
	case nodes.DropStmt:
	case nodes.TruncateStmt:
	case nodes.AlterTableStmt:
	case nodes.CreateSchemaStmt:
	case nodes.VariableSetStmt:
		// TODO: check for invalid pg variables
	default:
		return fmt.Errorf("unsupported statement: %v.", reflect.TypeOf(raw.Stmt))
	}

	return nil
}
