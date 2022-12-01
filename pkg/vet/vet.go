package vet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	pg_query2 "github.com/pganalyze/pg_query_go/v2"

	"github.com/samiam2013/sqlvet/pkg/schema"
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
	Location int32
}

type QueryParam struct {
	Number int32
	// TODO: also store related column type info for analysis
}

type ParseResult struct {
	Columns []ColumnUsed
	Params  []QueryParam
}

// insert query param based on parameter number and avoid deduplications
func AddQueryParam(target *[]QueryParam, param QueryParam) {
	params := *target
	for i, p := range params {
		if p.Number == param.Number {
			// avoid duplicate params
			return
		} else if p.Number > param.Number {
			*target = append(
				params[:i],
				append(
					[]QueryParam{param},
					params[i:]...,
				)...,
			)
			return
		}
	}
	*target = append(params, param)
}

func AddQueryParams(target *[]QueryParam, params []QueryParam) {
	for _, p := range params {
		AddQueryParam(target, p)
	}
}

func DebugQuery(q string) {
	b, _ := pg_query2.ParseToJSON(q)
	var pretty bytes.Buffer
	json.Indent(&pretty, []byte(b), "\t", "  ")
	fmt.Println("query: " + q)
	fmt.Println("parsed query: " + pretty.String())
}

func rangeVarToTableUsed(r pg_query2.Node_RangeVar) TableUsed {
	t := TableUsed{
		Name: r.RangeVar.Relname,
	}
	if r.RangeVar.Alias != nil {
		t.Alias = r.RangeVar.Alias.Aliasname
	}
	return t
}

// return nil if no specific column is being referenced
func columnRefToColumnUsed(colRef pg_query2.ColumnRef) *ColumnUsed {
	cu := ColumnUsed{
		Location: colRef.Location,
	}

	var colField any
	if len(colRef.Fields) > 1 {
		// in the form of SELECT table.column FROM table
		cu.Table = colRef.Fields[0].String()
		colField = *colRef.Fields[1]
	} else {
		// in the form of SELECT column FROM table
		colField = *colRef.Fields[0]
	}

	switch refField := colField.(type) {
	case pg_query2.Node_String_:
		cu.Column = refField.String_.Str
	case pg_query2.Node_AStar:
		// SELECT *
		return nil
	default:
		// FIXME: change to debug logging
		panic(fmt.Sprintf("Unsupported ref field type: %s", reflect.TypeOf(colField)))
	}

	return &cu
}

func getUsedTablesFromJoinArg(arg any) []TableUsed {
	switch n := arg.(type) {
	case pg_query2.Node_RangeVar:
		return []TableUsed{rangeVarToTableUsed(n)}
	case pg_query2.Node_JoinExpr:
		return append(
			getUsedTablesFromJoinArg(n.JoinExpr.Larg),
			getUsedTablesFromJoinArg(n.JoinExpr.Rarg)...)
	default:
		return []TableUsed{}
	}
}

// extract used tables from FROM clause and JOIN clauses
func getUsedTablesFromSelectStmt(fromClauseList pg_query2.List) []TableUsed {
	usedTables := []TableUsed{}

	if len(fromClauseList.Items) <= 0 {
		// skip because no table is referenced in the query
		return usedTables
	}

	var fromItem any
	for _, fromItem = range fromClauseList.Items {
		switch fromExpr := fromItem.(type) {
		case pg_query2.Node_RangeVar:
			// SELECT without JOIN
			usedTables = append(usedTables, rangeVarToTableUsed(fromExpr))
		case pg_query2.Node_JoinExpr:
			// SELECT with one or more JOINs
			usedTables = append(usedTables, getUsedTablesFromJoinArg(fromExpr.JoinExpr.Larg)...)
			usedTables = append(usedTables, getUsedTablesFromJoinArg(fromExpr.JoinExpr.Rarg)...)
		}
	}

	return usedTables
}

func getUsedColumnsFromJoinQuals(quals any /*pg_query2.Node*/) []ColumnUsed {
	usedCols := []ColumnUsed{}

	//var joinCond any
	switch joinCond := quals.(type) {
	case pg_query2.Node_AExpr:
		var jc interface{} = joinCond.AExpr.Lexpr
		lcolRef, ok := jc.(pg_query2.ColumnRef)
		if ok {
			cu := columnRefToColumnUsed(lcolRef)
			if cu != nil {
				usedCols = append(usedCols, *cu)
			}
		}
		var rc interface{} = joinCond.AExpr.Rexpr
		rcolRef, ok := rc.(pg_query2.ColumnRef)
		if ok {
			cu := columnRefToColumnUsed(rcolRef)
			if cu != nil {
				usedCols = append(usedCols, *cu)
			}
		}
	}

	return usedCols
}

// todo this rewrite seems especially dubious
func getUsedColumnsFromJoinExpr(expr any /*pg_query2.Node_JoinExpr*/) []ColumnUsed {
	usedCols := []ColumnUsed{}
	var joinExpr pg_query2.Node_JoinExpr
	var ok bool
	if joinExpr, ok = expr.(pg_query2.Node_JoinExpr); !ok {
		return usedCols
	}
	if larg := joinExpr.JoinExpr.Larg; larg != nil {
		usedCols = append(usedCols, getUsedColumnsFromJoinExpr(larg)...)
	}
	if rarg := joinExpr.JoinExpr.Rarg; rarg != nil {
		usedCols = append(usedCols, getUsedColumnsFromJoinExpr(rarg)...)
	}
	usedCols = append(usedCols, getUsedColumnsFromJoinQuals(joinExpr.JoinExpr.Quals)...)

	return usedCols
}

func getUsedColumnsFromJoinClauses(fromClauseList pg_query2.List) []ColumnUsed {
	usedCols := []ColumnUsed{}

	if len(fromClauseList.Items) <= 0 {
		// skip because no table is referenced in the query, which means there
		// is no Join clause
		return usedCols
	}

	for _, fromItem := range fromClauseList.Items {
		var fi interface{} = fromItem
		switch fromExpr := fi.(type) {
		case pg_query2.Node_RangeVar:
			// SELECT without JOIN
			continue
		case pg_query2.Node_JoinExpr:
			// SELECT with one or more JOINs
			usedCols = append(usedCols, getUsedColumnsFromJoinExpr(fromExpr)...)
		}
	}

	return usedCols
}

func getUsedColumnsFromReturningList(returningList pg_query2.List) []ColumnUsed {
	usedCols := []ColumnUsed{}

	for _, node := range returningList.Items {
		var n interface{} = node
		target, ok := n.(pg_query2.ResTarget)
		if !ok {
			continue
		}

		var t interface{} = target.Val
		switch targetVal := t.(type) {
		case pg_query2.ColumnRef:
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

func validateInsertValues(ctx VetContext, cols []ColumnUsed, vals []pg_query2.Node) error {
	colCnt := len(cols)
	// val could be nodes.ParamRef
	valCnt := len(vals)

	if colCnt != valCnt {
		return fmt.Errorf("Column count %d doesn't match value count %d.", colCnt, valCnt)
	}

	return nil
}

func parseWindowDef(ctx VetContext, winDef *pg_query2.WindowDef, parseRe *ParseResult) error {
	if len(winDef.PartitionClause) > 0 {
		if err := parseExpression(ctx, winDef.GetPartitionClause(), parseRe); err != nil {
			return err
		}
	}
	if len(winDef.OrderClause) > 0 {
		if err := parseExpression(ctx, winDef.OrderClause, parseRe); err != nil {
			return err
		}
	}
	return nil
}

func parseExpression(ctx VetContext, clause []*pg_query2.Node, parseRe *ParseResult) error {
	var c interface{} = clause
	switch expr := c.(type) {
	case pg_query2.Node_AExpr:
		if expr.AExpr.Lexpr != nil {
			err := parseExpression(ctx, []*pg_query2.Node{expr.AExpr.GetLexpr()}, parseRe)
			if err != nil {
				return err
			}
		}
		if expr.AExpr.Rexpr != nil {
			err := parseExpression(ctx, []*pg_query2.Node{expr.AExpr.GetRexpr()}, parseRe)
			if err != nil {
				return err
			}
		}
	case pg_query2.BoolExpr:
		return parseExpression(ctx, expr.Args, parseRe)
	case pg_query2.NullTest:
		return parseExpression(ctx, []*pg_query2.Node{expr.GetArg()}, parseRe)
	case pg_query2.ColumnRef:
		cu := columnRefToColumnUsed(expr)
		if cu == nil {
			return nil
		}
		parseRe.Columns = append(parseRe.Columns, *cu)
	case pg_query2.ParamRef:
		// WHERE id=$1
		AddQueryParam(&parseRe.Params, QueryParam{Number: expr.GetNumber()})
	case pg_query2.A_Const:
		// WHERE 1
	case pg_query2.FuncCall:
		// WHERE date=NOW()
		// WHERE MAX(id) > 1
		if err := parseExpression(ctx, expr.Args, parseRe); err != nil {
			return err
		}
		// SELECT ROW_NUMBER() OVER (PARTITION BY id)
		if expr.Over != nil {
			// TODO dubious rewrite
			err := parseExpression(ctx, expr.Over.OrderClause, parseRe)
			if err != nil {
				return err
			}
		}
	case pg_query2.TypeCast:
		// WHERE foo=True
		return parseExpression(ctx, []*pg_query2.Node{expr.Arg}, parseRe)
	case pg_query2.List:
		// WHERE id IN (1, 2, 3)
		for _, item := range expr.Items {
			err := parseExpression(ctx, []*pg_query2.Node{item}, parseRe)
			if err != nil {
				return err
			}
		}
	case pg_query2.SubLink:
		// WHERE id IN (SELECT id FROM foo)
		var subSel any = expr.GetSubselect()
		selectStmt, ok := subSel.(pg_query2.SelectStmt)
		if !ok {
			return fmt.Errorf(
				"Unsupported subquery type: %s", reflect.TypeOf(expr.Subselect))
		}
		queryParams, err := validateSelectStmt(ctx, selectStmt)
		if err != nil {
			return err
		}
		if len(queryParams) > 0 {
			AddQueryParams(&parseRe.Params, queryParams)
		}
	case pg_query2.CoalesceExpr:
		return parseExpression(ctx, expr.Args, parseRe)
	case *pg_query2.WindowDef:
		return parseWindowDef(ctx, expr, parseRe)
	case pg_query2.WindowDef:
		return parseWindowDef(ctx, &expr, parseRe)
	case pg_query2.SortBy:
		return parseExpression(ctx, []*pg_query2.Node{expr.Node}, parseRe)
	default:
		return fmt.Errorf(
			"Unsupported expression, found node of type: %v",
			reflect.TypeOf(clause),
		)
	}

	return nil
}

// find used column names from where clause
func parseWhereClause(ctx VetContext, clause pg_query2.Node, parseRe *ParseResult) error {
	err := parseExpression(ctx, []*pg_query2.Node{&clause}, parseRe)
	if err != nil {
		err = fmt.Errorf("Invalid WHERE clause: %w", err)
	}
	return err
}

func getUsedColumnsFromNodeList(nodelist pg_query2.List) []ColumnUsed {
	usedCols := []ColumnUsed{}
	var item any
	for _, item = range nodelist.Items {
		switch clause := item.(type) {
		case pg_query2.ColumnRef:
			cu := columnRefToColumnUsed(clause)
			if cu != nil {
				usedCols = append(usedCols, *cu)
			}
		}
	}
	return usedCols
}

func getUsedColumnsFromSortClause(sortList pg_query2.List) []ColumnUsed {
	usedCols := []ColumnUsed{}
	var item any
	for _, item = range sortList.Items {
		switch sortClause := item.(type) {
		case pg_query2.SortBy:
			var sc any = sortClause.Node
			if colRef, ok := sc.(pg_query2.ColumnRef); ok {
				cu := columnRefToColumnUsed(colRef)
				if cu != nil {
					usedCols = append(usedCols, *cu)
				}
			}
		}
	}
	return usedCols
}

func validateSelectStmt(ctx VetContext, stmt pg_query2.SelectStmt) ([]QueryParam, error) {
	usedTables := getUsedTablesFromSelectStmt(stmt.FromClause)

	usedCols := []ColumnUsed{}
	queryParams := []QueryParam{}

	for _, item := range stmt.TargetList.Items {
		target, ok := item.(nodes.ResTarget)
		if !ok {
			continue
		}

		re := &ParseResult{}
		err := parseExpression(ctx, target.Val, re)
		if err != nil {
			return nil, err
		}
		if len(re.Columns) > 0 {
			usedCols = append(usedCols, re.Columns...)
		}
		if len(re.Params) > 0 {
			AddQueryParams(&queryParams, re.Params)
		}
	}

	usedCols = append(usedCols, getUsedColumnsFromJoinClauses(stmt.FromClause)...)

	if stmt.WhereClause != nil {
		re := &ParseResult{}
		err := parseWhereClause(ctx, stmt.WhereClause, re)
		if err != nil {
			return nil, err
		}
		if len(re.Columns) > 0 {
			usedCols = append(usedCols, re.Columns...)
		}
		if len(re.Params) > 0 {
			AddQueryParams(&queryParams, re.Params)
		}
	}

	if len(stmt.GroupClause.Items) > 0 {
		usedCols = append(usedCols, getUsedColumnsFromNodeList(stmt.GroupClause)...)
	}

	if stmt.HavingClause != nil {
		re := &ParseResult{}
		err := parseExpression(ctx, stmt.HavingClause, re)
		if err != nil {
			return nil, err
		}
		if len(re.Columns) > 0 {
			usedCols = append(usedCols, re.Columns...)
		}
		if len(re.Params) > 0 {
			AddQueryParams(&queryParams, re.Params)
		}
	}

	if len(stmt.WindowClause.Items) > 0 {
		re := &ParseResult{}
		err := parseExpression(ctx, stmt.WindowClause, re)
		if err != nil {
			return nil, err
		}
		usedCols = append(usedCols, re.Columns...)
		AddQueryParams(&queryParams, re.Params)
	}

	if len(stmt.SortClause.Items) > 0 {
		usedCols = append(usedCols, getUsedColumnsFromSortClause(stmt.SortClause)...)
	}

	return queryParams, validateTableColumns(ctx, usedTables, usedCols)
}

func validateUpdateStmt(ctx VetContext, stmt nodes.UpdateStmt) ([]QueryParam, error) {
	tableName := *stmt.Relation.Relname
	usedTables := []TableUsed{{Name: tableName}}
	usedTables = append(usedTables, getUsedTablesFromSelectStmt(stmt.FromClause)...)

	usedCols := []ColumnUsed{}
	queryParams := []QueryParam{}

	for _, item := range stmt.TargetList.Items {
		target := item.(nodes.ResTarget)
		usedCols = append(usedCols, ColumnUsed{
			Table:    tableName,
			Column:   *target.Name,
			Location: target.Location,
		})

		// 'val' is the expression to assign.
		switch expr := target.Val.(type) {
		case nodes.ColumnRef:
			// UPDATE table1 SET table1.foo=table2.bar FROM table2
			cu := columnRefToColumnUsed(expr)
			if cu != nil {
				usedCols = append(usedCols, *cu)
			}
		case nodes.ParamRef:
			AddQueryParam(&queryParams, QueryParam{Number: expr.Number})
		}
	}

	if stmt.WhereClause != nil {
		re := &ParseResult{}
		err := parseWhereClause(ctx, stmt.WhereClause, re)
		if err != nil {
			return nil, err
		}
		usedCols = append(usedCols, re.Columns...)
		AddQueryParams(&queryParams, re.Params)
	}

	if len(stmt.ReturningList.Items) > 0 {
		usedCols = append(usedCols, getUsedColumnsFromReturningList(stmt.ReturningList)...)
	}

	return queryParams, validateTableColumns(ctx, usedTables, usedCols)
}

func validateInsertStmt(ctx VetContext, stmt nodes.InsertStmt) ([]QueryParam, error) {
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
	queryParams := []QueryParam{}

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
			re := &ParseResult{}
			err := parseExpression(ctx, node, re)
			if err != nil {
				return nil, fmt.Errorf("Invalid value list: %w", err)
			}
			if len(re.Columns) > 0 {
				usedCols = append(usedCols, re.Columns...)
			}
			if len(re.Params) > 0 {
				AddQueryParams(&queryParams, re.Params)
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
			re := &ParseResult{}
			err := parseWhereClause(ctx, selectStmt.WhereClause, re)
			if err != nil {
				return nil, err
			}
			if len(re.Columns) > 0 {
				usedCols = append(usedCols, re.Columns...)
			}
			if len(re.Params) > 0 {
				AddQueryParams(&queryParams, re.Params)
			}
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
					return nil, fmt.Errorf(
						"Unsupported subquery type in value list: %s", reflect.TypeOf(targetVal.Subselect))
				}
				qparams, err := validateSelectStmt(ctx, subquery)
				if err != nil {
					return nil, fmt.Errorf("Invalid SELECT query in value list: %w", err)
				}
				if len(qparams) > 0 {
					AddQueryParams(&queryParams, qparams)
				}
			}
		}
	}

	if len(stmt.ReturningList.Items) > 0 {
		usedCols = append(usedCols, getUsedColumnsFromReturningList(stmt.ReturningList)...)
	}

	if err := validateTableColumns(ctx, usedTables, usedCols); err != nil {
		return nil, err
	}

	if err := validateInsertValues(ctx, targetCols, values); err != nil {
		return nil, err
	}

	return queryParams, nil
}

func validateDeleteStmt(ctx VetContext, stmt nodes.DeleteStmt) ([]QueryParam, error) {
	tableName := *stmt.Relation.Relname
	if err := validateTable(ctx, tableName); err != nil {
		return nil, err
	}

	usedCols := []ColumnUsed{}
	queryParams := []QueryParam{}

	if stmt.WhereClause != nil {
		re := &ParseResult{}
		err := parseWhereClause(ctx, stmt.WhereClause, re)
		if err != nil {
			return nil, err
		}
		if len(re.Columns) > 0 {
			usedCols = append(usedCols, re.Columns...)
		}
		if len(re.Params) > 0 {
			queryParams = re.Params
		}
	}

	if len(stmt.ReturningList.Items) > 0 {
		usedCols = append(
			usedCols, getUsedColumnsFromReturningList(stmt.ReturningList)...)
	}

	if len(usedCols) > 0 {
		usedTables := []TableUsed{{Name: tableName}}
		if err := validateTableColumns(ctx, usedTables, usedCols); err != nil {
			return nil, err
		}
	}

	return queryParams, nil
}

func ValidateSqlQuery(ctx VetContext, queryStr string) ([]QueryParam, error) {
	tree, err := pg_query2.Parse(queryStr)
	if err != nil {
		return nil, err
	}

	if len(tree.Stmts) == 0 || len(tree.Stmts) > 1 {
		return nil, fmt.Errorf("query contained more than one statement.")
	}

	raw, ok := tree.Stmts[0].(*pg_query2.RawStmt)
	if !ok {
		return nil, fmt.Errorf("query contained invalid statement.")
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
		return nil, fmt.Errorf("unsupported statement: %v.", reflect.TypeOf(raw.Stmt))
	}

	return nil, nil
}
