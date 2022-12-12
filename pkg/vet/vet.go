package vet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	pg_query "github.com/pganalyze/pg_query_go/v2"

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
	b, _ := pg_query.ParseToJSON(q)
	var pretty bytes.Buffer
	json.Indent(&pretty, []byte(b), "\t", "  ")
	fmt.Println("query: " + q)
	fmt.Println("parsed query: " + pretty.String())
}

func rangeVarToTableUsed(r *pg_query.RangeVar) TableUsed {
	t := TableUsed{
		Name: r.Relname,
	}
	if r.Alias != nil {
		t.Alias = r.Alias.Aliasname
	}
	return t
}

/* =========================================================================================================
	this is where the rewrite to nil checks rather than type refleciton starts
 ========================================================================================================= */

// return nil if no specific column is being referenced
func columnRefToColumnUsed(colRef *pg_query.ColumnRef) *ColumnUsed {
	cu := ColumnUsed{
		Location: colRef.Location,
	}

	var colField *pg_query.Node
	if len(colRef.Fields) > 1 {
		// in the form of SELECT table.column FROM table
		cu.Table = colRef.Fields[0].GetString_().Str
		// fmt.Printf("table: %s\n", cu.Table)
		colField = colRef.Fields[1]
	} else {
		// in the form of SELECT column FROM table
		colField = colRef.Fields[0]
	}

	switch {
	case colField.GetString_() != nil:
		cu.Column = colField.GetString_().GetStr()
	case colField.GetAStar() != nil:
		// SELECT *
		return nil
	default:
		// FIXME: change to debug logging
		panic(fmt.Sprintf("Unsupported ref field type: %s", reflect.TypeOf(colField)))
	}

	return &cu
}

func getUsedTablesFromJoinArg(arg *pg_query.Node) []TableUsed {
	switch {
	case arg.GetRangeVar() != nil:
		return []TableUsed{rangeVarToTableUsed(arg.GetRangeVar())}
	case arg.GetJoinExpr() != nil:
		return append(
			getUsedTablesFromJoinArg(arg.GetJoinExpr().GetLarg()),
			getUsedTablesFromJoinArg(arg.GetJoinExpr().GetRarg())...)
	default:
		return []TableUsed{}
	}
}

// extract used tables from FROM clause and JOIN clauses
func getUsedTablesFromSelectStmt(fromClauseList []*pg_query.Node) []TableUsed {
	usedTables := []TableUsed{}

	if len(fromClauseList) <= 0 {
		// skip because no table is referenced in the query
		return usedTables
	}

	for _, fromItem := range fromClauseList {
		switch {
		case fromItem.GetRangeVar() != nil:
			// SELECT without JOIN
			usedTables = append(usedTables, rangeVarToTableUsed(fromItem.GetRangeVar()))
		case fromItem.GetJoinExpr() != nil:
			// SELECT with one or more JOINs
			usedTables = append(usedTables, getUsedTablesFromJoinArg(fromItem.GetJoinExpr().GetLarg())...)
			usedTables = append(usedTables, getUsedTablesFromJoinArg(fromItem.GetJoinExpr().GetRarg())...)
		}
	}

	return usedTables
}

func getUsedColumnsFromJoinQuals(quals *pg_query.Node) []ColumnUsed {
	usedCols := []ColumnUsed{}

	if quals.GetAExpr() != nil {
		joinCond := quals.GetAExpr()
		if lColRef := joinCond.GetLexpr().GetColumnRef(); lColRef != nil {
			cu := columnRefToColumnUsed(lColRef)
			if cu != nil {
				usedCols = append(usedCols, *cu)
			}
		}
		if rColRef := joinCond.GetRexpr().GetColumnRef(); rColRef != nil {
			cu := columnRefToColumnUsed(rColRef)
			if cu != nil {
				usedCols = append(usedCols, *cu)
			}
		}
	}

	return usedCols
}

// todo this rewrite seems especially dubious
func getUsedColumnsFromJoinExpr(expr *pg_query.Node) []ColumnUsed {
	usedCols := []ColumnUsed{}
	if expr.GetJoinExpr() == nil {
		return usedCols
	}
	joinExpr := expr.GetJoinExpr()
	if larg := joinExpr.Larg; larg != nil {
		usedCols = append(usedCols, getUsedColumnsFromJoinExpr(larg)...)
	}
	if rarg := joinExpr.Rarg; rarg != nil {
		usedCols = append(usedCols, getUsedColumnsFromJoinExpr(rarg)...)
	}
	usedCols = append(usedCols, getUsedColumnsFromJoinQuals(joinExpr.Quals)...)

	return usedCols
}

func getUsedColumnsFromJoinClauses(fromClauseList []*pg_query.Node) []ColumnUsed {
	usedCols := []ColumnUsed{}

	if len(fromClauseList) <= 0 {
		// skip because no table is referenced in the query, which means there
		// is no Join clause
		return usedCols
	}

	for _, fromItem := range fromClauseList {
		switch {
		case fromItem.GetRangeVar() != nil:
			// SELECT without JOIN
			continue
		case fromItem.GetJoinExpr() != nil:
			// SELECT with one or more JOINs
			usedCols = append(usedCols, getUsedColumnsFromJoinExpr(fromItem)...)
		}
	}

	return usedCols
}

func getUsedColumnsFromReturningList(returningList []*pg_query.Node) []ColumnUsed {
	usedCols := []ColumnUsed{}

	for _, node := range returningList {
		target := node.GetResTarget()
		if target == nil {
			continue
		}

		switch {
		// case pg_query.ColumnRef:
		case target.Val.GetColumnRef() != nil:
			cu := columnRefToColumnUsed(target.Val.GetColumnRef())
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

func validateInsertValues(ctx VetContext, cols []ColumnUsed, vals []*pg_query.Node) error {
	colCnt := len(cols)
	// val could be nodes.ParamRef
	valCnt := len(vals)

	if colCnt != valCnt {
		return fmt.Errorf("column count %d doesn't match value count %d", colCnt, valCnt)
	}

	return nil
}

func parseWindowDef(ctx VetContext, winDef *pg_query.WindowDef, parseRe *ParseResult) error {
	if len(winDef.PartitionClause) > 0 {
		// TODO should this be [0]
		if err := parseExpression(ctx, winDef.GetPartitionClause()[0], parseRe); err != nil {
			return err
		}
	}
	if len(winDef.OrderClause) > 0 {
		// TODO should this be [0]
		if err := parseExpression(ctx, winDef.OrderClause[0], parseRe); err != nil {
			return err
		}
	}
	return nil
}

// recursive function to parse expressions including nested expressions
func parseExpression(ctx VetContext, clause *pg_query.Node, parseRe *ParseResult) error {
	switch {
	case clause.GetAExpr() != nil:
		if clause.GetAExpr().GetLexpr() != nil {
			err := parseExpression(ctx, clause.GetAExpr().GetLexpr(), parseRe)
			if err != nil {
				return err
			}
		}
		if clause.GetAExpr().GetRexpr() != nil {
			err := parseExpression(ctx, clause.GetAExpr().GetRexpr(), parseRe)
			if err != nil {
				return err
			}
		}
	case clause.GetBoolExpr() != nil:
		// TODO should this be args or args[0]?
		return parseExpression(ctx, clause.GetBoolExpr().Args[0], parseRe)
	case clause.GetNullTest() != nil:
		nullTest := clause.GetNullTest()
		if arg := nullTest.GetArg(); arg != nil {
			return parseExpression(ctx, arg, parseRe)
		}
	case clause.GetColumnRef() != nil:
		// cu := columnRefToColumnUsed(expr)
		cu := columnRefToColumnUsed(clause.GetColumnRef())
		if cu == nil {
			return nil
		}
		parseRe.Columns = append(parseRe.Columns, *cu)
	case clause.GetParamRef() != nil:
		// WHERE id=$1
		AddQueryParam(&parseRe.Params, QueryParam{Number: clause.GetParamRef().GetNumber()})
	case clause.GetAConst() != nil:
		// WHERE 1
	case clause.GetFuncCall() != nil:
		// WHERE date=NOW()
		// WHERE MAX(id) > 1
		// TODO should this be args or args[0]?
		funcCall := clause.GetFuncCall()
		if len(funcCall.Args) > 0 {
			if err := parseExpression(ctx, funcCall.Args[0], parseRe); err != nil {
				return err
			}
		}
		// SELECT ROW_NUMBER() OVER (PARTITION BY id)
		if clause.GetFuncCall().GetOver() != nil {
			// TODO dubious rewrite and should this be [0]
			over := clause.GetFuncCall().GetOver()
			if len(over.PartitionClause) > 0 {
				err := parseExpression(ctx, over.PartitionClause[0], parseRe)
				if err != nil {
					return err
				}
			}
		}
	case clause.GetTypeCast() != nil:
		// WHERE foo=True
		return parseExpression(ctx, clause.GetTypeCast().Arg, parseRe)
	case clause.GetList() != nil:
		// WHERE id IN (1, 2, 3)
		for _, item := range clause.GetList().Items {
			err := parseExpression(ctx, item, parseRe)
			if err != nil {
				return err
			}
		}
	case clause.GetSubLink() != nil:
		// WHERE id IN (SELECT id FROM foo)
		subselect := clause.GetSubLink().GetSubselect()
		if subselect.GetSelectStmt() == nil {
			return fmt.Errorf(
				"unsupported subquery type: %v", subselect)
		}
		queryParams, err := validateSelectStmt(ctx, subselect.GetSelectStmt())
		if err != nil {
			return err
		}
		if len(queryParams) > 0 {
			AddQueryParams(&parseRe.Params, queryParams)
		}
	case clause.GetCoalesceExpr() != nil:
		// TODO should this be [0]?
		return parseExpression(ctx, clause.GetCoalesceExpr().GetArgs()[0], parseRe)
	case clause.GetWindowDef() != nil:
		return parseWindowDef(ctx, clause.GetWindowDef(), parseRe)
	case clause.GetSortBy() != nil:
		return parseExpression(ctx, clause.GetSortBy().Node, parseRe)
	default:
		return fmt.Errorf(
			"unsupported expression, found node of type: %v",
			reflect.TypeOf(clause),
		)
	}

	return nil
}

// find used column names from where clause
func parseWhereClause(ctx VetContext, clause *pg_query.Node, parseRe *ParseResult) error {
	err := parseExpression(ctx, clause, parseRe)
	if err != nil {
		err = fmt.Errorf("invalid WHERE clause: %w", err)
	}
	return err
}

func getUsedColumnsFromNodeList(nodelist []*pg_query.Node) []ColumnUsed {
	usedCols := []ColumnUsed{}
	for _, item := range nodelist {
		if item.GetColumnRef() != nil {
			cu := columnRefToColumnUsed(item.GetColumnRef())
			if cu != nil {
				usedCols = append(usedCols, *cu)
			}
		}
	}
	return usedCols
}

func getUsedColumnsFromSortClause(sortList []*pg_query.Node) []ColumnUsed {
	usedCols := []ColumnUsed{}
	for _, item := range sortList {
		if item.GetSortBy() != nil {
			if colRef := item.GetSortBy().GetNode().GetColumnRef(); colRef != nil {
				cu := columnRefToColumnUsed(colRef)
				if cu != nil {
					usedCols = append(usedCols, *cu)
				}
			}
		}
	}
	return usedCols
}

func validateSelectStmt(ctx VetContext, stmt *pg_query.SelectStmt) ([]QueryParam, error) {
	usedTables := getUsedTablesFromSelectStmt(stmt.FromClause)

	usedCols := []ColumnUsed{}
	queryParams := []QueryParam{}

	for _, item := range stmt.TargetList {
		if item.GetResTarget() == nil {
			continue
		}

		re := &ParseResult{}
		err := parseExpression(ctx, item.GetResTarget().Val, re)
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

	if len(stmt.GroupClause) > 0 {
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

	if len(stmt.WindowClause) > 0 {
		re := &ParseResult{}
		// TODO: should this be [0]?
		err := parseExpression(ctx, stmt.WindowClause[0], re)
		if err != nil {
			return nil, err
		}
		usedCols = append(usedCols, re.Columns...)
		AddQueryParams(&queryParams, re.Params)
	}

	if len(stmt.SortClause) > 0 {
		usedCols = append(usedCols, getUsedColumnsFromSortClause(stmt.SortClause)...)
	}

	return queryParams, validateTableColumns(ctx, usedTables, usedCols)
}

func validateUpdateStmt(ctx VetContext, stmt *pg_query.UpdateStmt) ([]QueryParam, error) {
	tableName := stmt.Relation.Relname
	usedTables := []TableUsed{{Name: tableName}}
	usedTables = append(usedTables, getUsedTablesFromSelectStmt(stmt.FromClause)...)

	usedCols := []ColumnUsed{}
	queryParams := []QueryParam{}

	for _, item := range stmt.TargetList {
		target := item.GetResTarget()
		usedCols = append(usedCols, ColumnUsed{
			Table:    tableName,
			Column:   target.Name,
			Location: target.Location,
		})

		switch {
		case target.Val != nil && target.Val.GetColumnRef() != nil:
			cu := columnRefToColumnUsed(target.Val.GetColumnRef())
			if cu != nil {
				usedCols = append(usedCols, *cu)
			}
		case target.Val != nil && target.Val.GetParamRef() != nil:
			AddQueryParam(&queryParams, QueryParam{Number: target.Val.GetParamRef().Number})
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

	if len(stmt.ReturningList) > 0 {
		usedCols = append(usedCols, getUsedColumnsFromReturningList(stmt.ReturningList)...)
	}

	return queryParams, validateTableColumns(ctx, usedTables, usedCols)
}

func validateInsertStmt(ctx VetContext, stmt *pg_query.InsertStmt) ([]QueryParam, error) {
	tableName := stmt.Relation.Relname
	usedTables := []TableUsed{{Name: tableName}}

	targetCols := []ColumnUsed{}
	for _, item := range stmt.Cols {
		target := item.GetResTarget()
		targetCols = append(targetCols, ColumnUsed{
			Table:    tableName,
			Column:   target.Name,
			Location: target.Location,
		})
	}

	values := []*pg_query.Node{}
	// make a copy of targetCols because we need it to do value count
	// validation separately
	usedCols := append([]ColumnUsed{}, targetCols...)
	queryParams := []QueryParam{}

	selectStmt := stmt.GetSelectStmt().GetSelectStmt()
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
		for _, list := range selectStmt.GetValuesLists() {
			for _, node := range list.GetList().Items {
				re := &ParseResult{}
				err := parseExpression(ctx, node, re)
				if err != nil {
					return nil, fmt.Errorf("invalid value list: %w", err)
				}
				if len(re.Columns) > 0 {
					usedCols = append(usedCols, re.Columns...)
				}
				if len(re.Params) > 0 {
					AddQueryParams(&queryParams, re.Params)
				}
				values = append(values, node)
			}
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

		for _, item := range selectStmt.TargetList {
			target := item.GetResTarget().Val
			values = append(values, target)

			switch {
			case target.GetColumnRef() != nil:
				cu := columnRefToColumnUsed(target.GetColumnRef())
				if cu == nil {
					continue
				}
				usedCols = append(usedCols, *cu)
			case target.GetSubLink() != nil:
				tv := target.GetSubLink().Subselect
				if tv.GetSelectStmt() == nil {
					return nil, fmt.Errorf(
						"unsupported subquery type in value list: %s", reflect.TypeOf(tv))
				}
				qparams, err := validateSelectStmt(ctx, tv.GetSelectStmt())
				if err != nil {
					return nil, fmt.Errorf("invalid SELECT query in value list: %w", err)
				}
				if len(qparams) > 0 {
					AddQueryParams(&queryParams, qparams)
				}
			}
		}
	}

	if len(stmt.ReturningList) > 0 {
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

func validateDeleteStmt(ctx VetContext, stmt *pg_query.DeleteStmt) ([]QueryParam, error) {
	tableName := stmt.Relation.Relname
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

	if len(stmt.ReturningList) > 0 {
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
	tree, err := pg_query.Parse(queryStr)
	if err != nil {
		return nil, err
	}

	if len(tree.Stmts) == 0 || len(tree.Stmts) > 1 {
		return nil, fmt.Errorf("query contained more than one statement")
	}

	var raw *pg_query.RawStmt = tree.Stmts[0]
	switch {
	case raw.Stmt.GetSelectStmt() != nil:
		return validateSelectStmt(ctx, raw.Stmt.GetSelectStmt())
	case raw.Stmt.GetUpdateStmt() != nil:
		return validateUpdateStmt(ctx, raw.Stmt.GetUpdateStmt())
	case raw.Stmt.GetInsertStmt() != nil:
		return validateInsertStmt(ctx, raw.Stmt.GetInsertStmt())
	case raw.Stmt.GetDeleteStmt() != nil:
		return validateDeleteStmt(ctx, raw.Stmt.GetDeleteStmt())
	case raw.Stmt.GetDropStmt() != nil:
	case raw.Stmt.GetTruncateStmt() != nil:
	case raw.Stmt.GetAlterTableStmt() != nil:
	case raw.Stmt.GetCreateSchemaStmt() != nil:
	case raw.Stmt.GetVariableSetStmt() != nil:
	// TODO: check for invalid pg variables
	default:
		return nil, fmt.Errorf("unsupported statement: %v", reflect.TypeOf(raw.Stmt))
	}

	return nil, nil
}
