package vet

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/houqp/sqlvet/pkg/schema"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

type Schema struct {
	Tables map[string]schema.Table
}

func NewContext(tables map[string]schema.Table) VetContext {
	return VetContext{
		Schema:      Schema{Tables: tables},
		InnerSchema: Schema{Tables: map[string]schema.Table{}},
	}
}

type VetContext struct {
	Schema      Schema
	InnerSchema Schema
	UsedTables  []TableUsed
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

type PostponedNodes struct {
	RangeSubselectNodes []*pg_query.RangeSubselect
}

func (p *PostponedNodes) Parse(ctx VetContext, parseRe *ParseResult) (err error) {
	for _, r := range p.RangeSubselectNodes {
		if err = parseRangeSubselect(ctx, r, parseRe); err != nil {
			return err
		}
	}
	return nil
}

func (p *PostponedNodes) Append(other *PostponedNodes) {
	if other == nil {
		return
	}
	p.RangeSubselectNodes = append(p.RangeSubselectNodes, other.RangeSubselectNodes...)
}

type ParseResult struct {
	Columns []ColumnUsed
	Tables  []TableUsed
	Params  []QueryParam

	PostponedNodes *PostponedNodes
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
		cu.Table = colRef.Fields[0].GetString_().Sval
		// fmt.Printf("table: %s\n", cu.Table)
		colField = colRef.Fields[1]
	} else {
		// in the form of SELECT column FROM table
		colField = colRef.Fields[0]
	}

	switch {
	case colField.GetString_() != nil:
		cu.Column = colField.GetString_().Sval
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
// TODO ? maybe this should be moved to parseExpression() and be collected in ParseResult
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

func parseFromClause(ctx VetContext, clause *pg_query.Node, parseRe *ParseResult) error {
	err := parseExpression(ctx, clause, parseRe)
	if err != nil {
		err = fmt.Errorf("invalid FROM clause: %w", err)
	}
	return err
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

func validateTable(ctx VetContext, tname string, notReadOnly bool) error {
	if ctx.Schema.Tables == nil {
		return nil
	}
	t, ok := ctx.Schema.Tables[tname]
	if !ok {
		return fmt.Errorf("invalid table name: %s", tname)
	}
	if notReadOnly && t.ReadOnly {
		return fmt.Errorf("read-only table: %s", tname)
	}
	return nil
}

func validateTableColumns(ctx VetContext, tables []TableUsed, cols []ColumnUsed) error {
	if ctx.Schema.Tables == nil || ctx.InnerSchema.Tables == nil {
		return nil
	}

	var ok bool
	usedTables := map[string]schema.Table{}
	for _, tu := range tables {
		usedTables[tu.Name], ok = ctx.InnerSchema.Tables[tu.Name]
		if !ok {
			usedTables[tu.Name], ok = ctx.Schema.Tables[tu.Name]
			if !ok {
				return fmt.Errorf("invalid table name: %s", tu.Name)
			}
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
				if len(usedTables) == 1 {
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
		return parseSublink(ctx, clause.GetSubLink(), parseRe)
	case clause.GetCoalesceExpr() != nil:
		// TODO should this be [0]?
		return parseExpression(ctx, clause.GetCoalesceExpr().GetArgs()[0], parseRe)
	case clause.GetWindowDef() != nil:
		return parseWindowDef(ctx, clause.GetWindowDef(), parseRe)
	case clause.GetSortBy() != nil:
		return parseExpression(ctx, clause.GetSortBy().Node, parseRe)
	case clause.GetJoinExpr() != nil:
		return parseJoinExpr(ctx, clause.GetJoinExpr(), parseRe)
	case clause.GetRangeVar() != nil:
		parseRe.Tables = append(parseRe.Tables, rangeVarToTableUsed(clause.GetRangeVar()))
		return nil
	case clause.GetRangeSubselect() != nil:
		// LEFT JOIN LATERAL (SELECT id FROM foo) AS bar ON true
		if parseRe.PostponedNodes == nil {
			parseRe.PostponedNodes = &PostponedNodes{}
		}
		parseRe.PostponedNodes.RangeSubselectNodes = append(parseRe.PostponedNodes.RangeSubselectNodes, clause.GetRangeSubselect())
		return nil
	default:
		return fmt.Errorf(
			"unsupported expression, found node of type: %v (%s)",
			reflect.TypeOf(clause.Node), clause.String(),
		)
	}

	return nil
}

func parseSublink(ctx VetContext, clause *pg_query.SubLink, parseRe *ParseResult) error {
	subSelect := clause.GetSubselect()
	if subSelect.GetSelectStmt() == nil {
		return fmt.Errorf(
			"unsupported sublink subselect type: %v", subSelect)
	}
	queryParams, _, err := validateSelectStmt(ctx, subSelect.GetSelectStmt())
	if err != nil {
		return err
	}
	if len(queryParams) > 0 {
		AddQueryParams(&parseRe.Params, queryParams)
	}

	return nil
}

func parseRangeSubselect(ctx VetContext, clause *pg_query.RangeSubselect, parseRe *ParseResult) error {
	subQuery := clause.GetSubquery()
	if subQuery.GetSelectStmt() == nil {
		return fmt.Errorf("unsupported range subselect subquery type: %v", clause)
	}

	queryParams, targetCols, err := validateSelectStmt(ctx, subQuery.GetSelectStmt())
	if err != nil {
		return err
	}

	if len(queryParams) > 0 {
		AddQueryParams(&parseRe.Params, queryParams)
	}

	if clause.Alias == nil {
		return nil
	}

	t := schema.Table{
		Name:     clause.Alias.Aliasname,
		ReadOnly: true,
		Columns:  map[string]schema.Column{},
	}

	for _, col := range targetCols {
		t.Columns[col.Name] = col
	}

	ctx.InnerSchema.Tables[t.Name] = t
	parseRe.Tables = append(parseRe.Tables, TableUsed{Name: t.Name})

	return nil
}

func parseJoinExpr(ctx VetContext, clause *pg_query.JoinExpr, parseRe *ParseResult) error {
	err := parseExpression(ctx, clause.Larg, parseRe)
	if err != nil {
		return err
	}
	err = parseExpression(ctx, clause.Rarg, parseRe)
	if err != nil {
		return err
	}
	err = parseExpression(ctx, clause.Quals, parseRe)
	if err != nil {
		return err
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

func parseUsingClause(ctx VetContext, clause *pg_query.Node, parseRe *ParseResult) error {
	err := parseExpression(ctx, clause, parseRe)
	if err != nil {
		err = fmt.Errorf("invalid USING clause: %w", err)
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

func validateSelectStmt(ctx VetContext, stmt *pg_query.SelectStmt) (queryParams []QueryParam, targetCols []schema.Column, err error) {
	usedCols := []ColumnUsed{}

	if stmt.GetWithClause() != nil {
		if err := parseCTE(ctx, stmt.GetWithClause()); err != nil {
			return nil, nil, err
		}
	}
	postponed := PostponedNodes{}
	for _, fromClause := range stmt.FromClause {
		re := &ParseResult{}
		err := parseFromClause(ctx, fromClause, re)
		if err != nil {
			return nil, nil, err
		}
		if len(re.Columns) > 0 {
			usedCols = append(usedCols, re.Columns...)
		}
		if len(re.Tables) > 0 {
			ctx.UsedTables = append(ctx.UsedTables, re.Tables...)
		}
		if len(re.Params) > 0 {
			AddQueryParams(&queryParams, re.Params)
		}

		postponed.Append(re.PostponedNodes)
	}

	re := &ParseResult{}
	if err := postponed.Parse(ctx, re); err != nil {
		return nil, nil, err
	}
	if len(re.Columns) > 0 {
		usedCols = append(usedCols, re.Columns...)
	}
	if len(re.Tables) > 0 {
		ctx.UsedTables = append(ctx.UsedTables, re.Tables...)
	}
	if len(re.Params) > 0 {
		AddQueryParams(&queryParams, re.Params)
	}

	for _, item := range stmt.TargetList {
		resTarget := item.GetResTarget()
		if resTarget == nil {
			continue
		}

		re := &ParseResult{}
		err := parseExpression(ctx, resTarget.Val, re)
		if err != nil {
			return nil, nil, err
		}
		if len(re.Columns) > 0 {
			usedCols = append(usedCols, re.Columns...)
		}
		if len(re.Params) > 0 {
			AddQueryParams(&queryParams, re.Params)
		}

		if col, ok := schema.GetResTargetColumn(resTarget); ok {
			targetCols = append(targetCols, col)
		}
	}

	if stmt.WhereClause != nil {
		re := &ParseResult{}
		err := parseWhereClause(ctx, stmt.WhereClause, re)
		if err != nil {
			return nil, nil, err
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
			return nil, nil, err
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
			return nil, nil, err
		}
		usedCols = append(usedCols, re.Columns...)
		AddQueryParams(&queryParams, re.Params)
	}

	if len(stmt.SortClause) > 0 {
		usedCols = append(usedCols, getUsedColumnsFromSortClause(stmt.SortClause)...)
	}

	return queryParams, targetCols, validateTableColumns(ctx, ctx.UsedTables, usedCols)
}

func validateUpdateStmt(ctx VetContext, stmt *pg_query.UpdateStmt) ([]QueryParam, []ColumnUsed, error) {
	if stmt.GetWithClause() != nil {
		if err := parseCTE(ctx, stmt.GetWithClause()); err != nil {
			return nil, nil, err
		}
	}
	tableName := stmt.Relation.Relname
	if err := validateTable(ctx, tableName, true); err != nil {
		return nil, nil, err
	}

	var tableAlias string
	if stmt.Relation.Alias != nil {
		tableAlias = stmt.Relation.Alias.Aliasname
	}

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
			return nil, nil, err
		}
		usedCols = append(usedCols, re.Columns...)
		AddQueryParams(&queryParams, re.Params)
	}

	if len(stmt.ReturningList) > 0 {
		usedCols = append(usedCols, getUsedColumnsFromReturningList(stmt.ReturningList)...)
	}

	if len(usedCols) > 0 {
		usedTables = append(usedTables, TableUsed{Name: tableName, Alias: tableAlias})
		if err := validateTableColumns(ctx, usedTables, usedCols); err != nil {
			return nil, nil, err
		}
	}

	return queryParams, usedCols, nil
}

func validateInsertStmt(ctx VetContext, stmt *pg_query.InsertStmt) ([]QueryParam, []ColumnUsed, error) {
	if stmt.GetWithClause() != nil {
		if err := parseCTE(ctx, stmt.GetWithClause()); err != nil {
			return nil, nil, err
		}
	}
	tableName := stmt.Relation.Relname
	if err := validateTable(ctx, tableName, true); err != nil {
		return nil, nil, err
	}
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
					return nil, nil, fmt.Errorf("invalid value list: %w", err)
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

		for _, fromClause := range selectStmt.FromClause {
			re := &ParseResult{}
			err := parseFromClause(ctx, fromClause, re)
			if err != nil {
				return nil, nil, err
			}
			if len(re.Columns) > 0 {
				usedCols = append(usedCols, re.Columns...)
			}
			if len(re.Params) > 0 {
				AddQueryParams(&queryParams, re.Params)
			}
		}

		if selectStmt.WhereClause != nil {
			re := &ParseResult{}
			err := parseWhereClause(ctx, selectStmt.WhereClause, re)
			if err != nil {
				return nil, nil, err
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
					return nil, nil, fmt.Errorf(
						"unsupported subquery type in value list: %s", reflect.TypeOf(tv))
				}
				qparams, _, err := validateSelectStmt(ctx, tv.GetSelectStmt())
				if err != nil {
					return nil, nil, fmt.Errorf("invalid SELECT query in value list: %w", err)
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
		return nil, nil, err
	}

	if err := validateInsertValues(ctx, targetCols, values); err != nil {
		return nil, nil, err
	}

	return queryParams, usedCols, nil
}

func validateDeleteStmt(ctx VetContext, stmt *pg_query.DeleteStmt) ([]QueryParam, []ColumnUsed, error) {
	if stmt.GetWithClause() != nil {
		if err := parseCTE(ctx, stmt.GetWithClause()); err != nil {
			return nil, nil, err
		}
	}
	tableName := stmt.Relation.Relname
	var tableAlias string

	if stmt.Relation.Alias != nil {
		tableAlias = stmt.Relation.Alias.Aliasname
	}

	if err := validateTable(ctx, tableName, true); err != nil {
		return nil, nil, err
	}

	usedCols := []ColumnUsed{}
	queryParams := []QueryParam{}
	usedTables := []TableUsed{}

	if stmt.WhereClause != nil {
		re := &ParseResult{}
		err := parseWhereClause(ctx, stmt.WhereClause, re)
		if err != nil {
			return nil, nil, err
		}
		if len(re.Columns) > 0 {
			usedCols = append(usedCols, re.Columns...)
		} else {
			return nil, nil, fmt.Errorf("no columns in DELETE's WHERE clause")
		}
		if len(re.Params) > 0 {
			queryParams = re.Params
		}
	} else {
		return nil, nil, fmt.Errorf("no WHERE clause for DELETE")
	}

	for _, using := range stmt.UsingClause {
		re := &ParseResult{}
		err := parseUsingClause(ctx, using, re)
		if err != nil {
			return nil, nil, err
		}
		usedTables = append(usedTables, re.Tables...)
	}

	if len(stmt.ReturningList) > 0 {
		usedCols = append(
			usedCols, getUsedColumnsFromReturningList(stmt.ReturningList)...)
	}

	if len(usedCols) > 0 {
		usedTables = append(usedTables, TableUsed{Name: tableName, Alias: tableAlias})
		if err := validateTableColumns(ctx, usedTables, usedCols); err != nil {
			return nil, nil, err
		}
	}

	return queryParams, usedCols, nil
}

func parseCTE(ctx VetContext, with *pg_query.WithClause) error {
	for _, cteNode := range with.Ctes {
		cte := cteNode.GetCommonTableExpr()
		query := cte.GetCtequery()
		_, cols, err := validateSqlQuery(ctx, query)
		if err != nil {
			return err
		}

		var columns map[string]schema.Column
		if cols != nil {
			columns = make(map[string]schema.Column)
			for _, col := range cols {
				columns[col.Column] = schema.Column{
					Name: col.Column,
				}
			}
		}

		ctx.InnerSchema.Tables[cte.Ctename] = schema.Table{
			Name:     cte.Ctename,
			Columns:  columns,
			ReadOnly: true,
		}
	}
	return nil
}

func ValidateSqlQuery(ctx VetContext, queryStr string) ([]QueryParam, error) {
	tree, err := pg_query.Parse(queryStr)
	if err != nil {
		return nil, err
	}

	if len(tree.Stmts) == 0 || len(tree.Stmts) > 1 {
		return nil, fmt.Errorf("query contained more than one statement")
	}

	params, _, err := validateSqlQuery(ctx, tree.Stmts[0].Stmt)
	return params, err
}

func ValidateSqlQueries(ctx VetContext, queryStr string) ([][]QueryParam, error) {
	var ret [][]QueryParam

	tree, err := pg_query.Parse(queryStr)
	if err != nil {
		return nil, err
	}

	for _, s := range tree.Stmts {
		params, _, err := validateSqlQuery(ctx, s.Stmt)
		if err != nil {
			return nil, err
		}
		ret = append(ret, params)
	}

	return ret, nil
}

func validateSqlQuery(ctx VetContext, node *pg_query.Node) ([]QueryParam, []ColumnUsed, error) {

	switch {
	case node.GetSelectStmt() != nil:
		qparams, targetCols, err := validateSelectStmt(ctx, node.GetSelectStmt())
		var cused []ColumnUsed
		for _, tcol := range targetCols {
			cused = append(cused, ColumnUsed{Column: tcol.Name})
		}
		return qparams, cused, err
	case node.GetUpdateStmt() != nil:
		return validateUpdateStmt(ctx, node.GetUpdateStmt())
	case node.GetInsertStmt() != nil:
		return validateInsertStmt(ctx, node.GetInsertStmt())
	case node.GetDeleteStmt() != nil:
		return validateDeleteStmt(ctx, node.GetDeleteStmt())
	case node.GetDropStmt() != nil:
	case node.GetTruncateStmt() != nil:
	case node.GetAlterTableStmt() != nil:
	case node.GetCreateSchemaStmt() != nil:
	case node.GetVariableSetStmt() != nil:

	// TODO: check for invalid pg variables
	default:
		return nil, nil, fmt.Errorf("unsupported statement: %v", reflect.TypeOf(node))
	}

	return nil, nil, nil
}
