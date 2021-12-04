package vet

import (
	"errors"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"os"
	"sort"
	"strings"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/inspect"
	"golang.org/x/tools/go/analysis/singlechecker"
	"golang.org/x/tools/go/ast/inspector"

	log "github.com/sirupsen/logrus"

	"github.com/houqp/sqlvet/pkg/parseutil"
)

var (
	ErrQueryArgUnsupportedType = errors.New("unexpected query arg type")
	ErrQueryArgUnsafe          = errors.New("potentially unsafe query string")
	ErrQueryArgTODO            = errors.New("TODO: support this type")
)

type QuerySite struct {
	Called            string
	Position          token.Position
	Query             string
	ParameterArgCount int
	Err               error
}

type SqlFuncMatchRule struct {
	FuncName string `toml:"func_name"`
	// zero indexed
	QueryArgPos  int    `toml:"query_arg_pos"`
	QueryArgName string `toml:"query_arg_name"`
}

type SqlFuncMatcher struct {
	PkgPath string             `toml:"pkg_path"`
	Rules   []SqlFuncMatchRule `toml:"rules"`
}

func handleQuery(ctx VetContext, qs *QuerySite) {
	// TODO: apply named query resolution based on v.X type and v.Sel.Name
	// e.g. for sqlx, only apply to NamedExec and NamedQuery
	qs.Query, _, qs.Err = parseutil.CompileNamedQuery(
		[]byte(qs.Query), parseutil.BindType("postgres"))

	if qs.Err != nil {
		return
	}
	var queryParams []QueryParam
	queryParams, qs.Err = ValidateSqlQuery(ctx, qs.Query)

	// log.Printf("QQQQ: %v err=%v", qs.Query, qs.Err)

	if qs.Err != nil {
		return
	}

	// query string is valid, now validate parameter args if exists
	if qs.ParameterArgCount < len(queryParams) {
		// qs.Err = fmt.Errorf(
		// 	"Query expects %d parameters, but received %d from function call",
		// 	len(queryParams), qs.ParameterArgCount,
		// )
	}
}

func getMatchers(extraMatchers []SqlFuncMatcher) []*SqlFuncMatcher {
	matchers := []*SqlFuncMatcher{
		{
			PkgPath: "github.com/jmoiron/sqlx",
			Rules: []SqlFuncMatchRule{
				{QueryArgName: "query"},
				{QueryArgName: "sql"},
				// for methods with Context suffix
				{QueryArgName: "query", QueryArgPos: 1},
				{QueryArgName: "sql", QueryArgPos: 1},
				{QueryArgName: "query", QueryArgPos: 2},
				{QueryArgName: "sql", QueryArgPos: 2},
			},
		},
		{
			PkgPath: "database/sql",
			Rules: []SqlFuncMatchRule{
				{QueryArgName: "query"},
				{QueryArgName: "sql"},
				// for methods with Context suffix
				{QueryArgName: "query", QueryArgPos: 1},
				{QueryArgName: "sql", QueryArgPos: 1},
			},
		},
		{
			PkgPath: "github.com/jinzhu/gorm",
			Rules: []SqlFuncMatchRule{
				{QueryArgName: "sql"},
			},
		},
		// TODO: xorm uses vararg, which is not supported yet
		// &SqlFuncMatcher{
		// 	PkgPath: "xorm.io/xorm",
		// 	Rules: []SqlFuncMatchRule{
		// 		{FuncName: "SQL"},
		// 		{FuncName: "Sql"},
		// 		{FuncName: "Exec"},
		// 		{FuncName: "Query"},
		// 		{FuncName: "QueryInterface"},
		// 		{FuncName: "QueryString"},
		// 		{FuncName: "QuerySliceString"},
		// 	},
		// },
		{
			PkgPath: "go-gorp/gorp",
			Rules: []SqlFuncMatchRule{
				{QueryArgName: "query"},
			},
		},
		{
			PkgPath: "gopkg.in/gorp.v1",
			Rules: []SqlFuncMatchRule{
				{QueryArgName: "query"},
			},
		},
	}
	for _, m := range extraMatchers {
		tmpm := m
		matchers = append(matchers, &tmpm)
	}

	return matchers
}

func shouldIgnoreNode(pass *analysis.Pass, ignoreNodes []ast.Node, callSitePos token.Pos) bool {
	if len(ignoreNodes) == 0 {
		return false
	}

	if callSitePos < ignoreNodes[0].Pos() {
		return false
	}

	if callSitePos > ignoreNodes[len(ignoreNodes)-1].End() {
		return false
	}

	for _, n := range ignoreNodes {
		if callSitePos < n.End() && callSitePos >= n.Pos() {
			return true
		}
	}

	return false
}

func getSortedIgnoreNodes(pass *analysis.Pass) []ast.Node {
	ignoreNodes := []ast.Node{}
	for _, file := range pass.Files {
		cmap := ast.NewCommentMap(pass.Fset, file, file.Comments)
		for node, cglist := range cmap {
			for _, cg := range cglist {
				// Remove `//` and spaces from comment line to get the
				// actual comment text. We can't use cg.Text() directly
				// here due to change introduced in
				// https://github.com/golang/go/issues/37974
				ctext := cg.List[0].Text
				if !strings.HasPrefix(ctext, "//") {
					continue
				}
				ctext = strings.TrimSpace(ctext[2:])

				anno, err := ParseComment(ctext)
				if err != nil {
					continue
				}
				if anno.Ignore {
					ignoreNodes = append(ignoreNodes, node)
					log.Tracef("Ignore ast node from %d to %d", node.Pos(), node.End())
				}
			}
		}
	}

	sort.Slice(ignoreNodes, func(i, j int) bool {
		return ignoreNodes[i].Pos() < ignoreNodes[j].Pos()
	})

	return ignoreNodes
}

type sqlFunc struct {
	QueryArgPos int
}

func isSQLFunc(fobj *types.Func, matchers []*SqlFuncMatcher) *sqlFunc {
	if fobj.Pkg() == nil {
		// Parse error?
		return nil
	}
	fpkgPath := fobj.Pkg().Path()
	for _, m := range matchers {
		if m.PkgPath != fpkgPath {
			continue
		}
		for _, rule := range m.Rules {
			if rule.FuncName != "" && fobj.Name() == rule.FuncName {
				return &sqlFunc{QueryArgPos: rule.QueryArgPos}
			}

			if rule.QueryArgName != "" {
				sigParams := fobj.Type().(*types.Signature).Params()
				if sigParams.Len()-1 < rule.QueryArgPos {
					continue
				}
				param := sigParams.At(rule.QueryArgPos)
				if param.Name() != rule.QueryArgName {
					continue
				}
				return &sqlFunc{QueryArgPos: rule.QueryArgPos}
			}
		}
	}
	return nil
}

// NewAnalyzer creates an analysis.Analyzer for sqlvet.  Unlike typical
// Analyzers, it does not report errors directly. Instead it invokes "result"
// for every detected sql query/exec site.
func NewAnalyzer(ctx VetContext, extraMatchers []SqlFuncMatcher, result func(qs *QuerySite)) *analysis.Analyzer {
	matchers := getMatchers(extraMatchers)

	pos := func(pass *analysis.Pass, pos token.Pos) token.Position { return pass.Fset.Position(pos) }

	run := func(pass *analysis.Pass) (interface{}, error) {
		funcCache := map[*types.Func]*sqlFunc{}
		ignoredNodes := getSortedIgnoreNodes(pass)
		inspect := pass.ResultOf[inspect.Analyzer].(*inspector.Inspector)
		inspect.Preorder(
			[]ast.Node{(*ast.CallExpr)(nil)},
			func(n ast.Node) {
				call := n.(*ast.CallExpr)
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return
				}
				var fobj *types.Func
				if s := pass.TypesInfo.Selections[sel]; s != nil {
					if s.Kind() == types.MethodVal {
						// method (e.g. foo.String())
						fobj = s.Obj().(*types.Func)
					}
				} else {
					// package-qualified function (e.g. fmt.Errorf)
					obj := pass.TypesInfo.Uses[sel.Sel]
					if obj, ok := obj.(*types.Func); ok {
						fobj = obj
					}
				}
				if fobj == nil {
					// A cast operator.
					return
				}

				match, ok := funcCache[fobj]
				if !ok {
					match = isSQLFunc(fobj, matchers)
					funcCache[fobj] = match
				}
				if match == nil {
					return
				}

				args := call.Args
				if len(args) < match.QueryArgPos {
					log.Printf("%v: cannot extract extract the query arg #%d", pos(pass, n.Pos()), match.QueryArgPos)
					return
				}
				queryTypeVal, ok := pass.TypesInfo.Types[call.Args[match.QueryArgPos]]
				if !ok {
					log.Printf("%v: cannot get query type info", pos(pass, n.Pos()))
					return
				}
				if shouldIgnoreNode(pass, ignoredNodes, n.Pos()) {
					return
				}
				qs := &QuerySite{
					Called:            fobj.Name(),
					Position:          pos(pass, n.Pos()),
					ParameterArgCount: len(args) - match.QueryArgPos - 1,
				}
				if queryTypeVal.Value == nil || queryTypeVal.Value.Kind() != constant.String {
					qs.Err = ErrQueryArgUnsafe
				} else {
					qs.Query = constant.Val(queryTypeVal.Value).(string)
				}
				handleQuery(ctx, qs)
				result(qs)
			})
		return nil, nil
	}
	return &analysis.Analyzer{
		Name:     "sqlvet",                               // name of the analyzer
		Doc:      "todo",                                 // documentation
		Run:      run,                                    // perform your analysis here
		Requires: []*analysis.Analyzer{inspect.Analyzer}, // a set of analyzers which must run before the current one.
	}
}

// CheckPackages runs the sqlvet analyzer on the given set of packages. Function
// "result" is invoked for each SQL query or exec site detected by the analyzer.
func CheckPackages(ctx VetContext, paths []string, extraMatchers []SqlFuncMatcher, result func(qs *QuerySite)) error {
	analyzer := NewAnalyzer(ctx, extraMatchers, result)
	os.Args = append([]string{"unused"}, paths...)
	singlechecker.Main(analyzer)
	return nil
}
