package vet

import (
	"errors"
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/callgraph/rta"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"

	log "github.com/sirupsen/logrus"

	"github.com/houqp/sqlvet/pkg/parseutil"
)

var (
	ErrQueryArgUnsupportedType = errors.New("unexpected query arg type")
	ErrQueryArgUnsafe          = errors.New("potentially unsafe query string")
	ErrQueryArgTODO            = errors.New("TODO: support this type")
)

const (
	sqlxLib   = "github.com/jmoiron/sqlx"
	dbSqlLib  = "database/sql"
	gormLib   = "github.com/jinzhu/gorm"
	goGorpLib = "go-gorp/gorp"
	gorpV1Lib = "gopkg.in/gorp.v1"

	queryArgName = "query"
	sqlArgName   = "sql"

	rebindMethodName  = "Rebind"
	rebindxMethodName = "Rebindx"
)

type QuerySite struct {
	Called            string
	Position          token.Position
	Query             string
	ParameterArgCount int
	Err               error
}

type MatchedSqlFunc struct {
	SSA         *ssa.Function
	QueryArgPos int
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

	pkg *packages.Package
}

func (s *SqlFuncMatcher) SetGoPackage(p *packages.Package) {
	s.pkg = p
}

func (s *SqlFuncMatcher) PackageImported() bool {
	return s.pkg != nil
}

func (s *SqlFuncMatcher) IterPackageExportedFuncs(cb func(*types.Func)) {
	scope := s.pkg.Types.Scope()
	for _, scopeName := range scope.Names() {
		obj := scope.Lookup(scopeName)
		if !obj.Exported() {
			continue
		}

		fobj, ok := obj.(*types.Func)
		if ok {
			cb(fobj)
		} else {
			// check for exported struct methods
			switch otype := obj.Type().(type) {
			case *types.Signature:
			case *types.Named:
				for i := 0; i < otype.NumMethods(); i++ {
					m := otype.Method(i)
					if !m.Exported() {
						continue
					}
					cb(m)
				}
			case *types.Basic:
			default:
				log.Debugf("Skipped pkg scope: %s (%s)", otype, reflect.TypeOf(otype))
			}
		}
	}
}

func (s *SqlFuncMatcher) MatchSqlFuncs(prog *ssa.Program) []MatchedSqlFunc {
	sqlfuncs := []MatchedSqlFunc{}

	s.IterPackageExportedFuncs(func(fobj *types.Func) {
		ssaFunc := prog.FuncValue(fobj)

		// Skip pass-through functions that shouldn't be validated as SQL functions
		if isPassThroughFunc(ssaFunc) {
			return
		}

		for _, rule := range s.Rules {
			if rule.FuncName != "" && fobj.Name() == rule.FuncName {
				sqlfuncs = append(sqlfuncs, MatchedSqlFunc{
					SSA:         ssaFunc,
					QueryArgPos: rule.QueryArgPos,
				})
				// callable matched one rule, no need to go through the rest
				break
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
				sqlfuncs = append(sqlfuncs, MatchedSqlFunc{
					SSA:         ssaFunc,
					QueryArgPos: rule.QueryArgPos,
				})
				// callable matched one rule, no need to go through the rest
				break
			}
		}
	})

	return sqlfuncs
}

// isNamedQueryFunc checks if a function name is a "named query" function
// that expects named parameters (like :param) instead of positional ($1, $2)
func isNamedQueryFunc(funcName string) bool {
	// Check for sqlx named query functions
	switch funcName {
	case "NamedExec", "NamedQuery", "NamedExecContext", "NamedQueryContext",
		"NamedQueryRow", "NamedQueryRowContext":
		return true
	}
	// Also check if the function name contains "Named" (catches custom wrappers)
	return strings.Contains(funcName, "Named")
}

func handleQuery(ctx VetContext, qs *QuerySite) {
	// Only apply named query resolution for named query functions
	// (e.g., NamedExec, NamedQuery, NamedExecContext, NamedQueryContext)
	// to avoid breaking PostgreSQL type casts (::) in regular queries
	if isNamedQueryFunc(qs.Called) {
		qs.Query, _, qs.Err = parseutil.CompileNamedQuery(
			[]byte(qs.Query), parseutil.BindType("postgres"))
		if qs.Err != nil {
			return
		}
	}

	var queryParams []QueryParam
	queryParams, qs.Err = ValidateSqlQuery(NewContext(ctx.Schema.Tables), qs.Query)

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
			PkgPath: sqlxLib,
			Rules: []SqlFuncMatchRule{
				{QueryArgName: queryArgName},
				{QueryArgName: sqlArgName},
				// for methods with Context suffix
				{QueryArgName: queryArgName, QueryArgPos: 1},
				{QueryArgName: sqlArgName, QueryArgPos: 1},
				{QueryArgName: queryArgName, QueryArgPos: 2},
				{QueryArgName: sqlArgName, QueryArgPos: 2},
			},
		},
		{
			PkgPath: dbSqlLib,
			Rules: []SqlFuncMatchRule{
				{QueryArgName: queryArgName},
				{QueryArgName: sqlArgName},
				// for methods with Context suffix
				{QueryArgName: queryArgName, QueryArgPos: 1},
				{QueryArgName: sqlArgName, QueryArgPos: 1},
			},
		},
		{
			PkgPath: gormLib,
			Rules: []SqlFuncMatchRule{
				{QueryArgName: sqlArgName},
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
			PkgPath: goGorpLib,
			Rules: []SqlFuncMatchRule{
				{QueryArgName: queryArgName},
			},
		},
		{
			PkgPath: gorpV1Lib,
			Rules: []SqlFuncMatchRule{
				{QueryArgName: queryArgName},
			},
		},
	}
	if extraMatchers != nil {
		for _, m := range extraMatchers {
			tmpm := m
			matchers = append(matchers, &tmpm)
		}
	}

	return matchers
}

func loadGoPackages(dir string, buildFlags string) ([]*packages.Package, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName |
			packages.NeedFiles |
			packages.NeedImports |
			packages.NeedDeps |
			packages.NeedTypes |
			packages.NeedSyntax |
			packages.NeedTypesInfo,
		Dir: dir,
		Env: append(os.Environ(), "GO111MODULE=auto"),
	}
	if buildFlags != "" {
		cfg.BuildFlags = strings.Split(buildFlags, " ")
	}
	dirAbs, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("invalid path: %w", err)
	}
	pkgPath := dirAbs + "/..."
	pkgs, err := packages.Load(cfg, pkgPath)
	if err != nil {
		return nil, err
	}
	// return early if any syntax error
	for _, pkg := range pkgs {
		if len(pkg.Errors) > 0 {
			return nil, fmt.Errorf("failed to load package, %w", pkg.Errors[0])
		}
	}
	return pkgs, nil
}

// isPassThroughMethodName checks if a method name is known to be a pass-through
func isPassThroughMethodName(methodName string) bool {
	switch methodName {
	case rebindMethodName, rebindxMethodName:
		return true
	}
	return false
}

// isPassThroughFunc checks if a function is known to be a pass-through
// that transforms query syntax without changing semantic meaning
func isPassThroughFunc(fn *ssa.Function) bool {
	if fn == nil {
		return false
	}

	// Get the package path and function name
	if fn.Pkg != nil && fn.Pkg.Pkg != nil {
		pkgPath := fn.Pkg.Pkg.Path()
		funcName := fn.Name()

		// sqlx package pass-through functions
		if pkgPath == sqlxLib && isPassThroughMethodName(funcName) {
			return true
		}
	}

	// Check by receiver type for methods
	if fn.Signature.Recv() != nil {
		recv := fn.Signature.Recv()
		recvType := recv.Type().String()
		funcName := fn.Name()

		// sqlx methods that are pass-through
		if strings.HasPrefix(recvType, sqlxLib+".") && isPassThroughMethodName(funcName) {
			return true
		}
	}

	return false
}

func extractQueryStrFromSsaValue(argVal ssa.Value) (string, error) {
	queryStr := ""

	switch queryArg := argVal.(type) {
	case *ssa.Const:
		queryStr = constant.StringVal(queryArg.Value)
	case *ssa.Phi:
		// TODO: resolve all phi options
		// for _, edge := range queryArg.Edges {
		// }
		log.Debug("TODO(callgraph) support ssa.Phi")
		return "", ErrQueryArgTODO
	case *ssa.BinOp:
		// only support string concat
		switch queryArg.Op {
		case token.ADD:
			lstr, err := extractQueryStrFromSsaValue(queryArg.X)
			if err != nil {
				return "", err
			}
			rstr, err := extractQueryStrFromSsaValue(queryArg.Y)
			if err != nil {
				return "", err
			}
			queryStr = lstr + rstr
		default:
			return "", ErrQueryArgUnsupportedType
		}
	case *ssa.Parameter:
		// query call is wrapped in a helper function, query string is passed
		// in as function parameter
		// TODO: need to trace the caller or add wrapper function to
		// matcher config
		return "", ErrQueryArgTODO
	case *ssa.Extract:
		// query string is from one of the multi return values
		// Try to trace the source of the multi-value return
		if queryArg.Tuple == nil {
			return "", ErrQueryArgTODO
		}

		// Check if the tuple comes from a function call
		if call, ok := queryArg.Tuple.(*ssa.Call); ok {
			callee := call.Call.StaticCallee()
			if callee == nil {
				return "", ErrQueryArgTODO
			}

			// Check if the function has a body
			if len(callee.Blocks) == 0 {
				// External function, can't trace further
				return "", ErrQueryArgTODO
			}

			// Look for return instructions and extract the specific index
			for _, block := range callee.Blocks {
				for _, instr := range block.Instrs {
					if ret, ok := instr.(*ssa.Return); ok {
						if queryArg.Index >= len(ret.Results) {
							continue
						}
						// Extract the query string from the specific return value at this index
						return extractQueryStrFromSsaValue(ret.Results[queryArg.Index])
					}
				}
			}
		}

		return "", ErrQueryArgTODO
	case *ssa.Call:
		// return value from a function call
		// Try to trace the function to extract the query string
		callee := queryArg.Call.StaticCallee()

		// Check if this is a known pass-through function call
		// For interface calls, callee will be nil, so we check by method name
		if callee == nil {
			// Dynamic call (interface method, function value, etc.)
			// Check if it's a known pass-through method by name
			if queryArg.Call.IsInvoke() {
				method := queryArg.Call.Method
				if method != nil && isPassThroughMethodName(method.Name()) {
					// Extract the query from the first argument
					callArgs := queryArg.Call.Args
					if len(callArgs) > 0 {
						return extractQueryStrFromSsaValue(callArgs[0])
					}
				}
			}
			return "", ErrQueryArgUnsafe
		}

		// Handle known pass-through functions that just transform the query
		// without changing its semantic meaning (e.g., sqlx.Rebind)
		if isPassThroughFunc(callee) {
			// Extract the query from the first argument
			callArgs := queryArg.Call.Args
			if len(callArgs) > 0 {
				// For method calls, the receiver is not in Args, so Args[0] is the first parameter
				return extractQueryStrFromSsaValue(callArgs[0])
			}
			return "", ErrQueryArgUnsafe
		}

		// Check if the function has a body (not external or builtin)
		if len(callee.Blocks) == 0 {
			return "", ErrQueryArgUnsafe
		}

		// Look for return instructions in the function
		// This handles simple cases where the function returns a constant or computed value
		for _, block := range callee.Blocks {
			for _, instr := range block.Instrs {
				if ret, ok := instr.(*ssa.Return); ok {
					if len(ret.Results) == 0 {
						continue
					}
					// Recursively extract the query string from the first return value
					// This handles cases like:
					// func getQuery() string { return "SELECT * FROM users" }
					return extractQueryStrFromSsaValue(ret.Results[0])
				}
			}
		}

		return "", ErrQueryArgUnsafe
	case *ssa.MakeInterface:
		// query function takes interface as input
		// check to see if interface is converted from a string
		switch interfaceFrom := queryArg.X.(type) {
		case *ssa.Const:
			queryStr = constant.StringVal(interfaceFrom.Value)
		default:
			return "", ErrQueryArgUnsupportedType
		}
	case *ssa.Slice:
		// function takes var arg as input

		// Type() returns string if the type of X was string, otherwise a
		// *types.Slice with the same element type as X.
		if _, ok := queryArg.Type().(*types.Slice); ok {
			log.Debug("TODO(callgraph) support slice for vararg")
		}
		return "", ErrQueryArgTODO
	default:
		return "", ErrQueryArgUnsupportedType
	}

	return queryStr, nil
}

func shouldIgnoreNode(ignoreNodes []ast.Node, callSitePos token.Pos) bool {
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
		if callSitePos < n.End() && callSitePos > n.Pos() {
			return true
		}
	}

	return false
}

func iterCallGraphNodeCallees(ctx VetContext, cgNode *callgraph.Node, prog *ssa.Program, sqlfunc MatchedSqlFunc, ignoreNodes []ast.Node) []*QuerySite {
	var queries []*QuerySite

	for _, inEdge := range cgNode.In {
		callerFunc := inEdge.Caller.Func
		if callerFunc.Pkg == nil {
			// skip calls from dependencies
			continue
		}

		callSite := inEdge.Site
		callSitePos := callSite.Pos()
		if shouldIgnoreNode(ignoreNodes, callSitePos) {
			continue
		}

		callSitePosition := prog.Fset.Position(callSitePos)
		log.Debugf("Validating %s @ %s", sqlfunc.SSA, callSitePosition)

		callArgs := callSite.Common().Args

		absArgPos := sqlfunc.QueryArgPos
		if callSite.Common().IsInvoke() {
			// interface method invocation.
			// In this mode, Value is the interface value and Method is the
			// interface's abstract method. Note: an abstract method may be
			// shared by multiple interfaces due to embedding; Value.Type()
			// provides the specific interface used for this call.
		} else {
			// "call" mode: when Method is nil (!IsInvoke), a CallCommon
			// represents an ordinary function call of the value in Value,
			// which may be a *Builtin, a *Function or any other value of
			// kind 'func'.
			if sqlfunc.SSA.Signature.Recv() != nil {
				// it's a struct method call, plus 1 to take receiver into
				// account
				absArgPos += 1
			}
		}
		queryArg := callArgs[absArgPos]

		qs := &QuerySite{
			Called:   inEdge.Callee.Func.Name(),
			Position: callSitePosition,
			Err:      nil,
		}

		if len(callArgs) > absArgPos+1 {
			// query function accepts query parameters
			paramArg := callArgs[absArgPos+1]
			// only support query param as variadic argument for now
			switch params := paramArg.(type) {
			case *ssa.Const:
				// likely nil
			case *ssa.Slice:
				sliceType := params.X.Type()
				switch t := sliceType.(type) {
				case *types.Pointer:
					elem := t.Elem()
					switch e := elem.(type) {
					case *types.Array:
						// query parameters are passed in as vararg: an array
						// of interface
						qs.ParameterArgCount = int(e.Len())
					}
				}
			}
		}

		qs.Query, qs.Err = extractQueryStrFromSsaValue(queryArg)
		if qs.Err != nil {
			switch qs.Err {
			case ErrQueryArgUnsupportedType:
				log.WithFields(log.Fields{
					"type":      reflect.TypeOf(queryArg),
					"pos":       prog.Fset.Position(callSite.Pos()),
					"caller":    callerFunc,
					"callerPkg": callerFunc.Pkg,
				}).Debug(fmt.Errorf("unsupported type in callgraph: %w", qs.Err))
			case ErrQueryArgTODO:
				log.WithFields(log.Fields{
					"type":      reflect.TypeOf(queryArg),
					"pos":       prog.Fset.Position(callSite.Pos()),
					"caller":    callerFunc,
					"callerPkg": callerFunc.Pkg,
				}).Debug(fmt.Errorf("TODO(callgraph) %w", qs.Err))
				// skip to be supported query type
				continue
			default:
				queries = append(queries, qs)
				continue
			}
		}

		if qs.Query == "" {
			continue
		}
		handleQuery(ctx, qs)
		queries = append(queries, qs)
	}

	return queries
}

func getSortedIgnoreNodes(pkgs []*packages.Package) []ast.Node {
	ignoreNodes := []ast.Node{}

	for _, p := range pkgs {
		for _, s := range p.Syntax {
			cmap := ast.NewCommentMap(p.Fset, s, s.Comments)
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
	}

	sort.Slice(ignoreNodes, func(i, j int) bool {
		return ignoreNodes[i].Pos() < ignoreNodes[j].Pos()
	})

	return ignoreNodes
}

func CheckDir(ctx VetContext, dir, buildFlags string, extraMatchers []SqlFuncMatcher) ([]*QuerySite, error) {
	_, err := os.Stat(filepath.Join(dir, "go.mod"))
	if os.IsNotExist(err) {
		return nil, errors.New("sqlvet only supports projects using go modules for now")
	}

	pkgs, err := loadGoPackages(dir, buildFlags)
	if err != nil {
		return nil, err
	}
	log.Debugf("Loaded %d packages: %s", len(pkgs), pkgs)

	ignoreNodes := getSortedIgnoreNodes(pkgs)
	log.Debugf("Identified %d queries to ignore", len(ignoreNodes))

	// check to see if loaded packages imported any package that matches our rules
	matchers := getMatchers(extraMatchers)
	log.Debugf("Loaded %d matchers, checking imported SQL packages...", len(matchers))
	for _, matcher := range matchers {
		for _, p := range pkgs {
			v, ok := p.Imports[matcher.PkgPath]
			if !ok {
				continue
			}
			// package is imported by at least of the loaded packages
			matcher.SetGoPackage(v)
			log.Debugf("\t%s imported", matcher.PkgPath)
			break
		}
	}

	mode := ssa.InstantiateGenerics
	prog, ssaPkgs := ssautil.Packages(pkgs, mode)
	log.Debug("Performing whole-program analysis...")
	prog.Build()

	// find ssa.Function for matched sqlfuncs from program
	var sqlfuncs []MatchedSqlFunc
	for _, matcher := range matchers {
		if !matcher.PackageImported() {
			// if package is not imported, then no sqlfunc should be matched
			continue
		}
		sqlfuncs = append(sqlfuncs, matcher.MatchSqlFuncs(prog)...)
	}
	log.Debugf("Matched %d sqlfuncs", len(sqlfuncs))

	log.Debugf("Locating main packages from %d packages.", len(ssaPkgs))
	mains := ssautil.MainPackages(ssaPkgs)

	log.Debug("Building call graph...")
	var funcs []*ssa.Function
	for _, fn := range mains {
		if main := fn.Func("main"); main != nil {
			funcs = append(funcs, main)
		}
		if init := fn.Func("init"); init != nil {
			funcs = append(funcs, init)
		}
	}

	rtaRes := rta.Analyze(funcs, true)
	if rtaRes == nil {
		return nil, nil
	}

	var queries []*QuerySite
	cg := rtaRes.CallGraph
	for _, sqlfunc := range sqlfuncs {
		cgNode := cg.CreateNode(sqlfunc.SSA)
		queries = append(
			queries,
			iterCallGraphNodeCallees(ctx, cgNode, prog, sqlfunc, ignoreNodes)...)
	}

	return queries, nil
}
