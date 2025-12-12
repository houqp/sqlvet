package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/houqp/sqlvet/pkg/cli"
	"github.com/houqp/sqlvet/pkg/config"
	"github.com/houqp/sqlvet/pkg/schema"
	"github.com/houqp/sqlvet/pkg/vet"
)

const version = "1.1.11"

var (
	gitCommit     = "?"
	flagErrFormat = false
)

// SQLVet includes Everything needed for check actions
type SQLVet struct {
	QueryCnt int32
	ErrCnt   int32

	Cfg         config.Config
	ProjectRoot string
	Schema      *schema.Db
}

func (s *SQLVet) reportError(format string, a ...interface{}) {
	cli.Error(format, a...)
	atomic.AddInt32(&s.ErrCnt, 1)
}

// Vet performs static analysis
func (s *SQLVet) Vet() {
	queries, err := vet.CheckDir(
		vet.NewContext(s.Schema.Tables),
		s.ProjectRoot,
		s.Cfg.BuildFlags,
		s.Cfg.SqlFuncMatchers,
	)
	if err != nil {
		cli.Exit(err)
	}

	for _, q := range queries {
		atomic.AddInt32(&s.QueryCnt, 1)

		if q.Err == nil {
			if cli.Verbose {
				cli.Show("query detected at %s", q.Position)
			}
			continue
		}

		// an error in the query is detected
		if flagErrFormat {
			relFilePath, err := filepath.Rel(s.ProjectRoot, q.Position.Filename)
			if err != nil {
				relFilePath = s.ProjectRoot
			}
			// format ref: https://github.com/reviewdog/reviewdog#errorformat
			cli.Show(
				"%s:%d:%d: %v",
				relFilePath, q.Position.Line, q.Position.Column, q.Err)
		} else {
			cli.Bold("%s @ %s", q.Called, q.Position)
			if q.Query != "" {
				cli.Show("\t%s\n", q.Query)
			}

			s.reportError("\tERROR: %v", q.Err)
			switch q.Err {
			case vet.ErrQueryArgUnsafe:
				cli.Show("\tHINT: if this is a false positive, annotate with `// sqlvet: ignore` comment")
			}
			cli.Show("")
		}
	}
}

// PrintSummary dumps analysis stats into stdout
func (s *SQLVet) PrintSummary() {
	cli.Show("Checked %d SQL queries.", s.QueryCnt)
	if s.ErrCnt == 0 {
		cli.Success("🎉 Everything is awesome!")
	} else {
		cli.Error("Identified %d errors.", s.ErrCnt)
	}
}

// NewSQLVet creates SQLVet for a given project dir
func NewSQLVet(projectRoot string) (*SQLVet, error) {
	cfg, err := config.Load(projectRoot)
	if err != nil {
		return nil, err
	}

	var dbSchema *schema.Db
	if cfg.SchemaPath != "" {
		dbSchema, err = schema.NewDbSchema(filepath.Join(projectRoot, cfg.SchemaPath))
		if err != nil {
			return nil, err
		}
		if !flagErrFormat {
			cli.Show("Loaded DB schema from %s", cfg.SchemaPath)
			for k, v := range dbSchema.Tables {
				cli.Show("\ttable %s with %d columns", k, len(v.Columns))
			}
		}
	} else {
		if !flagErrFormat {
			cli.Show("[!] No schema specified, will run without table and column validation.")
		}
	}

	return &SQLVet{
		Cfg:         cfg,
		ProjectRoot: projectRoot,
		Schema:      dbSchema,
	}, nil
}

func main() {
	var rootCmd = &cobra.Command{
		Use:     "sqlvet PATH",
		Short:   "Go fearless SQL",
		Args:    cobra.ExactArgs(1),
		Version: fmt.Sprintf("%s (%s)", version, gitCommit),
		PreRun: func(cmd *cobra.Command, args []string) {
			if cli.Verbose {
				log.SetLevel(log.DebugLevel)
			}
		},
		Run: func(cmd *cobra.Command, args []string) {
			projectRoot := args[0]
			s, err := NewSQLVet(projectRoot)
			if err != nil {
				cli.Exit(err)
			}
			s.Vet()

			if !flagErrFormat {
				s.PrintSummary()
			}

			if s.ErrCnt > 0 {
				os.Exit(1)
			}

		},
	}

	rootCmd.PersistentFlags().BoolVarP(
		&cli.Verbose, "verbose", "v", false, "verbose output")
	rootCmd.PersistentFlags().BoolVarP(
		&flagErrFormat, "errorformat", "e", false,
		"output error in errorformat fromat for easier integration")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
