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

const Version = "1.0.0"

var (
	FlagErrFormat = false
)

type SqlVet struct {
	QueryCnt int32
	ErrCnt   int32

	Cfg         config.Config
	ProjectRoot string
	Schema      *schema.Db
}

func (s *SqlVet) reportError(format string, a ...interface{}) {
	cli.Error(format, a...)
	atomic.AddInt32(&s.ErrCnt, 1)
}

func (s *SqlVet) Vet() {
	queries, err := vet.CheckDir(
		vet.VetContext{
			Schema: s.Schema,
		},
		s.ProjectRoot,
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
		if FlagErrFormat {
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

func (s *SqlVet) PrintSummary() {
	cli.Show("Checked %d SQL queries.", s.QueryCnt)
	if s.ErrCnt == 0 {
		cli.Success("🎉 Everything is awesome!")
	} else {
		cli.Error("Identified %d errors.", s.ErrCnt)
	}
}

func NewSqlVet(projectRoot string) (*SqlVet, error) {
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
		if !FlagErrFormat {
			cli.Show("Loaded DB schema from %s", cfg.SchemaPath)
			for k, v := range dbSchema.Tables {
				cli.Show("\ttable %s with %d columns", k, len(v.Columns))
			}
		}
	} else {
		if !FlagErrFormat {
			cli.Show("[!] No schema specified, will run without table and column validation.")
		}
	}

	return &SqlVet{
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
		Version: Version,
		PreRun: func(cmd *cobra.Command, args []string) {
			if cli.Verbose {
				log.SetLevel(log.DebugLevel)
			}
		},
		Run: func(cmd *cobra.Command, args []string) {
			projectRoot := args[0]
			s, err := NewSqlVet(projectRoot)
			if err != nil {
				cli.Exit(err)
			}
			s.Vet()

			if !FlagErrFormat {
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
		&FlagErrFormat, "errorformat", "e", false,
		"output error in errorformat fromat for easier integration")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
