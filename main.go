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

		if cli.Verbose || q.Err != nil {
			cli.Bold("%s @ %s", q.Called, q.Position)
			if q.Query != "" {
				cli.Show("\t%s\n", q.Query)
			}
		}

		if q.Err != nil {
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
		cli.Show("Loaded DB schema from %s", cfg.SchemaPath)
		for k, v := range dbSchema.Tables {
			cli.Show("\ttable %s with %d columns", k, len(v.Columns))
		}
	} else {
		cli.Show("[!] No schema specified, will run without table and column validation.")
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
			s.PrintSummary()

			if s.ErrCnt > 0 {
				os.Exit(1)
			}

		},
	}

	rootCmd.PersistentFlags().BoolVarP(&cli.Verbose, "verbose", "v", false, "verbose output")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
