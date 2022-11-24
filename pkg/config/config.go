package config

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pelletier/go-toml"

	"github.com/samiam2013/sqlvet/pkg/vet"
)

// Sqlvet project config
type Config struct {
	DbEngine        string               `toml:"db_engine"`
	SchemaPath      string               `toml:"schema_path"`
	BuildFlags      string               `toml:"build_flags"`
	SqlFuncMatchers []vet.SqlFuncMatcher `toml:"sqlfunc_matchers"`
}

// Load sqlvet config from project root
func Load(searchPath string) (conf Config, err error) {
	configPath := filepath.Join(searchPath, "sqlvet.toml")

	if _, e := os.Stat(configPath); os.IsNotExist(e) {
		conf.DbEngine = "postgres"
		// return default config if not found
		return
	}

	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return
	}

	err = toml.Unmarshal(data, &conf)
	return
}
