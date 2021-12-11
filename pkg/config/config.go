package config

import (
	"io/ioutil"
	"os"

	"github.com/pelletier/go-toml"

	"github.com/houqp/sqlvet/pkg/vet"
)

// Sqlvet project config
type Config struct {
	DbEngine        string               `toml:"db_engine"`
	SchemaPath      string               `toml:"schema_path"`
	SqlFuncMatchers []vet.SqlFuncMatcher `toml:"sqlfunc_matchers"`
}

// Load sqlvet config from project root
func Load(configPath string) (conf Config, err error) {
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
