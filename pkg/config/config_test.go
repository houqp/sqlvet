package config_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/houqp/gtest"
	"github.com/stretchr/testify/assert"

	"github.com/space307/sqlvet/pkg/config"
)

type ConfigTmpDir struct{}

func (s ConfigTmpDir) Construct(t *testing.T, fixtures struct{}) (string, string) {
	dir, err := ioutil.TempDir("", "gosource-tmpdir")
	assert.NoError(t, err)
	return dir, dir
}

func (s ConfigTmpDir) Destruct(t *testing.T, dir string) {
	os.RemoveAll(dir)
}

func init() {
	gtest.MustRegisterFixture("ConfigTmpDir", &ConfigTmpDir{}, gtest.ScopeSubTest)
}

type ConfigTests struct{}

func (s *ConfigTests) Setup(t *testing.T)      {}
func (s *ConfigTests) Teardown(t *testing.T)   {}
func (s *ConfigTests) BeforeEach(t *testing.T) {}
func (s *ConfigTests) AfterEach(t *testing.T)  {}

func (s *ConfigTests) SubTestMultipleMatchers(t *testing.T, fixtures struct {
	TmpDir string `fixture:"ConfigTmpDir"`
}) {
	configPath := filepath.Join(fixtures.TmpDir, "sqlvet.toml")
	err := ioutil.WriteFile(configPath, []byte(`
[[sqlfunc_matchers]]
  pkg_path = "github.com/mattermost/gorp"
  [[sqlfunc_matchers.rules]]
    query_arg_name = "query"

[[sqlfunc_matchers]]
  pkg_path = "github.com/mattermost/mattermost-server/v5"
  [[sqlfunc_matchers.rules]]
    query_arg_name = "sqlCommand"
    query_arg_pos = 2
`), 0644)
	assert.NoError(t, err)

	cfg, err := config.Load(fixtures.TmpDir)
	assert.NoError(t, err)

	assert.Equal(t, 2, len(cfg.SqlFuncMatchers))

	assert.Equal(t, "github.com/mattermost/gorp", cfg.SqlFuncMatchers[0].PkgPath)
	assert.Equal(t, 1, len(cfg.SqlFuncMatchers[0].Rules))

	assert.Equal(t, "github.com/mattermost/mattermost-server/v5", cfg.SqlFuncMatchers[1].PkgPath)
	assert.Equal(t, 1, len(cfg.SqlFuncMatchers[1].Rules))
}

// should return default config if config file is not found
func (s *ConfigTests) SubTestNoConfigFile(t *testing.T, fixtures struct {
	TmpDir string `fixture:"ConfigTmpDir"`
}) {
	configPath := filepath.Join(fixtures.TmpDir, "sqlvet.toml")
	_, e := os.Stat(configPath)
	assert.True(t, os.IsNotExist(e))

	cfg, err := config.Load(fixtures.TmpDir)
	assert.NoError(t, err)
	assert.Equal(t, config.Config{DbEngine: "postgres"}, cfg)
}

func TestConfig(t *testing.T) {
	gtest.RunSubTests(t, &ConfigTests{})
}
