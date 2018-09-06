package config

import (
	"flag"
	"fmt"
	"github.com/juju/errors"
)

func NewConfig() *Config {
	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("light", flag.ContinueOnError)

	cfg.FlagSet.StringVar(&cfg.Addr, "addr", "localhost:20280", "listening addr")
	cfg.FlagSet.StringVar(&cfg.ImporterAddr, "importer-addr", "", "importer listen address")
	cfg.FlagSet.StringVar(&cfg.TiDBAddr, "tidb-addr", "", "tidb tcp addr")
	cfg.FlagSet.StringVar(&cfg.TiDBUser, "tidb-user", "root", "tidb user")
	cfg.FlagSet.StringVar(&cfg.TiDBPass, "tidb-password", "root", "tidb password")
	cfg.FlagSet.StringVar(&cfg.TiDBHttpAddr, "tidb-http-addr", "", "tidb http addr")
	cfg.FlagSet.StringVar(&cfg.configFile, "config", "", "toml config file path")
	// cfg.FlagSet.StringVar(&cfg.StoreCfg.Path, "store", "", "pd path")
	return cfg
}

type Config struct {
	*flag.FlagSet `json:"-"`
	Addr          string `toml:"addr" json:"addr"`
	// StoreCfg      storeConfig `toml:"store-cfg" json:"store_cfg"`
	ImporterAddr string `toml:"importer-addr" json:"importer_addr"`
	TiDBAddr     string `toml:"tidb-addr" json:"tidb_addr"`
	TiDBUser     string `toml:"tidb-user" json:"tidb_user"`
	TiDBPass     string `toml:"tidb-pass" json:"tidb_pass"`
	TiDBHttpAddr string `toml:"tidb-http-addr" json:"tidb_http_addr"`
	configFile   string
}

func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Config(%v)", *c)
}

func (c *Config) Parse(args []string) error {
	err := c.FlagSet.Parse(args)
	if err != nil {
		return errors.Trace(err)
	}

	// Parse agin to replace with command line options
	err = c.FlagSet.Parse(args)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	return nil
}

// type storeConfig struct {
// 	Path string `toml:"path" json:"path"`
// }
