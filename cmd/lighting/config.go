package main

import (
	"flag"
	"fmt"
	"github.com/juju/errors"
)

func newConfig() *config {
	cfg := &config{}
	cfg.FlagSet = flag.NewFlagSet("light", flag.ContinueOnError)

	cfg.FlagSet.StringVar(&cfg.Addr, "addr", "localhost:20280", "listening addr")
	cfg.FlagSet.StringVar(&cfg.ImporterAddr, "importer-addr", "", "importer listen address")
	cfg.FlagSet.StringVar(&cfg.SessionId, "session-id", "", "session id")
	cfg.FlagSet.StringVar(&cfg.Key, "key", "", "key")
	cfg.FlagSet.StringVar(&cfg.Value, "value", "", "value")
	cfg.FlagSet.Uint64Var(&cfg.KeyNum, "key-num", 100000, "numbers of keys to insert")
	cfg.FlagSet.UintVar(&cfg.BatchSize, "batch-size", 1000, "batch size of on write")

	cfg.FlagSet.StringVar(&cfg.configFile, "config", "", "toml config file path")
	// cfg.FlagSet.StringVar(&cfg.StoreCfg.Path, "store", "", "pd path")
	return cfg
}

type config struct {
	*flag.FlagSet `json:"-"`
	Addr          string `toml:"addr" json:"addr"`
	// StoreCfg      storeConfig `toml:"store-cfg" json:"store_cfg"`
	ImporterAddr string `toml:"importer-addr" json:"importer_addr"`
	SessionId    string `toml:"session-id" json:"session_id"`
	Key          string `toml:"key" json:"key"`
	Value        string `toml:"value" json:"value"`
	KeyNum       uint64 `toml:"key-num" json:"key_num"`
	BatchSize    uint   `toml:"batch-size" json:"batch_size"`
	configFile   string
}

func (c *config) String() string {
	if c == nil {
		return "<nil>"
	}

	return fmt.Sprintf("Config(%v)", *c)
}

func (c *config) Parse(args []string) error {
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
