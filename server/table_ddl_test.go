package server_test

import (
	"github.com/lerencao/tidb-light/server"
	"strings"
	"testing"
)

func TestTableDDL_Show(t *testing.T) {
	// TODO: remove the pass
	db, err := server.OpenDB("172.16.48.192", "root", "root")
	if err != nil {
		t.Fatal(err)
	}
	show, err := server.TableDDL(db, "raw", "device_fingerprint")
	if err != nil {
		t.Fatal(err)
	}

	if !strings.HasPrefix(show, "CREATE TABLE") {
		t.Fatalf("show table should start with CREATE TABLE, table: %s", show)
	}
}
