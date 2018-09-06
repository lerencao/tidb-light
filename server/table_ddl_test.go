package server

import (
	"strings"
	"testing"
)

func TestTableDDL_Show(t *testing.T) {
	// TODO: remove the pass
	ddl, err := NewTableDDL("root:root@tcp(172.16.48.192:4000)/")
	if err != nil {
		t.Fatal(err)
	}

	show, err := ddl.Show("raw", "device_fingerprint")
	if err != nil {
		t.Fatal(err)
	}

	if !strings.HasPrefix(show, "CREATE TABLE") {
		t.Fatalf("show table should start with CREATE TABLE, table: %s", show)
	}
}
