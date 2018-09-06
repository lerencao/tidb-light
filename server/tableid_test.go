package server

import "testing"

func TestTableId(t *testing.T) {
	endpoint := "http://172.16.48.192:10800"
	tableId, err := TableId(endpoint, "raw", "device_fingerprint")
	if err != nil {

		t.Fatal(err)
	}

	if tableId <= 0 {
		t.Fatalf("table id should be > 0, which is %d", tableId)
	}

	if tableId != 103 {
		t.Fatalf("table id should be 103")
	}

	tableId, err = TableId(endpoint, "raw", "no_exists_table")
	if err == nil || tableId != 0 {
		t.Fatalf("no_exist_table should return err and tableId should be 0")
	}
}
