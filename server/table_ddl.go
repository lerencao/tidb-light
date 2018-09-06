package server

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

type TableDDL struct {
	db *sql.DB
}

func NewTableDDL(dataSourceName string) (*TableDDL , error) {
	db, err:= sql.Open("mysql", dataSourceName)
	if err != nil {
		return nil, err
	}

	return &TableDDL{
		db:db,
	}, nil
}

func (t *TableDDL) Show(schemaName, tableName string) (string, error) {
	row := t.db.QueryRow(fmt.Sprintf("show create table %s.%s", schemaName, tableName))
	var tbn, tableDDL string
	err := row.Scan(&tbn, &tableDDL)
	if err != nil {
		return "", err
	}
	return tableDDL, nil
}

func (t *TableDDL) Close() error {
	return t.db.Close()
}
