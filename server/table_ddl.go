package server

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

func TableDDL(db *sql.DB, schemaName, tableName string) (string, error) {
	row := db.QueryRow(fmt.Sprintf("show create table %s.%s", schemaName, tableName))
	var tbn, tableDDL string
	err := row.Scan(&tbn, &tableDDL)
	if err != nil {
		return "", err
	}
	return tableDDL, nil

}
