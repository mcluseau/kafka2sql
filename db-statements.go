package main

import (
	"bytes"
	"fmt"
)

func insertStatement(table string, columns []string) string {
	query := bytes.NewBufferString("INSERT INTO ")
	query.WriteString(table)
	query.WriteByte('(')
	for i, col := range columns {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString(col)
	}
	query.WriteString(") VALUES (")
	for i := range columns {
		if i > 0 {
			query.WriteString(", ")
		}
		fmt.Fprintf(query, "$%d", i+1)
	}
	query.WriteByte(')')
	return query.String()
}

func updateStatement(table string, columns, keyColumns []string) string {
	query := bytes.NewBufferString("UPDATE ")
	query.WriteString(table)
	query.WriteString(" SET ")
	for i, col := range columns {
		if i > 0 {
			query.WriteString(", ")
		}
		query.WriteString(col)
		query.WriteString("=?")
	}
	query.WriteString(" WHERE ")
	for i, col := range keyColumns {
		if i > 0 {
			query.WriteString(" AND ")
		}
		query.WriteString(col)
		query.WriteString("=?")
	}
	return query.String()
}
