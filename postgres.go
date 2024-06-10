package main

import (
	"github.com/jmoiron/sqlx"
)

// PostgresCleanDB resets sequences and wipes all tables in the provided postgres database.
func PostgresCleanDB(db *sqlx.DB) error {
	var sequences []string
	err := db.Select(&sequences,
		`SELECT sequence_name
		FROM information_schema.sequences
		WHERE sequence_schema NOT IN ('information_schema, pg_catalog')`)
	if err != nil {
		return err
	}

	for _, s := range sequences {
		if _, err := db.Exec("ALTER SEQUENCE " + s + " RESTART WITH 1"); err != nil {
			return err
		}
	}

	var tables []struct {
		Name   string `db:"table_name"`
		Schema string `db:"table_schema"`
	}
	err = db.Select(&tables,
		`SELECT table_name, table_schema
		FROM information_schema.tables
		WHERE table_schema NOT IN ('information_schema', 'pg_catalog') AND table_type = 'BASE TABLE'`)
	if err != nil {
		return err
	}

	for _, table := range tables {
		if _, err := db.Exec("DELETE FROM " + table.Schema + "." + table.Name + " CASCADE"); err != nil {
			return err
		}
	}
	return nil
}
