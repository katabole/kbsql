package kbsql

import (
	"fmt"
	"net/url"
	"strings"

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
		return fmt.Errorf("error selecting sequences from information schema: %w", err)
	}

	for _, s := range sequences {
		if _, err := db.Exec("ALTER SEQUENCE " + s + " RESTART WITH 1"); err != nil {
			return fmt.Errorf("error restarting sequence %s: %w", s, err)
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
		return fmt.Errorf("error selecting tables from information schema: %w", err)
	}

	// Though a little hackish, this seems to be the easiest way to clear all the tables of a DB which has foreign key
	// relationships. We just try clearing them all repeatedly up to a sane limit of attempts.
	deletesAttempted := 0
	for {
		var lastError error
		for _, table := range tables {
			if _, err := db.Exec("DELETE FROM " + table.Schema + "." + table.Name + " CASCADE"); err != nil {
				lastError = err
				if !strings.Contains(err.Error(), "foreign key constraint") {
					return fmt.Errorf("error deleting from table %s.%s: %w", table.Schema, table.Name, err)
				}
			}
		}

		deletesAttempted++
		// If you need to increase this, make a PR and I'll give you the "ridiculous-est number of foreign keys" award.
		if deletesAttempted > 100 {
			return fmt.Errorf("error deleting from tables: too many attempts, last error: %w", lastError)
		}

		if lastError == nil {
			break
		}
	}
	return nil
}

// PostgresCreateDBIfNotExistsByURL ensures the given database exists, creating it if it doesn't. Generally for dev/test
// databases. It takes a URL connection string like `postgres://joe:secret@kb.example.com:5432/testdb`
func PostgresCreateDBIfNotExistsByURL(dbURL string) error {
	// First parse it and remove the database name, if we leave it there and the DB doesn't exist yet then connecting to
	// the database will fail
	parsed, err := url.Parse(dbURL)
	if err != nil {
		return fmt.Errorf("error parsing dbURL: %w", err)
	}

	dbName := strings.TrimPrefix(parsed.Path, "/")
	if dbName == "" {
		return fmt.Errorf("no database name found in dbURL: %s", dbURL)
	}

	connURL := parsed
	connURL.Path = ""

	db, err := sqlx.Connect("pgx", connURL.String())
	if err != nil {
		return fmt.Errorf("error connecting: %w", err)
	}

	var results []int
	if err := db.Select(&results, `SELECT 1 FROM pg_database WHERE datname = $1`, dbName); err != nil {
		return fmt.Errorf("error selecting tables from information schema: %w", err)
	}
	if len(results) == 0 {
		_, err := db.Exec("CREATE DATABASE " + dbName)
		if err != nil {
			return fmt.Errorf("error creating database %s: %w", dbName, err)
		}
	}
	return nil
}
