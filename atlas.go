package kbsql

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/jmoiron/sqlx"
)

// AtlasSetupDB runs atlas to ensure the database (usually dev/test) is up to date.
func AtlasSetupDB(dbURL string, atlasDevDBURL string) error {
	if err := ensureAtlasDevExists(atlasDevDBURL); err != nil {
		return err
	}

	cmd := exec.Command("atlas", "schema", "apply",
		"--to", "file://schema.sql",
		"--url", dbURL,
		"--dev-url", atlasDevDBURL,
		"--auto-approve")
	cmd.Dir = ".."
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("Error running atlas schema apply: %v\n\nOutput: %s\n", err, output)
	}
	return nil
}

// ensureAtlasDevExists creates atlas_dev if needed, required to run atlas commands.
// `task setup` creates this but that doesn't run for CI
func ensureAtlasDevExists(atlasDevDBURL string) error {
	db, err := sqlx.Connect("pgx", strings.ReplaceAll(atlasDevDBURL, "atlas_dev", ""))
	if err != nil {
		return fmt.Errorf("failed to connect to atlas dev db: %w", err)
	}
	defer db.Close()

	var results []int
	err = db.Select(&results, `SELECT 1 FROM pg_database WHERE datname = 'atlas_dev'`)
	if err != nil {
		return fmt.Errorf("error selecting tables from information schema: %w", err)
	}
	if len(results) == 0 {
		_, err := db.Exec("CREATE DATABASE atlas_dev")
		if err != nil {
			return fmt.Errorf("error creating atlas_dev database: %w", err)
		}
	}
	return nil
}
