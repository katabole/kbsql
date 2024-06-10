package kbsql

import (
	"fmt"
	"os/exec"
)

// AtlasSetupDB runs atlas to ensure the database (usually dev/test) is up to date.
func AtlasSetupDB(dbURL string, atlasDevDBURL string) error {
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
