package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func cmdResetCerts() error {
	dataDir := serviceDataDir()

	removed := 0
	for _, name := range []string{"atlas-ca.pem", "client.pem"} {
		p := filepath.Join(dataDir, name)
		if err := os.Remove(p); err == nil {
			fmt.Printf("✓ Removed %s\n", p)
			removed++
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("remove %s: %w", p, err)
		}
	}

	if removed == 0 {
		fmt.Println("No pinned certificates found — nothing to clear.")
		return nil
	}

	fmt.Println("\nCertificates cleared. Restart the agent to re-pin:")
	fmt.Println("  sudo zwiron-agent restart")
	return nil
}
