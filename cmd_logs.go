package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

func cmdLogs() error {
	switch runtime.GOOS {
	case "linux":
		cmd := exec.Command("journalctl", "-u", serviceName, "-f", "--no-pager", "-n", "100")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()

	case "darwin":
		logFile := filepath.Join(svcLogDir(), serviceName+".err.log")
		if _, err := os.Stat(logFile); err == nil {
			cmd := exec.Command("tail", "-f", "-n", "100", logFile)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			return cmd.Run()
		}
		// Fallback to log stream.
		cmd := exec.Command("log", "stream", "--predicate", fmt.Sprintf(`process == "%s"`, serviceName), "--style", "compact")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		return cmd.Run()

	case "windows":
		fmt.Println("On Windows, view logs in Event Viewer under Application log, source:", serviceName)
		return nil

	default:
		return fmt.Errorf("unsupported platform: %s", runtime.GOOS)
	}
}
