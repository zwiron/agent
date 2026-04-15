package main

import (
	"fmt"
	"os"

	"github.com/kardianos/service"

	_ "github.com/zwiron/connector/sql/mysql"
	_ "github.com/zwiron/connector/sql/postgres"
)

var version = "develop"

func main() {
	// If running as a system service (not interactive), run directly.
	if !service.Interactive() {
		prg := &program{}
		s, err := service.New(prg, svcConfig())
		if err != nil {
			fmt.Fprintf(os.Stderr, "service init: %v\n", err)
			os.Exit(1)
		}
		if err := s.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "service run: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// Interactive mode — parse subcommand.
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	var err error
	switch os.Args[1] {
	case "install":
		err = cmdInstall(os.Args[2:])
	case "uninstall":
		err = cmdUninstall()
	case "start":
		err = cmdStart()
	case "stop":
		err = cmdStop()
	case "restart":
		err = cmdRestart()
	case "status":
		err = cmdStatus()
	case "logs":
		err = cmdLogs()
	case "run":
		// Strip "run" from args so pkg/config doesn't see it.
		os.Args = append(os.Args[:1], os.Args[2:]...)
		err = cmdRun()
	case "version":
		fmt.Println(version)
	case "--help", "-h", "help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
