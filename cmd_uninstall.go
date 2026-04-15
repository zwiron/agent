package main

import (
	"fmt"

	"github.com/kardianos/service"
)

func cmdUninstall() error {
	prg := &program{}
	s, err := service.New(prg, svcConfig())
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}

	_ = s.Stop()
	fmt.Println("✓ Service stopped")

	if err := s.Uninstall(); err != nil {
		return fmt.Errorf("uninstall service: %w", err)
	}
	fmt.Println("✓ Service removed")

	if err := removeConfig(); err != nil {
		return fmt.Errorf("remove config: %w", err)
	}
	fmt.Println("✓ Config removed")

	fmt.Println("\nAgent uninstalled. Data directory preserved at", serviceDataDir())
	return nil
}
