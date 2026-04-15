package main

import (
	"fmt"

	"github.com/kardianos/service"
)

func cmdStart() error {
	prg := &program{}
	s, err := service.New(prg, svcConfig())
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}
	if err := s.Start(); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	fmt.Println("✓ Service started")
	return nil
}

func cmdStop() error {
	prg := &program{}
	s, err := service.New(prg, svcConfig())
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}
	if err := s.Stop(); err != nil {
		return fmt.Errorf("stop: %w", err)
	}
	fmt.Println("✓ Service stopped")
	return nil
}

func cmdRestart() error {
	prg := &program{}
	s, err := service.New(prg, svcConfig())
	if err != nil {
		return fmt.Errorf("create service: %w", err)
	}
	if err := s.Restart(); err != nil {
		return fmt.Errorf("restart: %w", err)
	}
	fmt.Println("✓ Service restarted")
	return nil
}
