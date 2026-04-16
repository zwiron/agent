package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	githubRepo   = "zwiron/agent"
	githubAPIURL = "https://api.github.com/repos/" + githubRepo + "/releases/latest"
)

type ghRelease struct {
	TagName string `json:"tag_name"`
}

func cmdUpdate() error {
	fmt.Println("Checking for updates...")

	// 1. Get latest version from GitHub.
	latest, err := fetchLatestVersion()
	if err != nil {
		return fmt.Errorf("check latest version: %w", err)
	}

	current := version
	if !strings.HasPrefix(current, "v") {
		current = "v" + current
	}

	if latest == current {
		fmt.Printf("Already up to date (%s)\n", current)
		return nil
	}

	fmt.Printf("Update available: %s → %s\n", current, latest)

	// 2. Download the new binary.
	goos := runtime.GOOS
	goarch := runtime.GOARCH
	tarball := fmt.Sprintf("zwiron-agent-%s-%s-%s.tar.gz", latest, goos, goarch)
	url := fmt.Sprintf("https://github.com/%s/releases/download/%s/%s", githubRepo, latest, tarball)

	fmt.Printf("Downloading %s...\n", tarball)

	tmpDir, err := os.MkdirTemp("", "zwiron-update-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	tarPath := filepath.Join(tmpDir, tarball)
	if err := downloadFile(url, tarPath); err != nil {
		return fmt.Errorf("download: %w", err)
	}

	// 3. Extract the binary.
	newBin := filepath.Join(tmpDir, "zwiron-agent")
	if err := extractTarGz(tarPath, newBin); err != nil {
		return fmt.Errorf("extract: %w", err)
	}

	// 4. Replace the current binary.
	selfPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("find executable path: %w", err)
	}
	selfPath, err = filepath.EvalSymlinks(selfPath)
	if err != nil {
		return fmt.Errorf("resolve symlinks: %w", err)
	}

	// Atomic-ish replace: rename old, copy new, remove old.
	backupPath := selfPath + ".bak"
	if err := os.Rename(selfPath, backupPath); err != nil {
		return fmt.Errorf("backup current binary: %w (try running with sudo)", err)
	}

	if err := copyFile(newBin, selfPath, 0755); err != nil {
		// Restore backup on failure.
		_ = os.Rename(backupPath, selfPath)
		return fmt.Errorf("install new binary: %w", err)
	}

	_ = os.Remove(backupPath)

	fmt.Printf("\n  ✓ Updated to %s\n", latest)
	fmt.Println("  Restart the agent:  sudo zwiron-agent restart")
	fmt.Println()
	return nil
}

func fetchLatestVersion() (string, error) {
	client := &http.Client{Timeout: 10 * time.Second}

	req, err := http.NewRequest("GET", githubAPIURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GitHub API returned %d", resp.StatusCode)
	}

	var rel ghRelease
	if err := json.NewDecoder(resp.Body).Decode(&rel); err != nil {
		return "", err
	}

	if rel.TagName == "" {
		return "", fmt.Errorf("no tag_name in response")
	}

	return rel.TagName, nil
}

func downloadFile(url, dest string) error {
	client := &http.Client{Timeout: 120 * time.Second}

	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}

	f, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, resp.Body)
	return err
}

func extractTarGz(tarPath, destBinary string) error {
	f, err := os.Open(tarPath)
	if err != nil {
		return err
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return fmt.Errorf("zwiron-agent binary not found in archive")
		}
		if err != nil {
			return err
		}

		if filepath.Base(hdr.Name) == "zwiron-agent" && hdr.Typeflag == tar.TypeReg {
			out, err := os.OpenFile(destBinary, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0755)
			if err != nil {
				return err
			}
			defer out.Close()

			// Limit extraction size to 500MB to prevent zip bombs.
			_, err = io.Copy(out, io.LimitReader(tr, 500*1024*1024))
			return err
		}
	}
}

func copyFile(src, dst string, perm os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}
