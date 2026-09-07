package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadSignedReportDirectoryRejectsUnexpectedFilesAndSymlinks(t *testing.T) {
	t.Run("unexpected extension", func(t *testing.T) {
		directory := t.TempDir()
		if err := os.WriteFile(filepath.Join(directory, "README.txt"), []byte("not a report\n"), 0o640); err != nil {
			t.Fatal(err)
		}
		if _, err := loadSignedReportDirectory(directory); err == nil || !strings.Contains(err.Error(), "unexpected entry") {
			t.Fatalf("unexpected entry error=%v", err)
		}
	})
	t.Run("symlink", func(t *testing.T) {
		directory := t.TempDir()
		target := filepath.Join(t.TempDir(), "outside.json")
		if err := os.WriteFile(target, []byte("{}\n"), 0o640); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(directory, "report.json")); err != nil {
			t.Fatal(err)
		}
		if _, err := loadSignedReportDirectory(directory); err == nil || !strings.Contains(err.Error(), "regular file without symlinks") {
			t.Fatalf("symlink error=%v", err)
		}
	})
}
