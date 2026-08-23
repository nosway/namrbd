package mcpops

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPublicMCPDescriptorsUseProductNames(t *testing.T) {
	internalTokens := []string{"phase" + " y", "phase" + "_", "phase" + "-"}
	foundEvidenceTool := false
	for _, tool := range Tools() {
		text := strings.ToLower(tool.Name + " " + tool.Description)
		for _, token := range internalTokens {
			if strings.Contains(text, token) {
				t.Errorf("tool exposes an internal planning name: %s", tool.Name)
			}
		}
		if tool.Name == "namrbd.evidence.latest" {
			foundEvidenceTool = true
		}
	}
	if !foundEvidenceTool {
		t.Fatal("product-facing evidence tool is not registered")
	}
	for _, resource := range Resources() {
		text := strings.ToLower(resource.URI + " " + resource.Description)
		for _, token := range internalTokens {
			if strings.Contains(text, token) {
				t.Errorf("resource exposes an internal planning name: %s", resource.URI)
			}
		}
	}
}

func TestRunbookIndexPointsToPublicDocuments(t *testing.T) {
	root := mcpRepoRoot(t)
	for _, runbook := range RunbookIndex() {
		doc, ok := runbook["doc"].(string)
		if !ok || doc == "" {
			t.Fatalf("runbook has no document path: %+v", runbook)
		}
		lower := strings.ToLower(doc)
		if !strings.HasPrefix(doc, "docs-src/") || strings.Contains(lower, "phase-") {
			t.Errorf("runbook points outside the public documentation source: %s", doc)
			continue
		}
		if info, err := os.Stat(filepath.Join(root, doc)); err != nil || info.IsDir() {
			t.Errorf("runbook document is not a public file: %s (%v)", doc, err)
		}
	}
}

func mcpRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find repository root")
		}
		dir = parent
	}
}
