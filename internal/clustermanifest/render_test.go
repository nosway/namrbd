package clustermanifest

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderIsByteIdenticalAndContainsExpectedBundles(t *testing.T) {
	first, err := Render(exactManifestForTest(), testPolicy())
	if err != nil {
		t.Fatalf("Render first: %v", err)
	}
	second, err := Render(exactManifestForTest(), testPolicy())
	if err != nil {
		t.Fatalf("Render second: %v", err)
	}
	if first.ManifestDigest != second.ManifestDigest || first.RenderDigest != second.RenderDigest {
		t.Fatalf("rerender digests differ: first=%+v second=%+v", first, second)
	}
	if len(first.Bundles) != ExactNodeCount {
		t.Fatalf("bundle count=%d want=%d", len(first.Bundles), ExactNodeCount)
	}
	for i := range first.Bundles {
		a, b := first.Bundles[i], second.Bundles[i]
		if a.NodeID != b.NodeID || a.BundleDigest != b.BundleDigest || len(a.Files) != len(b.Files) {
			t.Fatalf("bundle %d differs: first=%+v second=%+v", i, a, b)
		}
		for j := range a.Files {
			if a.Files[j].RelativePath != b.Files[j].RelativePath || a.Files[j].Mode != b.Files[j].Mode || !bytes.Equal(a.Files[j].Content, b.Files[j].Content) {
				t.Fatalf("bundle %s file %d is not byte-identical", a.NodeID, j)
			}
		}
		wantFileCount := 5
		if containsStringValue(a.Roles, "sbs-service-active") || containsStringValue(a.Roles, "sbs-service-standby") {
			wantFileCount = 7
		}
		if len(a.Files) != wantFileCount {
			t.Fatalf("bundle %s files=%d want=%d", a.NodeID, len(a.Files), wantFileCount)
		}
	}
	dataUnit1 := renderedContent(t, first.Bundles[0], "namrbd-sbs-data.service")
	dataUnit2 := renderedContent(t, first.Bundles[1], "namrbd-sbs-data.service")
	if !bytes.Equal(dataUnit1, dataUnit2) {
		t.Fatalf("common data systemd unit changed across nodes")
	}
}

func TestWriteRenderSetRefusesNonEmptyOutputAndPreservesModes(t *testing.T) {
	set, err := Render(exactManifestForTest(), testPolicy())
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	root := t.TempDir()
	output := filepath.Join(root, "rendered")
	if err := WriteRenderSet(output, set); err != nil {
		t.Fatalf("WriteRenderSet: %v", err)
	}
	dataConfig := filepath.Join(output, "nodes", "node1", "etc", "namrbd", "sbs-data.yaml")
	info, err := os.Stat(dataConfig)
	if err != nil {
		t.Fatalf("Stat rendered config: %v", err)
	}
	if info.Mode().Perm() != os.FileMode(DefaultRenderedConfigMode) {
		t.Fatalf("rendered mode=%#o want=%#o", info.Mode().Perm(), DefaultRenderedConfigMode)
	}
	if err := WriteRenderSet(output, set); err == nil || !strings.Contains(err.Error(), "is not empty") {
		t.Fatalf("second WriteRenderSet error=%v, want non-empty refusal", err)
	}
}

func renderedContent(t *testing.T, bundle NodeBundle, suffix string) []byte {
	t.Helper()
	for _, file := range bundle.Files {
		if strings.HasSuffix(file.RelativePath, suffix) {
			return file.Content
		}
	}
	t.Fatalf("bundle %s missing %s", bundle.NodeID, suffix)
	return nil
}
