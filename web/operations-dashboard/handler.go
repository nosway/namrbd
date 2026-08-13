package opsdashboard

import (
	"embed"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strings"
)

const (
	Route              = "/console/"
	PrimarySnapshotAPI = "/api/v1/sbs/cluster"
)

//go:embed static/*
var embeddedStatic embed.FS

func Handler() http.Handler {
	staticFS, err := fs.Sub(embeddedStatic, "static")
	if err != nil {
		panic(err)
	}
	return &dashboardHandler{staticFS: staticFS}
}

type dashboardHandler struct {
	staticFS fs.FS
}

func (h *dashboardHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	assetPath := strings.TrimPrefix(r.URL.Path, "/console")
	assetPath = strings.TrimPrefix(assetPath, "/")
	if assetPath == "" {
		assetPath = "index.html"
	} else {
		assetPath = strings.TrimPrefix(path.Clean("/"+assetPath), "/")
	}

	if info, err := fs.Stat(h.staticFS, assetPath); err != nil || info.IsDir() {
		assetPath = "index.html"
	}

	if assetPath == "index.html" || strings.HasPrefix(assetPath, "fixtures/") {
		w.Header().Set("Cache-Control", "no-store")
	} else {
		w.Header().Set("Cache-Control", "public, max-age=300")
	}
	w.Header().Set("X-NAMRBD-Dashboard", "read-only")
	if contentType := mime.TypeByExtension(path.Ext(assetPath)); contentType != "" {
		w.Header().Set("Content-Type", contentType)
	}
	data, err := fs.ReadFile(h.staticFS, assetPath)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(data)
	}
}
