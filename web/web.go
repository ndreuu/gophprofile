package web

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static/*
var files embed.FS

func Index(w http.ResponseWriter, _ *http.Request) {
	data, err := files.ReadFile("static/index.html")
	if err != nil {
		http.Error(w, "Web interface unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(data)
}

func Assets() http.Handler {
	assets, err := fs.Sub(files, "static")
	if err != nil {
		panic(err)
	}
	return http.FileServer(http.FS(assets))
}
