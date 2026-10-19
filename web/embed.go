package web

import "embed"

// FS embeds the static dashboard assets.
//
//go:embed index.html style.css app.js
var FS embed.FS
