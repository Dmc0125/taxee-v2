package main

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"path"
	"runtime"
	"strings"
	"taxee/pkg/assert"
	"taxee/pkg/coingecko"
	"taxee/pkg/db"
	"taxee/pkg/dotenv"
	"time"
)

func sendSSEError(
	w http.ResponseWriter,
	flusher http.Flusher,
	msg string,
) {
	fmt.Fprintf(w, "event: error\ndata: %s\n\n", msg)
	flusher.Flush()
}

func sendSSEUpdate(
	w http.ResponseWriter,
	flusher http.Flusher,
	data string,
) {
	fmt.Fprintf(w, "event: update\ndata: %s\n\n", data)
	flusher.Flush()
}

func sendSSEClose(
	w http.ResponseWriter,
	flusher http.Flusher,
) {
	fmt.Fprintf(w, "event: close\ndata:\n\n")
	flusher.Flush()
}

func initSSEHandler(w http.ResponseWriter) (flusher http.Flusher, ok bool) {
	headers := w.Header()
	headers.Set("content-type", "text/event-stream")
	headers.Set("cache-control", "no-cache")
	headers.Set("connection", "keep-alive")

	flusher, ok = w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", 500)
	}
	return
}

type pageLayoutComponentData struct {
	Content template.HTML
}

func loadTemplates(templates *template.Template, dirPath string) {
	entries, err := os.ReadDir(dirPath)
	assert.NoErr(err, "unable to read html dir")

	for _, entry := range entries {
		if !entry.IsDir() {
			fileName := path.Join(dirPath, entry.Name())
			fileData, err := os.ReadFile(fileName)
			assert.NoErr(err, fmt.Sprintf("unable to read html for: %s", fileName))

			templates, err = templates.Parse(string(fileData))
			assert.NoErr(err, fmt.Sprintf("unable to parse html for: %s", fileName))
		}
	}
}

type bufWriter []byte

func (b *bufWriter) Write(n []byte) (int, error) {
	*b = append(*b, n...)
	return len(n), nil
}

func executeTemplateMust(tmpl *template.Template, name string, data any) []byte {
	buf := bufWriter([]byte{})
	err := tmpl.ExecuteTemplate(&buf, name, data)
	assert.NoErr(err, "unable to execute template")
	return buf
}

func serveStaticFiles(prod bool) {
	staticDir := ""

	switch {
	case prod:
		staticPath := os.Getenv("STATIC_PATH")
		assert.True(staticPath != "", "missing path to static files")
	default:
		_, file, _, ok := runtime.Caller(0)
		assert.True(ok, "unable to get runtime caller")
		staticDir = path.Join(file, "../../static")
	}

	handler := http.FileServer(http.Dir(staticDir))
	http.Handle("/static/", http.StripPrefix("/static/", handler))
}

func renderError(r http.ResponseWriter, statusCode int, msg string, args ...any) {
	r.WriteHeader(statusCode)
	r.Write(fmt.Appendf(nil, msg, args...))
}

func shorten(s string, start, end int) string {
	return fmt.Sprintf("%s...%s", s[:start], s[len(s)-end:])
}

func toTitle(s string) string {
	if len(s) == 0 {
		return s
	}
	const toLower = 32
	t := strings.Builder{}

	isLower := func(c byte) bool {
		return (c >= 97 && c <= 122)
	}

	if isLower(s[0]) {
		t.WriteByte(s[0] - toLower)
	} else {
		t.WriteByte(s[0])
	}

	isAlpha := func(c byte) bool {
		return isLower(c) || 65 <= c && c <= 90
	}

	for i := 1; i < len(s); i += 1 {
		prev := s[i-1]
		c := s[i]

		switch {
		case c == 95:
			t.WriteByte(32)
		case !isAlpha(prev) && isLower(c):
			t.WriteByte(c - toLower)
		default:
			t.WriteByte(c)
		}
	}

	return t.String()
}

// usage:
// {{ tern <condition> <value if true> <value if false> }}
//
// can be nested
// {{ tern <condition> <value if true> <condition> <value if true> <value if false> }}
func tern(pairs ...any) any {
	assert.True(len(pairs) >= 3, "invalid ternary")
	cond, ok := pairs[0].(bool)
	assert.True(ok, "expected bool")
	if cond {
		return pairs[1]
	}

	falseVal := pairs[2]
	if _, ok := falseVal.(bool); ok {
		return tern(pairs[2:]...)
	}

	return falseVal
}

func main() {
	appEnv := os.Getenv("APP_ENV")
	prod := true
	if appEnv != "PROD" {
		assert.NoErr(dotenv.ReadEnv(), "")
		prod = false
	}

	htmlPath := os.Getenv("HTML")
	assert.True(len(htmlPath) > 0, "missing html path")

	coingecko.Init()

	templateFuncs := template.FuncMap{
		"formatDate": func(t time.Time) string {
			return t.Format("02/01/2006")
		},
		"formatTime": func(t time.Time) string {
			return t.Format("15:04:05")
		},
		"upper": func(s string) string {
			return strings.ToUpper(s)
		},
		"shorten": shorten,
		"toTitle": toTitle,
		"tern":    tern,
	}
	var templates *template.Template = template.New("root").Funcs(templateFuncs)
	loadTemplates(templates, htmlPath)
	pool, err := db.InitPool(context.Background(), appEnv)
	assert.NoErr(err, "")

	serveStaticFiles(prod)

	http.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/static/favicon.ico", http.StatusMovedPermanently)
	})
	http.HandleFunc(
		"/events",
		eventsHandler(context.Background(), pool, templates),
	)
	http.HandleFunc(
		"/sync_request",
		syncRequestHandler(pool, templates),
	)
	http.HandleFunc(
		"/tokens",
		tokensHandler(pool, templates),
	)

	fmt.Println("Listening at: http://localhost:8888")
	err = http.ListenAndServe("localhost:8888", nil)
	assert.NoErr(err, "")
}
