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
	"taxee/pkg/db"
	"taxee/pkg/dotenv"
	"time"
)

type pageLayoutComponentData struct {
	Content template.HTML
}

var pageLayoutComponent = `
{{define "page_layout"}}
	<!DOCTYPE html>
	<html>
		<head>
			<title>Taxee</title>
			<link href="static/output.css" rel="stylesheet">
		</head>
		<body class="min-h-screen bg-gray-50">
			{{ .Content }}
		</body>
	</html>
{{end}}
`

func loadTemplate(templates *template.Template, tmpl string) {
	var err error
	templates, err = templates.Parse(tmpl)
	assert.NoErr(err, "unable to load template")
}

type bufWriter []byte

func (b *bufWriter) Write(n []byte) (int, error) {
	*b = append(*b, n...)
	return len(n), nil
}

func executeTemplate(tmpl *template.Template, name string, data any) ([]byte, error) {
	buf := bufWriter([]byte{})
	err := tmpl.ExecuteTemplate(&buf, name, data)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func executeTemplateMust(tmpl *template.Template, name string, data any) []byte {
	buf, err := executeTemplate(tmpl, name, data)
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
		staticDir = path.Join(file, "../static")
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

func main() {
	appEnv := os.Getenv("APP_ENV")
	prod := true
	if appEnv != "PROD" {
		assert.NoErr(dotenv.ReadEnv(), "")
		prod = false
	}

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
		"toTitle": func(s string) string {
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
		},
	}
	var templates *template.Template = template.New("root").Funcs(templateFuncs)
	loadTemplate(templates, pageLayoutComponent)
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
		"/transactions",
		transactionsHandler(context.Background(), pool, templates),
	)

	fmt.Println("Listening at: http://localhost:8888")
	err = http.ListenAndServe("localhost:8888", nil)
	assert.NoErr(err, "")
}
