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

	"github.com/jackc/pgx/v5"
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

type uiIndicatorStatus string

const (
	uiIndicatorStatusSuccess       uiIndicatorStatus = "success"
	uiIndicatorStatusError         uiIndicatorStatus = "error"
	uiIndicatorStatusUninitialized uiIndicatorStatus = "uninitialized"
	uiIndicatorStatusInProgress    uiIndicatorStatus = "in_progress"
)

type uiStatusIndicatorData struct {
	SseSubscribe   bool
	Status         uiIndicatorStatus
	InProgressJobs []string
}

func (s *uiStatusIndicatorData) eq(other *uiStatusIndicatorData) bool {
	if s.Status == uiIndicatorStatusInProgress && s.Status == other.Status {
		for i, j := range s.InProgressJobs {
			if i > len(other.InProgressJobs)-1 {
				return false
			}

			if j != other.InProgressJobs[i] {
				return false
			}
		}
	}

	return s.Status == other.Status
}

func newUiStatusIndicatorData(jobs []*uiJob) *uiStatusIndicatorData {
	if len(jobs) == 0 {
		return &uiStatusIndicatorData{
			Status: uiIndicatorStatusUninitialized,
		}
	}

	status := uiIndicatorStatusSuccess
	var inProgressJobs []string

	for _, j := range jobs {
		switch j.t {
		case db.JobFetchWallet:
			switch j.status {
			case db.StatusQueued, db.StatusInProgress:
				inProgressJobs = append(inProgressJobs, j.label)
			}
		case db.JobParseTransactions, db.JobParseEvents:
			switch j.status {
			case db.StatusError:
				return &uiStatusIndicatorData{
					Status: uiIndicatorStatusError,
				}
			case db.StatusInProgress, db.StatusQueued, db.StatusResetScheduled:
				inProgressJobs = append(inProgressJobs, j.label)
			}
		}
	}

	subscribe := false

	if len(inProgressJobs) > 0 {
		subscribe = true
		status = uiIndicatorStatusInProgress
	}

	return &uiStatusIndicatorData{
		SseSubscribe:   subscribe,
		Status:         status,
		InProgressJobs: inProgressJobs,
	}
}

type uiJob struct {
	status db.Status
	label  string
	t      db.JobType
}

type uiJobs []*uiJob

func (jobs *uiJobs) appendFromRows(rows pgx.Rows) error {
	var jobType db.JobType
	var jobStatus db.Status
	if err := rows.Scan(&jobType, &jobStatus); err != nil {
		return err
	}

	var label string
	switch jobType {
	case db.JobParseTransactions:
		label = "Parse transactions"
	case db.JobParseEvents:
		label = "Parse events"
	}

	*jobs = append(*jobs, &uiJob{
		status: jobStatus,
		label:  label,
		t:      jobType,
	})

	return nil
}

type dashboardPageData struct {
	Content         template.HTML
	StatusIndicator *uiStatusIndicatorData
}

func loadTemplates(templates *template.Template, dirPath string) {
	entries, err := os.ReadDir(dirPath)
	assert.NoErr(err, "unable to read html dir")

	for _, entry := range entries {
		if !entry.IsDir() {
			fileName := path.Join(dirPath, entry.Name())
			fileData, err := os.ReadFile(fileName)
			assert.NoErr(err, fmt.Sprintf("unable to read html for: %s", fileName))

			// NOTE: replace new lines with space so we can send SSE updates
			// without encoding
			sb := strings.Builder{}
			var prev byte

			isWhitespace := func(c byte) bool {
				return c == ' ' || c == '\t'
			}

			for _, c := range fileData {
				if c != '\n' {
					if !isWhitespace(c) || (isWhitespace(c) && !isWhitespace(prev)) {
						sb.WriteByte(c)
					}
				}

				prev = c
			}

			templates, err = templates.Parse(sb.String())
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
		"struct": func(keyvals ...any) map[string]any {
			assert.True(len(keyvals)%2 == 0, "invalid struct args len")

			m := make(map[string]any)
			for i := 0; i < len(keyvals); i += 2 {
				k, v := keyvals[i], keyvals[i+1]
				key, ok := k.(string)
				assert.True(ok, "invalid key")
				m[key] = v
			}

			return m
		},
		"now": func() int64 {
			return time.Now().Unix()
		},
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
		"/wallets",
		walletsHandler(pool, templates),
	)
	http.HandleFunc(
		"/wallets/sse",
		walletsSseHandler(pool, templates),
	)
	http.HandleFunc(
		"/jobs/sse",
		jobsSseHandler(pool, templates),
	)
	http.HandleFunc(
		"/events",
		eventsHandler(context.Background(), pool, templates),
	)
	http.HandleFunc(
		"/tokens",
		tokensHandler(pool, templates),
	)

	fmt.Println("Listening at: http://localhost:8888")
	err = http.ListenAndServe("localhost:8888", nil)
	assert.NoErr(err, "")
}
