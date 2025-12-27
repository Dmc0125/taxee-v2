package main

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"taxee/pkg/assert"
	"taxee/pkg/coingecko"
	"taxee/pkg/db"
	"taxee/pkg/dotenv"
	"time"
	"unsafe"

	"github.com/jackc/pgx/v5"
	"github.com/lmittmann/tint"
	"github.com/shopspring/decimal"
)

// Components

const (
	navbarStatusUninit     = ""
	navbarStatusSuccess    = "success"
	navbarStatusError      = "error"
	navbarStatusInProgress = "in_progress"
)

type cmpNavbarStatus struct {
	SseSubscribe bool
	Status       string
	Wallets      []string
}

func (c *cmpNavbarStatus) eq(other *cmpNavbarStatus) bool {
	if c.Status == navbarStatusInProgress && c.Status == other.Status {
		if len(c.Wallets) != len(other.Wallets) {
			return false
		}

		for i, w := range c.Wallets {
			if w != other.Wallets[i] {
				return false
			}
		}

		return true
	}

	return c.Status == other.Status
}

func (c *cmpNavbarStatus) appendWallet(label string, status db.WalletStatus) {
	switch status {
	case db.WalletQueued, db.WalletInProgress:
		c.Wallets = append(c.Wallets, label)
	}
}

func (c *cmpNavbarStatus) appendParser(status db.ParserStatus) {
	switch status {
	case db.ParserStatusUninitialized:
		c.Status = navbarStatusUninit
	case db.ParserStatusSuccess:
		c.Status = navbarStatusSuccess
	case db.ParserStatusPTError, db.ParserStatusPEError:
		c.Status = navbarStatusError
	default:
		c.Status = navbarStatusInProgress
	}
}

func (c *cmpNavbarStatus) appendParserFromRow(row pgx.Row) error {
	var status db.ParserStatus
	if err := row.Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			c.Status = navbarStatusUninit
			return nil
		}
		return err
	}

	c.appendParser(status)
	return nil
}

type cmpDashboard struct {
	Content      template.HTML
	NavbarStatus *cmpNavbarStatus
}

// Networking

var errParamMissing = errors.New("missing")

type paramGetter interface {
	Get(string) string
}

func parseIntParam[T any](
	getter paramGetter,
	result *T,
	key string,
	positiveOnly bool,
	size int,
) error {
	value := getter.Get(key)

	if value == "" {
		return fmt.Errorf("%s: %w", key, errParamMissing)
	}

	if v, err := strconv.ParseInt(value, 10, size); err == nil {
		if positiveOnly && v < 0 {
			return fmt.Errorf("%s: must be positive", key)
		}
		*result = *(*T)(unsafe.Pointer(&v))
		return nil
	} else {
		return fmt.Errorf("%s: %w", key, err)
	}
}

func newResponseHtml(fragments ...[]byte) []byte {
	var sum []byte
	for i, f := range fragments {
		sum = append(sum, f...)
		if i < len(fragments)-1 {
			sum = append(sum, []byte("<!--delimiter-->")...)
		}
	}
	return sum
}

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

func renderError(r http.ResponseWriter, statusCode int, msg string, args ...any) {
	r.WriteHeader(statusCode)
	r.Write(fmt.Appendf(nil, msg, args...))
}

// templates helpers

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

// helpers

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

func formatDecimal(d decimal.Decimal, digits int32) string {
	assert.True(digits >= 0, "digits can not be negative")

	if d.IsZero() {
		return "0"
	}

	isNegative := d.IsNegative()
	if isNegative {
		d = d.Abs()
	}

	formatWithSuffix := func(n decimal.Decimal, suffix string, maxDigits int32) string {
		intDigits := len(fmt.Sprintf("%d", n.IntPart()))
		decimalPlaces := max(int(maxDigits)-intDigits, 0)

		result := d.StringFixed(int32(decimalPlaces))
		result = strings.TrimRight(result, "0")
		result = strings.TrimRight(result, ".")

		return result + suffix
	}

	var (
		dAbs     = d.Abs()
		smallNum = decimal.New(1, -digits)
		one      = decimal.NewFromInt(1)
		thousand = decimal.NewFromInt(1_000)
		million  = decimal.NewFromInt(1_000_000)
		billion  = decimal.NewFromInt(1_000_000_000)
		trillion = decimal.NewFromInt(1_000_000_000_000)

		result string
	)

	switch {
	// Very small numbers: < 0.0001
	case dAbs.LessThan(smallNum):
		result = fmt.Sprintf("<%s", smallNum.String())
	// Small numbers: 0.0001 <= x < 1
	case dAbs.LessThan(one):
		result = d.StringFixed(digits)
		result = strings.TrimRight(result, "0")
		result = strings.TrimRight(result, ".")
	// Regular numbers: 1 <= x <= 999
	case dAbs.LessThan(thousand):
		intDigits := len(fmt.Sprintf("%d", dAbs.IntPart()))
		decimalPlaces := max(5-intDigits, 0)

		result = d.StringFixed(int32(decimalPlaces))
		result = strings.TrimRight(result, "0")
		result = strings.TrimRight(result, ".")
	// Thousands: 1000 <= x < 1M
	case dAbs.LessThan(million):
		result = formatWithSuffix(d.Div(thousand), "K", digits-1)
	// Millions: 1M <= x < 1B
	case dAbs.LessThan(billion):
		result = formatWithSuffix(d.Div(million), "M", digits-1)
	// Billions: 1B <= x < 1T
	case dAbs.LessThan(trillion):
		result = formatWithSuffix(d.Div(billion), "B", digits-1)
	// Trillions and above
	default:
		result = formatWithSuffix(d.Div(trillion), "T", digits-1)
	}

	if isNegative && result != "<0.0001" {
		result = "-" + result
	}

	return result
}

func main() {
	appEnv := os.Getenv("APP_ENV")
	prod := true
	if appEnv != "PROD" {
		assert.NoErr(dotenv.ReadEnv(), "")
		prod = false
	}

	slog.SetDefault(slog.New(tint.NewHandler(os.Stderr, &tint.Options{
		AddSource: true,
	})))

	htmlPath := os.Getenv("HTML")
	assert.True(len(htmlPath) > 0, "missing html path")

	coingeckoApiKey := os.Getenv("COINGECKO_API_KEY")
	coingeckoApiType := os.Getenv("COINGECKO_API_TYPE")
	coingecko.Init(coingeckoApiKey, coingeckoApiType)

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
		"formatDecimal": formatDecimal,
		// TODO: remove
		"round": func(n decimal.Decimal) string {
			return n.StringFixed(2)
		},
		"now": func() int64 {
			return time.Now().Unix()
		},
		"dateRelative": func(t time.Time) string {
			n := time.Now()
			if n.Day() != t.Day() {
				return t.Format("02/01/2006")
			}
			return t.Format("15:04:05")
		},
		"date": func(t time.Time) string {
			return t.Format("02/01/2006")
		},
		"time": func(t time.Time) string {
			return t.Format("15:04:05")
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
		"/parser/sse",
		parserSseHandler(pool, templates),
	)
	http.HandleFunc(
		"/events",
		eventsHandler(pool, templates),
	)
	http.HandleFunc(
		"/tokens",
		tokensHandler(pool, templates),
	)

	fmt.Println("Listening at: http://localhost:8888")
	err = http.ListenAndServe("localhost:8888", nil)
	assert.NoErr(err, "")
}
