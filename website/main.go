package main

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"taxee/pkg/dotenv"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

var eventsGroupHeaderComponent = `
{{ define "events_group_header" }}
<li class="
	col-span-full w-full rounded-md bg-gray-200 px-4 mt-4 h-8 bg-crossed
	flex items-center
">
	<p>{{ formatDate . }}</p>
</li>
{{ end }}
`

type parserErr struct {
	IxIdx   int32
	Kind    db.ErrType
	Address string
	Data    any
}

type TransactionComponentData struct {
	TxId       string
	App        string
	MethodType string
	Timestamp  time.Time

	DisposalsAmounts    template.HTML
	DisposalsValues     template.HTML
	AcquisitionsAmounts template.HTML
	AcquisitionsValues  template.HTML
	Gain                string
}

var transactionComponent = `
{{define "transaction"}}
	<li class="
		col-span-full grid grid-cols-subgrid items-center
		mt-4 relative w-full px-4 min-h-16
	">
		<div class="flex flex-col">
			<a href="https://solscan.io/tx/{{ .TxId }}" target="_blank">
				{{ shorten .TxId 4 2 }}
			</a>
			<p class="text-sm text-gray-600">{{ formatTime .Timestamp }}</p>
		</div>

		<!-- card bg -->
		<div 
			class="
				absolute col-[2/-1] top-0 left-0 bottom-0 -right-4
				bg-none border border-gray-200
				mouse-events-none bg-gray-100 rounded-lg
			"
		></div>

		<!-- divider -->
		<div class="col-[2/3]"></div>

		<!-- app / method info -->
		<div class="flex items-center absolute col-[3/4] h-full">
			<div class="w-10 h-10 rounded-full bg-gray-400"></div>
			<div class="flex flex-col ml-4">
				<h3 class="font-medium">{{ toTitle .MethodType }}</h3>
				<p class="text-sm text-gray-600">{{ toTitle .App }}</p>
			</div>
		</div>

		<!-- Disposals -->
		<div class="absolute col-[4/5]">
			{{ .DisposalsAmounts }}
		</div>
		<div class="absolute col-[5/6]">
			{{ .DisposalsValues }}
		</div>
		<div class="absolute col-[6/7]">
			{{ .AcquisitionsAmounts }}
		</div>
		<div class="absolute col-[7/8]">
			{{ .AcquisitionsValues }}
		</div>
		<p class="absolute col-[8/9]">{{ .Gain }}</p>
	</li>
{{end}}
`

type TransactionsComponentData struct {
	Events template.HTML
}

var transactionsComponent = `
{{define "transactions"}}
	<div class="mx-auto w-[80%] py-10">
		<header class="py-10 flex items-center justify-between">
			<h1 class="text-3xl font-semibold text-gray-900">Transactions</h1>

			<div>
				<a
					href="?from_slot=&from_block_index="
				>
					Next
				</a>
			</div>
		</header>
		<ul class="
			w-full relative
			grid grid-cols-[180px_1rem_minmax(250px,20%)_repeat(4,1fr)_8%]
		">
			<li class="
				h-10 px-4 col-span-full grid grid-cols-subgrid items-center 
				sticky top-2 z-100 bg-gray-50 border rounded-lg border-gray-200
			">
				<p>Date</p> 
				<div></div>
				<p>App</p>
				<p>Sent</p>
				<p>Disposal</p>
				<p>Received</p>
				<p>Acquisition</p>
				<p>Gain</p>
			</li>
			{{ .Events }}
		</ul>
	</div>
{{end}}
`

var transactionsPage = `
{{define "transactions_page"}}
	<!DOCTYPE html>
	<html>
		<head>
			<title>Taxee</title>
			<link href="static/output.css" rel="stylesheet">
		</head>
		<body class="min-h-screen bg-gray-50">
			{{template "transactions" .}}
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

type EventErr struct {
	IxIdx   pgtype.Int4
	Idx     int32
	Origin  db.ErrOrigin
	Kind    db.ErrType
	Address string
	Data    []byte
}

type eventWithErrors struct {
	db.Event
	errs pgtype.FlatArray[*EventErr]
}

func getEventsWithErrors(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
	limit int32,
) ([]*eventWithErrors, error) {
	// TODO: still will probably require tx ?? slot based search ?
	const q = `
		select
			e.tx_id, e.network, e.ix_idx, e.timestamp,
			e.ui_app_name, e.ui_method_name, e.type,
			e.data,
			array_agg(
				row(
					err.idx,
					err.origin,
					err.type,
					err.address,
					err.data
				) order by err.idx asc
			) filter (where err.id is not null) as errs
		from
			event e
		left join
			err on
				err.user_account_id = e.user_account_id and
				err.tx_id = e.tx_id and
				err.ix_idx = e.ix_idx 
		where
			e.user_account_id = $1
		group by
			e.tx_id, e.network, e.idx, e.ix_idx, e.timestamp,
			e.ui_app_name, e.ui_method_name, e.type,
			e.data
		order by
			e.idx asc
		limit
			$2
	`
	rows, err := pool.Query(ctx, q, userAccountId, limit)
	if err != nil {
		return nil, fmt.Errorf("unable to get events with errors: %w", err)
	}

	res := make([]*eventWithErrors, 0)
	for rows.Next() {
		e := eventWithErrors{}
		var dataMarshaled []byte
		err := rows.Scan(
			&e.TxId,
			&e.Network,
			&e.IxIdx,
			&e.Timestamp,
			&e.UiAppName,
			&e.UiMethodName,
			&e.Type,
			&dataMarshaled,
			&e.errs,
		)
		if err != nil {
			return nil, fmt.Errorf("unable to scan event: %w", err)
		}

		err = e.Event.UnmarshalData(dataMarshaled)
		assert.NoErr(err, "unable to unmarshal transfers")
		res = append(res, &e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to scan events: %w", err)
	}

	return res, nil
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
		"shorten": func(s string, start, end int) string {
			return fmt.Sprintf("%s...%s", s[:start], s[len(s)-end:])
		},
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
	loadTemplate(templates, transactionsPage)
	loadTemplate(templates, transactionsComponent)
	loadTemplate(templates, transactionComponent)
	loadTemplate(templates, eventsGroupHeaderComponent)

	pool, err := db.InitPool(context.Background(), appEnv)
	assert.NoErr(err, "")

	serveStaticFiles(prod)

	http.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/static/favicon.ico", http.StatusMovedPermanently)
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() != "/" {
			w.WriteHeader(404)
			return
		}

		query := r.URL.Query()
		userAccountId := int32(1)
		if prod {
			userAccountIdParam := query.Get("user_account_id")
			id, err := strconv.Atoi(userAccountIdParam)
			if err != nil {
				w.WriteHeader(400)
				w.Write(fmt.Appendf(nil, "Invalid user account id: %s", userAccountIdParam))
				return
			}
			userAccountId = int32(id)

			// TODO: Auth
		}

		const limit = 50
		events, err := getEventsWithErrors(
			context.Background(), pool, userAccountId, limit,
		)
		if err != nil {
			w.WriteHeader(500)
			m := fmt.Sprintf("unable to query events: %s", err)
			w.Write([]byte(m))
			return
		}

		renderedTransactions := make([]byte, 0)
		var prevGroupTsUnix int64

		for _, event := range events {
			const oneDay = 60 * 60 * 24
			dateTsUnix := event.Timestamp.Unix() / oneDay * oneDay
			if prevGroupTsUnix != dateTsUnix {
				groupHeader, err := executeTemplate(
					templates, "events_group_header", time.Unix(dateTsUnix, 0),
				)
				if err != nil {
					w.WriteHeader(500)
					w.Write(fmt.Appendf(nil, "500 server error: %s", err))
					return
				}
				renderedTransactions = append(renderedTransactions, groupHeader...)
				prevGroupTsUnix = dateTsUnix
			}

			cmpData := TransactionComponentData{
				TxId:       event.TxId,
				App:        event.UiAppName,
				MethodType: string(event.Type),
				Timestamp:  event.Timestamp,
			}

			switch data := event.Data.(type) {
			case *db.EventTransferInternal:
				amount := fmt.Sprintf("<p>%s</p>", data.Amount.StringFixed(2))
				value := fmt.Sprintf("<p>%s</p>", data.Value.StringFixed(2))

				cmpData.DisposalsAmounts = template.HTML(amount)
				cmpData.DisposalsValues = template.HTML(value)
				cmpData.AcquisitionsAmounts = template.HTML(amount)
				cmpData.AcquisitionsValues = template.HTML(value)

				cmpData.Gain = data.Profit.StringFixed(2)
			case *db.EventTransfer:
				if event.Type == db.EventTypeMint ||
					(event.Type == db.EventTypeTransfer && data.Direction == db.EventTransferIncoming) {
					cmpData.DisposalsAmounts = "<p>-</p>"
					cmpData.DisposalsValues = "<p>-</p>"
					cmpData.AcquisitionsAmounts = template.HTML(fmt.Sprintf(
						"<p>%s</p>", data.Amount.StringFixed(2),
					))
					cmpData.AcquisitionsValues = template.HTML(fmt.Sprintf(
						"<p>%s</p>", data.Value.StringFixed(2),
					))
				} else if event.Type == db.EventTypeBurn ||
					(event.Type == db.EventTypeTransfer && data.Direction == db.EventTransferOutgoing) {
					cmpData.DisposalsAmounts = template.HTML(fmt.Sprintf(
						"<p>%s</p>", data.Amount.StringFixed(2),
					))
					cmpData.DisposalsValues = template.HTML(fmt.Sprintf(
						"<p>%s</p>", data.Value.StringFixed(2),
					))
					cmpData.AcquisitionsAmounts = "<p>-</p>"
					cmpData.AcquisitionsValues = "<p>-</p>"
				} else {
					assert.True(false, "invalid event type: %T", event.Type)
				}

				cmpData.Gain = data.Profit.StringFixed(2)
			default:
				assert.True(false, "invalid event data: %T", data)
			}

			rendered, err := executeTemplate(templates, "transaction", cmpData)
			if err != nil {
				w.WriteHeader(500)
				w.Write(fmt.Appendf(nil, "500 server error: %s", err))
				return
			}
			renderedTransactions = append(renderedTransactions, rendered...)
		}

		w.Header().Set("content-type", "text/html")
		buf, err := executeTemplate(templates, "transactions_page", TransactionsComponentData{
			Events: template.HTML(renderedTransactions),
		})
		if err != nil {
			w.WriteHeader(500)
			m := fmt.Sprintf("500 server error: %s", err)
			w.Write([]byte(m))
		}
		w.Write(buf)
	})

	fmt.Println("Listening at: http://localhost:8888")
	err = http.ListenAndServe("localhost:8888", nil)
	assert.NoErr(err, "")
}
