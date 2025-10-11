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

	"github.com/jackc/pgx/v5"
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

type amountComponentData struct {
	Symbol   string
	Amount   string
	ImageUrl string
}

var amountComponent = `
{{ define "amount" }}
	<div class="flex gap-x-1">
		{{ if .Amount }}
			<p>{{ .Amount }}</p>
			<p>{{ upper .Symbol }}</p>
		{{ else }}
			<p>-</p>
		{{ end }}
	</div>
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
	Gain                template.HTML
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
		<div class="absolute col-[8/9]">{{ .Gain }}</div>
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

func renderError(r http.ResponseWriter, statusCode int, msg string, args ...any) {
	r.WriteHeader(statusCode)
	r.Write(fmt.Appendf(nil, msg, args...))
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
	loadTemplate(templates, amountComponent)

	pool, err := db.InitPool(context.Background(), appEnv)
	assert.NoErr(err, "")

	serveStaticFiles(prod)

	http.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/static/favicon.ico", http.StatusMovedPermanently)
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() != "/" {
			renderError(w, 404, "")
			return
		}

		query := r.URL.Query()
		userAccountId := int32(1)
		if prod {
			userAccountIdParam := query.Get("user_account_id")
			id, err := strconv.Atoi(userAccountIdParam)
			if err != nil {
				renderError(w, 400, "invalid user account id: %s", userAccountIdParam)
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
			renderError(w, 500, "unable to query events: %s", err)
			return
		}

		// fetch tokens
		fetchTokensBatch := pgx.Batch{}
		type tokenId struct {
			address string
			network db.Network
		}
		type tokenMeta struct {
			symbol   string
			imageUrl string
		}
		tokens := make(map[tokenId]tokenMeta)

		for _, event := range events {
			const q = `
				select 
					ctd.symbol, ctd.image_url
				from
					coingecko_token_data ctd
				inner join
					coingecko_token ct on
						ct.coingecko_id = ctd.coingecko_id
				where
					ct.address = $1 and ct.network = $2
			`

			var token string

			switch data := event.Data.(type) {
			case *db.EventTransferInternal:
				token = data.Token
			case *db.EventTransfer:
				token = data.Token
			default:
				assert.True(false, "unknown event data: %T", data)
			}

			queued := fetchTokensBatch.Queue(q, token, event.Network)
			queued.QueryRow(func(row pgx.Row) error {
				symbol, imgUrl := "", pgtype.Text{}
				if err := row.Scan(&symbol, &imgUrl); err != nil {
					return nil
				}

				meta := tokenMeta{
					symbol: symbol,
				}
				if imgUrl.Valid {
					meta.imageUrl = imgUrl.String
				}

				tokenId := tokenId{
					address: token,
					network: event.Network,
				}
				tokens[tokenId] = meta
				return nil
			})
		}

		br := pool.SendBatch(context.Background(), &fetchTokensBatch)
		if err := br.Close(); err != nil {
			renderError(w, 500, "unable to query tokens: %s", err)
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
				assert.NoErr(err, "unable to execute template")

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
				token := tokens[tokenId{data.Token, event.Network}]

				amountCmp := executeTemplateMust(templates, "amount", amountComponentData{
					Symbol: token.symbol,
					Amount: data.Amount.StringFixed(2),
				})
				valueCmp := executeTemplateMust(templates, "amount", amountComponentData{
					Symbol: "eur",
					Amount: data.Value.StringFixed(2),
				})

				cmpData.DisposalsAmounts = template.HTML(amountCmp)
				cmpData.DisposalsValues = template.HTML(valueCmp)
				cmpData.AcquisitionsAmounts = template.HTML(amountCmp)
				cmpData.AcquisitionsValues = template.HTML(valueCmp)

				cmpData.Gain = template.HTML(executeTemplateMust(templates, "amount", amountComponentData{
					Symbol: "eur",
					Amount: data.Profit.StringFixed(2),
				}))
			case *db.EventTransfer:
				var disposalAmount, disposalValue,
					acquisitionAmount, acquisitionValue []byte
				token := tokens[tokenId{data.Token, event.Network}]

				if event.Type == db.EventTypeMint ||
					(event.Type == db.EventTypeTransfer && data.Direction == db.EventTransferIncoming) {
					disposalAmount = executeTemplateMust(templates, "amount", amountComponentData{})
					disposalValue = disposalAmount

					acquisitionAmount = executeTemplateMust(templates, "amount", amountComponentData{
						Symbol: token.symbol,
						Amount: data.Amount.StringFixed(2),
					})
					acquisitionValue = executeTemplateMust(templates, "amount", amountComponentData{
						Symbol: "eur",
						Amount: data.Value.StringFixed(2),
					})
				} else if event.Type == db.EventTypeBurn ||
					(event.Type == db.EventTypeTransfer && data.Direction == db.EventTransferOutgoing) {
					disposalAmount = executeTemplateMust(templates, "amount", amountComponentData{
						Symbol: token.symbol,
						Amount: data.Amount.StringFixed(2),
					})
					disposalValue = executeTemplateMust(templates, "amount", amountComponentData{
						Symbol: "eur",
						Amount: data.Value.StringFixed(2),
					})

					acquisitionAmount := executeTemplateMust(templates, "amount", amountComponentData{})
					acquisitionValue = acquisitionAmount
				} else {
					assert.True(false, "invalid event type: %T", event.Type)
				}

				cmpData.DisposalsAmounts = template.HTML(disposalAmount)
				cmpData.DisposalsValues = template.HTML(disposalValue)
				cmpData.AcquisitionsAmounts = template.HTML(acquisitionAmount)
				cmpData.AcquisitionsValues = template.HTML(acquisitionValue)

				cmpData.Gain = template.HTML(executeTemplateMust(templates, "amount", amountComponentData{
					Symbol: "eur",
					Amount: data.Profit.StringFixed(2),
				}))
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
