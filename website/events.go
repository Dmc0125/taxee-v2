package main

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TODO:
//
// Pagination - first page, last page
// Image urls
// Symbols seem to not be working correcly
// Missing prices should display coingecko token names, symbols and id
// Amount rounding - >0.01 / 5.44e3 ??
// Eplorer urls for each network
// Programs images

var eventsGroupHeaderComponent = `<!-- html -->
{{ define "events_group_header" }}
<li class="
	w-full rounded-md bg-gray-200 px-4 mt-4 h-8 bg-crossed
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

var amountComponent = `<!-- html -->
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

type parserErrorComponentData struct {
	Title string
	Type  db.ParserErrorType

	Address  string
	Expected string
	Had      string

	Token string
}

func newParserErrorComponentData(parserError *parserError) parserErrorComponentData {
	var d parserErrorComponentData
	d.Type = parserError.Type

	switch parserError.Type {
	case db.ParserErrorTypeMissingAccount:
		d.Title = "Missing account"

		data, ok := parserError.Data.(*db.ParserErrorMissingAccount)
		assert.True(ok, "invalid parser error data: %T", parserError.Data)
		d.Address = data.AccountAddress
	case db.ParserErrorTypeAccountBalanceMismatch:
		d.Title = "Balance mismatch"

		data, ok := parserError.Data.(*db.ParserErrorAccountBalanceMismatch)
		assert.True(ok, "invalid parser error data: %T", parserError.Data)
		d.Address = data.AccountAddress
		d.Expected = data.Expected.String()
		d.Had = data.Had.String()
	case db.ParserErrorTypeInsufficientBalance:
	case db.ParserErrorTypeMissingPrice:
		d.Title = "Missing price"

		data, ok := parserError.Data.(*db.ParserErrorMissingPrice)
		assert.True(ok, "invalid parser error data: %T", parserError.Data)
		d.Token = data.Token
	default:
		assert.True(false, "invalid parser error type: %d", parserError.Type)
	}

	fmt.Println(parserErrorComponent)

	return d
}

var parserErrorComponent = fmt.Sprintf(
	`<!-- html -->
	{{ define "parser_error" }}
	<li class="p-2 rounded-sm flex flex-col bg-red-100">
		<span class="font-medium text-sm text-gray-800">
			{{ .Title }}
		</span>
		{{ if eq .Type %d }}
			<span class="text-sm text-gray-800">
				{{ .Address }}
			</span>
		{{ else if eq .Type %d }}
		{{ else if eq .Type %d }}
			<span class="text-sm text-gray-800">
				{{ .Token }}
			</span>
		{{ end }}
	</li>
	{{ end }}
	`,
	db.ParserErrorTypeMissingAccount,
	db.ParserErrorTypeAccountBalanceMismatch,
	db.ParserErrorTypeMissingPrice,
)

type eventComponentData struct {
	TxId          string
	App           string
	MethodType    string
	Timestamp     time.Time
	ExplorerUrl   string
	NetworkImgUrl string

	DisposalsAmounts    template.HTML
	DisposalsValues     template.HTML
	AcquisitionsAmounts template.HTML
	AcquisitionsValues  template.HTML
	Gain                template.HTML

	PreprocessErrors []*parserErrorComponentData
	ProcessErrors    []*parserErrorComponentData
	PriceErrors      []*parserErrorComponentData
}

type networkUrls struct {
	explorer string
	img      string
}

var networksUrls = map[db.Network]networkUrls{
	db.NetworkSolana: {
		explorer: "solscan.io",
		img:      "https://solana.com/src/img/branding/solanaLogoMark.svg",
	},
	db.NetworkArbitrum: {
		explorer: "arbiscan.io",
		img:      "https://file.notion.so/f/f/80206c3c-8bc5-49a2-b0cd-756884a06880/03db2a62-e8b5-4ec0-9d9d-e044010606c6/0923_Arbitrum_Logos_Logomark_RGB.svg?table=block&id=4b82f11a-85c1-4775-a4f1-904d43798391&spaceId=80206c3c-8bc5-49a2-b0cd-756884a06880&expirationTimestamp=1762228800000&signature=jT8E7tBzukmQgLg1cqtASkdQ7zZ5i-Ntmtnkky-ggUA&downloadName=Primary_Logomark_RGB.svg",
	},
}

var eventComponent = `<!-- html -->
{{define "event"}}
<li class="
	mt-4 w-full 
	flex items-stretch
">
	<div class="pl-4 w-[180px] flex flex-col flex-shrink-0">
		<a href="{{ .ExplorerUrl }}" target="_blank">
			{{ shorten .TxId 4 2 }}
		</a>
		<p class="text-sm text-gray-600">{{ formatTime .Timestamp }}</p>
	</div>

	<!-- card -->
	<div class="
		w-full h-full self-stretch
		bg-gray-100 border border-gray-200 rounded-lg
	">
		<div class="
			p-4 grid grid-cols-[250px_repeat(4,1fr)_8%]
			items-center
		">
			<div class="flex items-center h-full">
				<div class="w-10 h-10 rounded-full bg-gray-400 relative">
					<div class="
						absolute w-4 h-4 top-0 left-0 rounded-full
					">
						<img class="w-full h-full" src="{{ .NetworkImgUrl }}" />
					</div>
				</div>
				<div class="flex flex-col ml-4">
					<h3 class="font-medium">{{ toTitle .MethodType }}</h3>
					<p class="text-sm text-gray-600">{{ toTitle .App }}</p>
				</div>
			</div>

			<div> 
				{{ .DisposalsAmounts }}
			</div>
			<div> 
				{{ .DisposalsValues }}
			</div>
			<div> 
				{{ .AcquisitionsAmounts }}
			</div>
			<div> 
				{{ .AcquisitionsValues }}
			</div>
			<div>{{ .Gain }}</div>
		</div>

		<div class="grid grid-cols-[repeat(3,1fr)] border-t border-gray-200">
			<div class="p-4 border-r border-gray-200">
				<p>Preprocess errors</p>
				<ul class="mt-2 flex flex-col gap-y-2">
					{{ range .PreprocessErrors }}
						{{ template "parser_error" . }}
					{{ end }}
				</ul>
			</div>
			<div class="p-4 border-r border-gray-200">
				<p>Process errors</p>
				<ul class="mt-2 flex flex-col gap-y-2">
					{{ range .ProcessErrors }}
						{{ template "parser_error" . }}
					{{ end }}
				</ul>
			</div>
			<div class="p-4 border-r border-gray-200">
				<p>Price errors</p>
				<ul class="mt-2 flex flex-col gap-y-2">
					{{ range .PriceErrors }}
						{{ template "parser_error" . }}
					{{ end }}
				</ul>
			</div>

		</div>
	</div>
</li>
{{end}}
`

type eventsPageComponentData struct {
	LastEventIdx int32
	Events       template.HTML
}

var eventsPageComponent = `<!-- html -->
{{define "events_page"}}
	<div class="mx-auto w-[80%] py-10">
		<header class="py-10 flex items-center justify-between">
			<h1 class="text-3xl font-semibold text-gray-900">Transactions</h1>

			<div>
				<a
					href="?from={{ .LastEventIdx }}"
				>
					Next
				</a>
			</div>
		</header>
		<ul class="
			w-full relative
		">
			<li class="
					h-10 w-full px-4 z-1000
					flex items-center
					sticky top-2 bg-gray-50 border rounded-lg border-gray-200
				"
			>
				<p class="w-[180px] flex-shrink-0">Date</p> 
				<div class="
					w-full
					grid grid-cols-[250px_repeat(4,1fr)_8%]
				">
					<p>App</p>
					<p>Sent</p>
					<p>Disposal</p>
					<p>Received</p>
					<p>Acquisition</p>
					<p>Gain</p>
				</div>
			</li>
			{{ .Events }}
		</ul>
	</div>
{{end}}
`

type parserError struct {
	Origin db.ErrOrigin
	Type   db.ParserErrorType
	Data   any
}

func (dst *parserError) UnmarshalJSON(src []byte) error {
	type noData struct {
		Origin db.ErrOrigin `json:"origin"`
		Type   string       `json:"type"`
	}
	var nd noData
	if err := json.Unmarshal(src, &nd); err != nil {
		return fmt.Errorf("unable to unmarshal type and oriding: %w", err)
	}

	t, err := db.NewParserErrorTypeFromString(nd.Type)
	if err != nil {
		return fmt.Errorf("invalid error type: %s", nd.Type)
	}

	dst.Origin = nd.Origin
	dst.Type = t

	type d[T any] struct {
		Data *T `json:"data"`
	}

	switch t {
	case db.ParserErrorTypeMissingAccount:
		var data d[db.ParserErrorMissingAccount]
		if err = json.Unmarshal(src, &data); err != nil {
			return fmt.Errorf("unable to unmarshal parser error data: %w", err)
		}
		dst.Data = data.Data
	case db.ParserErrorTypeAccountBalanceMismatch:
		var data d[db.ParserErrorAccountBalanceMismatch]
		if err = json.Unmarshal(src, &data); err != nil {
			return fmt.Errorf("unable to unmarshal parser error data: %w", err)
		}
		dst.Data = data.Data
	case db.ParserErrorTypeInsufficientBalance:
		var data d[db.ParserErrorInsufficientBalance]
		if err = json.Unmarshal(src, &data); err != nil {
			return fmt.Errorf("unable to unmarshal parser error data: %w", err)
		}
		dst.Data = data.Data
	case db.ParserErrorTypeMissingPrice:
		var data d[db.ParserErrorMissingPrice]
		if err = json.Unmarshal(src, &data); err != nil {
			return fmt.Errorf("unable to unmarshal parser error data: %w", err)
		}
		dst.Data = data.Data
	}

	return nil
}

type eventWithErrors struct {
	db.Event
	idx    int32
	errors []*parserError
}

func getEventsWithErrors(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId,
	fromIdx int32,
	limit int32,
) ([]*eventWithErrors, error) {
	// TODO: still will probably require tx ?? slot based search ?
	const q = `
		select
			e.idx,
			e.tx_id,
			e.network,
			e.ix_idx,
			e.timestamp,
			e.ui_app_name,
			e.ui_method_name,
			e.type,
			e.data,
			coalesce(
				jsonb_agg(
					jsonb_build_object(
						'origin', err.origin,
						'type', err.type,
						'data', err.data
					) order by err.id asc
				) filter (where err.id is not null),
				'[]'::jsonb
			) as errs
		from
			event e
		left join
			parser_err err on
				err.user_account_id = e.user_account_id and
				err.tx_id = e.tx_id and
				err.ix_idx = e.ix_idx 
		where
			e.user_account_id = $1 and e.idx > $3
		group by
			e.idx, e.tx_id, e.network, e.ix_idx, e.timestamp,
			e.ui_app_name, e.ui_method_name, e.type,
			e.data
		order by
			e.idx asc
		limit
			$2
	`
	rows, err := pool.Query(ctx, q, userAccountId, limit, fromIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to get events with errors: %w", err)
	}

	res := make([]*eventWithErrors, 0)
	for rows.Next() {
		e := eventWithErrors{}
		var dataMarshaled, errorsMarshaled []byte
		err := rows.Scan(
			&e.idx,
			&e.TxId,
			&e.Network,
			&e.IxIdx,
			&e.Timestamp,
			&e.UiAppName,
			&e.UiMethodName,
			&e.Type,
			&dataMarshaled,
			&errorsMarshaled,
		)
		if err != nil {
			return nil, fmt.Errorf("unable to scan event: %w", err)
		}

		err = e.Event.UnmarshalData(dataMarshaled)
		assert.NoErr(err, "unable to unmarshal event data")

		err = json.Unmarshal(errorsMarshaled, &e.errors)
		assert.NoErr(err, "unable to unmarshal errors")

		res = append(res, &e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to scan events: %w", err)
	}

	return res, nil
}

func eventsHandler(
	ctx context.Context,
	pool *pgxpool.Pool,
	templates *template.Template,
) http.HandlerFunc {
	loadTemplate(templates, eventsPageComponent)
	loadTemplate(templates, eventComponent)
	loadTemplate(templates, eventsGroupHeaderComponent)
	loadTemplate(templates, amountComponent)
	loadTemplate(templates, parserErrorComponent)

	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		userAccountId := int32(1)
		// if prod {
		// 	userAccountIdParam := query.Get("user_account_id")
		// 	id, err := strconv.Atoi(userAccountIdParam)
		// 	if err != nil {
		// 		renderError(w, 400, "invalid user account id: %s", userAccountIdParam)
		// 		return
		// 	}
		// 	userAccountId = int32(id)
		//
		// 	// TODO: Auth
		// }

		var fromIdx int32
		fromIdxQueryVal := query.Get("from")
		if v, err := strconv.Atoi(fromIdxQueryVal); err == nil {
			fromIdx = int32(v)
		}

		const limit = 50
		events, err := getEventsWithErrors(
			ctx, pool, userAccountId, fromIdx, limit,
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
				where
					exists (
						select 1 from coingecko_token ct where
							ct.address = $1 and ct.network = $2
					) or ctd.coingecko_id = $1
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

		br := pool.SendBatch(ctx, &fetchTokensBatch)
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

			cmpData := eventComponentData{
				TxId:       event.TxId,
				App:        event.UiAppName,
				MethodType: string(event.Type),
				Timestamp:  event.Timestamp,

				PreprocessErrors: make([]*parserErrorComponentData, 0),
				ProcessErrors:    make([]*parserErrorComponentData, 0),
				PriceErrors:      make([]*parserErrorComponentData, 0),
			}

			networkUrls, ok := networksUrls[event.Network]
			assert.True(ok, "missing network urls: %s", event.Network.String())
			cmpData.ExplorerUrl = fmt.Sprintf("%s/tx/%s", networkUrls.explorer, event.TxId)
			cmpData.NetworkImgUrl = networkUrls.img

			for _, parserError := range event.errors {
				data := newParserErrorComponentData(parserError)

				if _, ok := parserError.Data.(*db.ParserErrorMissingPrice); ok {
					cmpData.PriceErrors = append(cmpData.PriceErrors, &data)
					continue
				}

				switch parserError.Origin {
				case db.ErrOriginPreprocess:
					cmpData.PreprocessErrors = append(cmpData.PreprocessErrors, &data)
				case db.ErrOriginProcess:
					cmpData.ProcessErrors = append(cmpData.ProcessErrors, &data)
				}
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

			rendered, err := executeTemplate(templates, "event", cmpData)
			if err != nil {
				w.WriteHeader(500)
				w.Write(fmt.Appendf(nil, "500 server error: %s", err))
				return
			}
			renderedTransactions = append(renderedTransactions, rendered...)
		}

		lastEventIdx := int32(-1)
		if len(events) > 0 {
			lastEventIdx = events[len(events)-1].idx
		}

		w.Header().Set("content-type", "text/html")
		buf := executeTemplateMust(templates, "events_page", eventsPageComponentData{
			LastEventIdx: lastEventIdx,
			Events:       template.HTML(renderedTransactions),
		})

		page := executeTemplateMust(templates, "page_layout", pageLayoutComponentData{
			Content: template.HTML(buf),
		})
		w.Write(page)
	}
}
