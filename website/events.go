package main

import (
	"context"
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

var eventsGroupHeaderComponent = `<!--html-->
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

type eventComponentData struct {
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

var eventComponent = `
{{define "event"}}
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

type eventsPageComponentData struct {
	LastEventIdx int32
	Events       template.HTML
}

var eventsPageComponent = `
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
	idx  int32
	errs pgtype.FlatArray[*EventErr]
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
			e.tx_id, e.network, e.ix_idx, e.idx, e.timestamp,
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
			e.user_account_id = $1 and e.idx > $3
		group by
			e.tx_id, e.network, e.idx, e.ix_idx, e.timestamp,
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
		var dataMarshaled []byte
		err := rows.Scan(
			&e.TxId,
			&e.Network,
			&e.IxIdx,
			&e.idx,
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

func eventsHandler(
	ctx context.Context,
	pool *pgxpool.Pool,
	templates *template.Template,
) http.HandlerFunc {
	loadTemplate(templates, eventsPageComponent)
	loadTemplate(templates, eventComponent)
	loadTemplate(templates, eventsGroupHeaderComponent)
	loadTemplate(templates, amountComponent)

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
