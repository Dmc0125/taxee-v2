package main

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"strconv"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type networkGlobals struct {
	explorerUrl string
	imgUrl      string
}

var networksGlobals = map[db.Network]networkGlobals{
	db.NetworkSolana: {
		explorerUrl: "https://solscan.io",
		imgUrl:      "/static/logo_solana.svg",
	},
	db.NetworkArbitrum: {
		explorerUrl: "https://arbiscan.io",
		imgUrl:      "/static/logo_arbitrum.svg",
	},
}

type eventTableRow struct {
	Type  uint8
	Date  time.Time
	Event eventComponentData
}

const eventsPage = `<!-- html -->
{{ define "events_page" }}
<div class="w-[80%] mx-auto mt-10">
	<header class="w-full">
		<h1 class="font-semibold text-2xl">Events</h1>
	</header>

	<ul class="w-full mt-10 flex flex-col gap-y-4 relative">
		<!-- header -->
		<li class="
			w-full py-2 pl-4 sticky z-1000 top-2
			grid grid-cols-[150px_1fr]
			bg-gray-50 border border-gray-200 rounded-lg text-gray-800
		">
			<span>Date</span>
			<div class="w-full px-4 grid grid-cols-[300px_repeat(2,450px)_1fr]">
				<span>App</span>
				<div class="flex">
					<span class="w-1/2">Outgoing</span>
					<span>Disposal</span>
				</div>
				<div class="flex">
					<span class="w-1/2">Incoming</span>
					<span>Acquisition</span>
				</div>
				<span>Profit</span>
			</div>
		</li>

		{{ range . }}
			{{ if eq .Type 0 }}
				{{ template "group_divider_component" .Date }}
			{{ else if eq .Type 1 }}
				{{ template "event_component" .Event }}
			{{ end }}
		{{ end }}
	</ul>
</div>
{{ end }}
`

const groupDividerComponent = `<!-- html -->
{{ define "group_divider_component" }}
<li class="w-full py-1 px-4 bg-crossed rounded-lg">
	<span class="text-sm text-gray-800">{{ formatDate . }}</span>
</li>
{{ end }}
`

type eventComponentData struct {
	TxId          string
	ExplorerUrl   string
	Timestamp     time.Time
	NetworkImgUrl string

	EventType string
	Method    string

	OutgoingTransfers *eventTransfersComponentData
	IncomingTransfers *eventTransfersComponentData
	Profits           []*eventFiatAmountComponentData
}

const eventComponent = `<!-- html -->
{{ define "event_component" }}
<li class="w-full pl-4 grid grid-cols-[150px_1fr]">
	<!-- Timestamp + explorer -->
	<div class="flex flex-col">
		<a
			href="{{ .ExplorerUrl }}"
			target="_blank"
			class="text-gray-800"
		>{{ .TxId }}</a>
		<span class="text-sm text-gray-700">{{ formatTime .Timestamp }}</span>
	</div>

	<!-- Event -->
	<div class="
		w-full px-4 py-2 grid grid-cols-[300px_repeat(2,450px)_1fr]
		border border-gray-200 rounded-lg
	">
		<!-- App img, network img, event type, onchain method -->
		<div class="flex">
			<div class="w-10 h-10 rounded-full bg-gray-200 relative">
				<div class="
					w-5 h-5 absolute top-0 left-0 
					rounded-full bg-gray-100 border border-gray-200
					overflow-hidden
				">
					<img 
						src="{{ .NetworkImgUrl }}" 
						class="w-full h-full p-0.25"
					/>
				</div>
			</div>

			<div class="flex flex-col ml-4">
				<span class="text-gray-800 font-medium">{{ .EventType }}</span>
				<span class="text-gray-600 text-sm">{{ .Method }}</span>
			</div>
		</div>

		<!-- Outgoing -->
		{{ template "event_transfers_component" .OutgoingTransfers }}

		<!-- Incoming -->
		{{ template "event_transfers_component" .IncomingTransfers }}

		{{ if .Profits }}
			<div class="w-full flex flex-col gap-y-1">
				<div class="opacity-0 h-[1em] text-sm"></div>
				<div class="mt-1">
					{{ range .Profits }}
						{{ template "event_fiat_amount_component" . }}
					{{ end }}
				</div>
			</div>
		{{ else }}
			<div class="w-full flex flex-col gap-y-1">
				<div class="opacity-0 h-[1em] text-sm"></div>
				<div class="mt-1">-</div>
			</div>
		{{ end }}
	</div>
</li>
{{ end }}
`

type eventTransfersComponentData struct {
	Wallet string
	Tokens []*eventTokenAmountComponentData
	Fiats  []*eventFiatAmountComponentData
}

const eventTransfersComponent = `<!-- html -->
{{ define "event_transfers_component" }}
{{ if . }}
<div class="w-full flex flex-col gap-y-1">
	<div class="w-full text-sm h-[1em]">
		<span class="text-gray-500 font-medium">{{ .Wallet }}</span>
	</div>

	<div class="flex mt-1">
		<!-- Token amounts -->
		<div class="w-1/2">
			{{ range .Tokens }}
				{{ template "event_token_amount_component" . }}
			{{ end }}
		</div>

		<!-- Fiat amounts -->
		<div>
			{{ range .Fiats }}
				{{ template "event_fiat_amount_component" . }}
			{{ end }}
		</div>
	</div>
</div>
{{ else }}
<div class="w-full flex flex-col gap-y-1">
	<div class="opacity-0 text-sm h-[1em]">placeholder</div>
	<div class="mt-1">-</div>
</div>
{{ end }}
{{ end }}
`

type eventTokenAmountComponentData struct {
	ImgUrl string
	Amount string
	Symbol string
}

// renders token img + amount + symbol
const eventTokenAmountComponent = `<!-- html -->
{{ define "event_token_amount_component" }}
<div class="w-fit flex items-center gap-2">
	<div class="w-6 h-6 flex items-center justify-center rounded-full bg-gray-200">
		{{ if .ImgUrl }}
			<img src="{{ .ImgUrl }}" />
		{{ else }}
			<span class="text-sm text-gray-600">?</span>
		{{ end }}
	</div>

	<span class="text-gray-800">{{ .Amount }} {{ upper .Symbol }}</span>
</div>
{{ end }}
`

type eventFiatAmountComponentData struct {
	Amount   string
	Currency string
}

const eventFiatAmountCompoent = `<!-- html -->
{{ define "event_fiat_amount_component" }}
<span class="text-gray-800">{{ .Amount }} {{ upper .Currency }}</span>
{{ end }}
`

func eventsHandler(
	ctx context.Context,
	pool *pgxpool.Pool,
	templates *template.Template,
) http.HandlerFunc {
	loadTemplate(templates, eventsPage)
	loadTemplate(templates, groupDividerComponent)
	loadTemplate(templates, eventComponent)
	loadTemplate(templates, eventTransfersComponent)
	loadTemplate(templates, eventTokenAmountComponent)
	loadTemplate(templates, eventFiatAmountCompoent)

	return func(w http.ResponseWriter, r *http.Request) {
		/////////////
		// parse query

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
		limit := int32(50)

		fromIdxQueryVal, limitQueryVal := query.Get("from"), query.Get("limit")

		if v, err := strconv.Atoi(fromIdxQueryVal); err == nil {
			fromIdx = int32(v)
		}
		if v, err := strconv.Atoi(limitQueryVal); err == nil {
			limit = int32(v)
		}

		/////////////
		// get events

		const getEventsQuery = `
			select
				e.tx_id,
				e.network,
				e.timestamp,
				e.type,
				e.ui_app_name,
				e.data
			from
				event e
			where
				e.user_account_id = $1 and
				e.idx > $2
			order by
				e.idx asc
			limit
				$3
		`
		eventsRows, err := pool.Query(ctx, getEventsQuery, userAccountId, fromIdx, limit)
		if err != nil {
			renderError(w, 500, "unable to query events: %s", err)
			return
		}

		type event struct {
			db.Event
		}
		events := make([]*event, 0)

		for eventsRows.Next() {
			var eventData []byte
			e := new(event)
			if err := eventsRows.Scan(
				&e.TxId,
				&e.Network,
				&e.Timestamp,
				&e.Type,
				&e.UiAppName,
				&eventData,
			); err != nil {
				renderError(w, 500, "unable to scan events: %s", err)
				return
			}

			if err := e.UnmarshalData(eventData); err != nil {
				renderError(w, 500, "unable to unmarshal event: %s", err)
				return
			}

			events = append(events, e)
		}

		/////////////
		// render events
		eventTableRows := make([]eventTableRow, 0)
		prevDateUnix := int64(0)

		for _, event := range events {
			const daySecs = 60 * 60 * 24
			dateUnix := event.Timestamp.Unix() / daySecs * daySecs

			if prevDateUnix != dateUnix {
				prevDateUnix = dateUnix
				eventTableRows = append(eventTableRows, eventTableRow{
					Type: 0,
					Date: time.Unix(dateUnix, 0),
				})
			}

			networkGlobals, ok := networksGlobals[event.Network]
			assert.True(ok, "missing globals for network: %s", event.Network.String())

			txId := fmt.Sprintf(
				"%s...%s",
				event.TxId[:5], event.TxId[len(event.TxId)-2:],
			)
			explorerUrl := fmt.Sprintf(
				"%s/tx/%s",
				networkGlobals.explorerUrl, event.TxId,
			)

			eventComponentData := eventComponentData{
				TxId:          txId,
				ExplorerUrl:   explorerUrl,
				Timestamp:     event.Timestamp,
				NetworkImgUrl: networkGlobals.imgUrl,
				EventType:     toTitle(string(event.Type)),
				Method:        toTitle(string(event.UiAppName)),
			}

			///////////////
			// transfers
			const getTokenMetaByCoingeckoId = `
				select
					symbol, image_url
				from
					coingecko_token_data 
				where
					coingecko_id = $1	
			`
			const getTokenMetaByAddressAndNetwork = `
				select
					ctd.symbol, ctd.image_url
				from
					coingecko_token ct
				inner join
					coingecko_token_data ctd on
						ct.coingecko_id = ctd.coingecko_id
				where
					ct.address = $1 and ct.network = $2
			`
			getTokensMetaBatch := pgx.Batch{}

			switch data := event.Data.(type) {
			case *db.EventTransfer:
				tokenAmountComponentData := &eventTokenAmountComponentData{
					Amount: data.Amount.StringFixed(2),
				}
				fiatAmountComponentData := &eventFiatAmountComponentData{
					Amount:   data.Value.StringFixed(2),
					Currency: "eur",
				}
				eventComponentData.Profits = append(
					eventComponentData.Profits,
					&eventFiatAmountComponentData{
						Amount:   data.Profit.StringFixed(2),
						Currency: "eur",
					},
				)

				if data.TokenSource == math.MaxUint16 {
					q := getTokensMetaBatch.Queue(
						getTokenMetaByCoingeckoId,
						data.Token,
					)
					q.QueryRow(func(row pgx.Row) error {
						var imgUrl pgtype.Text
						err := row.Scan(
							&tokenAmountComponentData.Symbol,
							&imgUrl,
						)
						if err != nil {
							return err
						}
						if imgUrl.Valid {
							tokenAmountComponentData.ImgUrl = imgUrl.String
						} else {
							// fetch from coingecko
						}
						return nil
					})
				} else {
					q := getTokensMetaBatch.Queue(
						getTokenMetaByAddressAndNetwork,
						data.Token,
						event.Network,
					)
					q.QueryRow(func(row pgx.Row) error {
						var imgUrl pgtype.Text
						err := row.Scan(
							&tokenAmountComponentData.Symbol,
							&imgUrl,
						)
						if errors.Is(err, pgx.ErrNoRows) {
							tokenAmountComponentData.Symbol = shorten(data.Token, 3, 2)
							return nil
						}
						if imgUrl.Valid {
							tokenAmountComponentData.ImgUrl = imgUrl.String
						} else {
							// fetch from coingecko
						}
						return err
					})
				}

				transfersComponentData := eventTransfersComponentData{
					Wallet: shorten(data.Wallet, 4, 4),
					Tokens: []*eventTokenAmountComponentData{
						tokenAmountComponentData,
					},
					Fiats: []*eventFiatAmountComponentData{
						fiatAmountComponentData,
					},
				}

				switch data.Direction {
				case db.EventTransferIncoming:
					eventComponentData.IncomingTransfers = &transfersComponentData
				case db.EventTransferOutgoing:
					eventComponentData.OutgoingTransfers = &transfersComponentData
				}
			case *db.EventTransferInternal:
				// fromWallet = data.FromWallet
				// toWallet = data.ToWallet
			}

			br := pool.SendBatch(ctx, &getTokensMetaBatch)
			if err := br.Close(); err != nil {
				renderError(w, 500, "unable to query tokens meta: %s", err)
				return
			}

			eventTableRows = append(eventTableRows, eventTableRow{
				Type:  1,
				Event: eventComponentData,
			})
		}

		eventsPageContent := executeTemplateMust(
			templates,
			"events_page",
			eventTableRows,
		)

		page := executeTemplateMust(templates, "page_layout", pageLayoutComponentData{
			Content: template.HTML(eventsPageContent),
		})
		w.Write(page)
	}
}
