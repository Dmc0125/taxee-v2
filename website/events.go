package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"strconv"
	"taxee/pkg/assert"
	"taxee/pkg/coingecko"
	"taxee/pkg/db"
	"taxee/pkg/logger"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
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

	<div class="w-full mt-10 overflow-x-auto">
		<ul class="w-fit min-w-full flex flex-col gap-y-4 relative">
			<!-- header -->
			<!-- TODO: sticky -->
			<li class="
				w-full py-2 pl-4 top-2
				grid grid-cols-[150px_1fr]
				bg-gray-50 border border-gray-200 rounded-lg text-gray-800
			">
				<span>Date</span>
				<div class="w-full px-4 grid grid-cols-[300px_repeat(2,minmax(350px,1fr))_150px]">
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
	<div class="w-full border border-gray-200 rounded-lg">
		<div class="
			w-full px-4 py-2 grid grid-cols-[300px_repeat(2,minmax(350px,1fr))_150px]
		">
			<!-- App img, network img, event type, onchain method -->
			<div class="flex">
				<div class="w-10 h-10 rounded-full bg-gray-200 relative">
					<div class="
						w-5 h-5 p-0.5 absolute top-0 left-0
						rounded-full bg-gray-100 border border-gray-200
						overflow-hidden
					">
						<img 
							src="{{ .NetworkImgUrl }}" 
							class="w-full h-full"
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

		<!--
		<div class="
			w-full grid grid-cols-[repeat(3,1fr)]
			border-t border-gray-200
		">
			<div>
				<span>Missing prices</span> 
			</div>
			<div>
				<span>Missing prices</span>
			</div>
		</div>
		-->
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
<div class="w-full flex flex-col gap-y-1">
	<div class="w-full text-sm h-[1em]">
		<span class="text-gray-500 font-medium">
			{{ if . }}
				{{ .Wallet }}
			{{ end }}
		</span>
	</div>

	<div class="flex items-center mt-1">
		<!-- Token amounts -->
		<div class="w-1/2">
			{{ if . }}
				{{ range .Tokens }}
					{{ template "event_token_amount_component" . }}
				{{ end }}
			{{ else }}
				<div class="mt-1">-</div>
			{{ end }}
		</div>

		<!-- Fiat amounts -->
		<div>
			{{ if . }}
				{{ range .Fiats }}
					{{ template "event_fiat_amount_component" . }}
				{{ end }}
			{{ else }}
				<div class="mt-1">-</div>
			{{ end }}
		</div>
	</div>
</div>
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
	<div class="
		w-5 h-5 
		rounded-full overflow-hidden
	">
		{{ if .ImgUrl }}
			<img src="{{ .ImgUrl }}" class="w-5 h-5 rounded-full" />
		{{ else }}
			<div class="
				w-full h-full flex items-center justify-center
				text-sm text-gray-600 bg-gray-200
			">
				?
			</div>
		{{ end }}
	</div>

	<span class="text-gray-800 text-sm">{{ .Amount }} {{ upper .Symbol }}</span>
</div>
{{ end }}
`

type eventFiatAmountComponentData struct {
	Amount   string
	Currency string
	Zero     bool
	Missing  bool
	Sign     int
}

const eventFiatAmountComponent = `<!-- html -->
{{ define "event_fiat_amount_component" }}
<div class="h-5 flex items-center gap-x-2">
{{ if .Zero }}
	{{ if .Missing }}
		<div class="w-full flex items-center gap-x-1">
			<span class="
				w-5 h-5 rounded-full text-xs bg-red-700/20 text-red-700
				flex items-center justify-center flex-shrink-0
			">!</span>
			<span class="
				w-full px-2 py-1 text-sm text-red-800
			">Missing price</span>
		</div>
	{{ else }}
		<span class="text-gray-800 text-sm">-</span>
	{{ end }}
{{ else }}
	<span class="
		text-sm
		{{ tern (eq .Sign 0) "text-gray-800" (eq .Sign -1) "text-red-800" "text-green-800" }}
	">{{ .Amount }} {{ upper .Currency }}</span>
{{ end }}
</div>
{{ end }}
`

type fetchTokenMetadataQueued struct {
	coingeckoId string
	tokensData  []*eventTokenAmountComponentData
}

func eventsRenderTokenAmounts(
	amount,
	value,
	profit,
	price decimal.Decimal,
	token string,
	network db.Network,
	tokenSource uint16,
	getTokensMetaBatch *pgx.Batch,
	tokensQueue *[]*fetchTokenMetadataQueued,
) (tokenData *eventTokenAmountComponentData, fiatData, profitData *eventFiatAmountComponentData) {
	tokenData = &eventTokenAmountComponentData{
		Amount: amount.StringFixed(2),
	}
	fiatData = &eventFiatAmountComponentData{
		Amount:   value.StringFixed(2),
		Currency: "eur",
		Sign:     value.Sign(),
		Missing:  price.Equal(decimal.Zero),
		Zero:     value.Equal(decimal.Zero),
	}
	fmt.Printf("%#v\n", *fiatData)
	profitData = &eventFiatAmountComponentData{
		Amount:   profit.StringFixed(2),
		Currency: "eur",
		Sign:     profit.Sign(),
		Zero:     profit.Equal(decimal.Zero),
	}

	appendTokenToQueue := func(coingeckoId string) {
		contains := false
		for _, q := range *tokensQueue {
			if q.coingeckoId == coingeckoId {
				contains = true
				q.tokensData = append(q.tokensData, tokenData)
				break
			}
		}

		if !contains {
			*tokensQueue = append(*tokensQueue, &fetchTokenMetadataQueued{
				coingeckoId: coingeckoId,
				tokensData: []*eventTokenAmountComponentData{
					tokenData,
				},
			})
		}
	}

	if tokenSource == math.MaxUint16 {
		const getTokenMetaByCoingeckoId = `
			select
				symbol, image_url
			from
				coingecko_token_data 
			where
				coingecko_id = $1	
		`
		q := getTokensMetaBatch.Queue(
			getTokenMetaByCoingeckoId,
			token,
		)
		q.QueryRow(func(row pgx.Row) error {
			var imgUrl pgtype.Text
			err := row.Scan(
				&tokenData.Symbol,
				&imgUrl,
			)
			if err != nil {
				return err
			}
			if imgUrl.Valid {
				tokenData.ImgUrl = imgUrl.String
			} else {
				appendTokenToQueue(token)
			}
			return nil
		})
	} else {
		const getTokenMetaByAddressAndNetwork = `
			select
				ctd.symbol, ctd.image_url, ctd.coingecko_id
			from
				coingecko_token ct
			inner join
				coingecko_token_data ctd on
					ct.coingecko_id = ctd.coingecko_id
			where
				ct.address = $1 and ct.network = $2
		`
		q := getTokensMetaBatch.Queue(
			getTokenMetaByAddressAndNetwork,
			token,
			network,
		)
		q.QueryRow(func(row pgx.Row) error {
			var imgUrl pgtype.Text
			var coingeckoId string
			err := row.Scan(
				&tokenData.Symbol,
				&imgUrl,
				&coingeckoId,
			)
			if errors.Is(err, pgx.ErrNoRows) {
				tokenData.Symbol = shorten(token, 3, 2)
				return nil
			}
			if imgUrl.Valid {
				tokenData.ImgUrl = imgUrl.String
			} else {
				appendTokenToQueue(coingeckoId)
			}
			return err
		})
	}

	return
}

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
	loadTemplate(templates, eventFiatAmountComponent)

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
				e.data,
				(
					select
						coalesce(
							jsonb_agg(
								jsonb_build_object(
									'origin', perr.origin,
									'type', perr.type,
									'data', perr.data
								) order by perr.id asc
							),
							'[]'::jsonb
						)
					from
						parser_err perr
					where
						perr.tx_id = e.tx_id and
						perr.ix_idx = e.ix_idx and
						perr.user_account_id = e.user_account_id
				) as errors
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
			var eventData, errorsSerialized []byte
			e := new(event)
			if err := eventsRows.Scan(
				&e.TxId,
				&e.Network,
				&e.Timestamp,
				&e.Type,
				&e.UiAppName,
				&eventData,
				&errorsSerialized,
			); err != nil {
				renderError(w, 500, "unable to scan events: %s", err)
				return
			}

			if err := e.UnmarshalData(eventData); err != nil {
				renderError(w, 500, "unable to unmarshal event: %s", err)
				return
			}

			decoder := json.NewDecoder(bytes.NewBuffer(errorsSerialized))
			{
				token, err := decoder.Token()
				assert.NoErr(err, "empty errors json")
				if delim, ok := token.(json.Delim); ok {
					assert.True(delim == '[', "invalid errors json")
				} else {
					assert.True(false, "expected [")
				}
			}

			properties := 0
			var (
				parserErrorData   json.RawMessage
				parserErrorOrigin db.ErrOrigin
				parserErrorType   db.ParserErrorType
			)

			for decoder.More() {
				token, err := decoder.Token()
				assert.NoErr(err, "invalid errors json")

				if delim, ok := token.(json.Delim); ok {
					switch delim {
					case ']':
						break
					default:
						continue
					}
				}

				ident, ok := token.(string)
				if ok {
					var err error
					switch ident {
					case "origin":
						err = decoder.Decode(&parserErrorOrigin)
						properties += 1
					case "type":
						err = decoder.Decode(&parserErrorType)
						properties += 1
					case "data":
						err = decoder.Decode(&parserErrorData)
						properties += 1
					}
					assert.NoErr(err, fmt.Sprintf("unable to decode: %s", ident))

					if properties == 3 {
						// done
						fmt.Println("Done")
					}
				}
			}

			events = append(events, e)
		}

		/////////////
		// render events
		eventTableRows := make([]eventTableRow, 0)
		prevDateUnix := int64(0)

		getTokensMetaBatch := pgx.Batch{}
		fetchTokensMetadataQueue := make([]*fetchTokenMetadataQueued, 0)

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

			switch data := event.Data.(type) {
			case *db.EventTransfer:
				tokenData, fiatData, profitData := eventsRenderTokenAmounts(
					data.Amount,
					data.Value,
					data.Profit,
					data.Price,
					data.Token,
					event.Network,
					data.TokenSource,
					&getTokensMetaBatch,
					&fetchTokensMetadataQueue,
				)

				eventComponentData.Profits = append(
					eventComponentData.Profits,
					profitData,
				)
				transfersComponentData := eventTransfersComponentData{
					Wallet: shorten(data.Wallet, 4, 4),
					Tokens: []*eventTokenAmountComponentData{
						tokenData,
					},
					Fiats: []*eventFiatAmountComponentData{
						fiatData,
					},
				}

				switch data.Direction {
				case db.EventTransferIncoming:
					eventComponentData.IncomingTransfers = &transfersComponentData
				case db.EventTransferOutgoing:
					eventComponentData.OutgoingTransfers = &transfersComponentData
				}
			case *db.EventTransferInternal:
				// NOTE: profit can exist event on internal transfers
				// in case of missing balances
				tokenData, fiatData, profitData := eventsRenderTokenAmounts(
					data.Amount,
					data.Value,
					data.Profit,
					data.Price,
					data.Token,
					event.Network,
					data.TokenSource,
					&getTokensMetaBatch,
					&fetchTokensMetadataQueue,
				)
				fiatData.Sign = 0

				eventComponentData.Profits = append(
					eventComponentData.Profits,
					profitData,
				)

				eventComponentData.OutgoingTransfers = &eventTransfersComponentData{
					Wallet: shorten(data.FromWallet, 4, 4),
					Tokens: []*eventTokenAmountComponentData{tokenData},
					Fiats:  []*eventFiatAmountComponentData{fiatData},
				}
				eventComponentData.IncomingTransfers = &eventTransfersComponentData{
					Wallet: shorten(data.ToWallet, 4, 4),
					Tokens: []*eventTokenAmountComponentData{tokenData},
					Fiats:  []*eventFiatAmountComponentData{fiatData},
				}
			}

			eventTableRows = append(eventTableRows, eventTableRow{
				Type:  1,
				Event: eventComponentData,
			})
		}

		///////////////
		// execute network stuff

		br := pool.SendBatch(ctx, &getTokensMetaBatch)
		if err := br.Close(); err != nil {
			renderError(w, 500, "unable to query tokens meta: %s", err)
			return
		}

		// TODO: this should not be done like this but after the html was sent
		// via server sent events or something
		insertTokenImgUrlBatch := pgx.Batch{}
		logger.Info("Fetching tokens: %d", len(fetchTokensMetadataQueue))
		for _, queued := range fetchTokensMetadataQueue {
			meta, err := coingecko.GetCoinMetadata(queued.coingeckoId)
			if err == nil {
				for _, tokenData := range queued.tokensData {
					tokenData.ImgUrl = meta.Image.Small
				}

				const insertTokenImgUrl = `
					update coingecko_token_data set
						image_url = $1
					where
						coingecko_id = $2
				`
				insertTokenImgUrlBatch.Queue(
					insertTokenImgUrl,
					meta.Image.Small,
					queued.coingeckoId,
				)
			}
		}

		br = pool.SendBatch(ctx, &insertTokenImgUrlBatch)
		if err := br.Close(); err != nil {
			renderError(w, 500, "unable to insert token img urls: %s", err)
			return
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
