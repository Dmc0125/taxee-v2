package main

import (
	"context"
	"encoding/hex"
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

type eventTableRowComponentData struct {
	Type uint8

	Timestamp   time.Time
	TxId        string
	ExplorerUrl string

	Events []*eventComponentData

	HasErrors        bool
	PreprocessErrors eventErrorGroupComponentData
	ProcessErrors    eventErrorGroupComponentData
}

type eventsPageData struct {
	NextUrl string
	Rows    []eventTableRowComponentData
}

const eventsPage = `<!-- html -->
{{ define "events_page" }}
<div class="w-[80%] mx-auto mt-10">
	<header class="w-full flex items-center justify-between">
		<h1 class="font-semibold text-2xl">Events</h1>
		<a href="{{ .NextUrl }}" class="">
			Next
		</a>
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

			{{ range .Rows }}
				{{ if eq .Type 0 }}
					<li class="w-full py-1 px-4 bg-crossed rounded-lg">
						<span class="text-sm text-gray-800">{{ formatDate .Timestamp }}</span>
					</li>
				{{ else }}
					<!-- Timestamp + explorer -->
					<li class="w-full pl-4 grid grid-cols-[150px_1fr] gap-y-4">
						<div class="flex flex-col col-[1/2]">
							<a
								href="{{ .ExplorerUrl }}"
								target="_blank"
								class="text-gray-800"
							>{{ .TxId }}</a>
							<span class="text-sm text-gray-700">{{ formatTime .Timestamp }}</span>
						</div>

						<!-- event data -->
						
						{{ if .Events }}
							{{ range .Events }}
								<div class="
									w-full col-[2/-1] border border-gray-200 rounded-lg
								">
									{{ template "event_component" . }}
								</div>
							{{ end }}
						{{ else }}
							<div class="
								w-full col-[2/-1] min-h-14
								flex items-center justify-center 
								border border-gray-200 rounded-lg text-gray-600
							">
								This transaction does not have any events	
							</div>
						{{ end }}

						{{ if .HasErrors }}
							<div class="
								w-full col-[2/-1] grid grid-cols-[repeat(2,1fr)]
								border border-gray-200 rounded-lg 
							">
								{{ template "event_error_group_component" .PreprocessErrors }}
								{{ template "event_error_group_component" .ProcessErrors }}
							</div>
						{{ end }}
					</li>
				{{ end }}
			{{ end }}
		</ul>
	</div>
</div>
{{ end }}
`

type eventComponentData struct {
	NetworkImgUrl string
	EventType     string
	Method        string

	OutgoingTransfers *eventTransfersComponentData
	IncomingTransfers *eventTransfersComponentData
	Profits           []*eventFiatAmountComponentData
}

const eventComponent = `<!-- html -->
{{ define "event_component" }}
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
{{ end }}
`

type eventErrorComponentData struct {
	Address  string
	Type     int
	Had      any
	Expected any
}

type eventErrorGroupComponentData struct {
	Type   int
	Errors []eventErrorComponentData
}

const eventErrorGroupComponent = `<!-- html -->
{{ define "event_error_group_component" }}
<div class="px-4 py-4 flex flex-col border-r border-gray-200">
	<div class="w-full flex items-center justify-between">
		<span class="text-gray-600 text-sm">
		{{ if eq .Type 0 }}
			Preprocess errors 
		{{ else }}
			Process errors
		{{ end }}
		</span>

		{{ if .Errors }}
			<span class="
				w-5 h-5 rounded-full text-xs bg-red-700/20 text-red-700
				flex items-center justify-center flex-shrink-0
			">{{ len .Errors }}</span>
		{{ end }}
	</div> 

	{{ if .Errors }}
		<ul class="
			w-full mt-2 gap-y-4
			grid grid-cols-[20%_repeat(3,1fr)]
		">
			<li class="
				w-full py-1 col-[1/-1] grid grid-cols-subgrid
				text-gray-600 border-b border-gray-200 text-sm
			">
				<span>Account</span>
				<span>Type</span>
				<span>Real</span>
				<span>Expected</span>
			</li>
			{{ range .Errors }}
				<li class="col-[1/-1] grid grid-cols-subgrid text-gray-800 text-sm">
					<span>{{ shorten .Address 4 4 }} </span>
					{{ if eq .Type 0 }}
						<span>Missing account</span>
						<span>-</span>
						<span>-</span>
					{{ else if eq .Type 1 }}
						<span>Balance mismatch</span>
						<span>{{ .Had }}</span>
						<span>{{ .Expected }}</span>
					{{ else if eq .Type 2 }}
						<span>Data mismatch</span>
						<span>{{ .Had }}</span>
						<span>{{ .Expected }}</span>
					{{ end }}
				</li>
			{{ end }}
		</ul>
	{{ else }}
		<div class="w-full h-full mt-2 flex items-center justify-center">
			<span class="text-gray-600">
				No errors of this kind
			</span>
		</div>
	{{ end }}
</div>
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

type eventsPagination struct {
	SolanaSlot         int64
	SolanaBlockIndex   int32
	ArbitrumBlock      int64
	ArbitrumBlockIndex int32
}

func (p *eventsPagination) serialize() string {
	serialized, err := json.Marshal(p)
	assert.NoErr(err, "unable to serialize events pagination")
	return hex.EncodeToString(serialized)
}

func (p *eventsPagination) deserialize(src []byte) error {
	parsed, err := hex.DecodeString(string(src))
	if err != nil {
		return fmt.Errorf("unable to parse pagination: %w", err)
	}
	err = json.Unmarshal(parsed, p)
	if err != nil {
		return fmt.Errorf("unable to deserialize pagination: %w", err)
	}
	return nil
}

func eventsHandler(
	ctx context.Context,
	pool *pgxpool.Pool,
	templates *template.Template,
) http.HandlerFunc {
	loadTemplate(templates, eventsPage)
	loadTemplate(templates, eventComponent)
	loadTemplate(templates, eventTransfersComponent)
	loadTemplate(templates, eventTokenAmountComponent)
	loadTemplate(templates, eventFiatAmountComponent)
	loadTemplate(templates, eventErrorGroupComponent)

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

		var (
			limit = int32(50)

			offset        = query.Get("offset")
			limitQueryVal = query.Get("limit")
			txId          = query.Get("tx_id")
		)

		if v, err := strconv.Atoi(limitQueryVal); err == nil {
			limit = int32(v)
		}

		var eventsOffset eventsPagination
		if offset != "" {
			if err := eventsOffset.deserialize([]byte(offset)); err != nil {
				renderError(w, 400, "invalid pagination: %s", err)
				return
			}
		}

		/////////////
		// get events

		const getEventsQuerySelect = `
			select
				tx.id,
				tx.network,
				tx.timestamp,
				(tx.data->>'slot')::bigint as solana_slot,
				(tx.data->>'blockIndex')::integer as solana_block_index,
				(tx.data->>'block')::bigint as evm_block,
				(tx.data->>'txIdx')::integer as evm_block_index,
				coalesce(
					jsonb_agg(
						jsonb_build_object(
							'IxIdx', e.ix_idx,
							'UiAppName', e.ui_app_name,
							'UiMethodName', e.ui_method_name,
							'Type', e.type,
							'Data', e.data
						) order by e.idx asc
					) filter (where e.tx_id is not null),
					'[]'::jsonb
				) as events,
				coalesce(
					jsonb_agg(
						jsonb_build_object(
							'IxIdx', pe.ix_idx,
							'EventId', pe.event_idx,
							'Origin', pe.origin,
							'Type', pe.type,
							'Data', pe.data
						) order by pe.origin asc
					) filter (where pe.id is not null),
					'[]'::jsonb
				) as errors
			from
				tx_ref
			inner join
				tx on tx.id = tx_ref.tx_id
			left join
				event e on 
					e.tx_id = tx_ref.tx_id and 
					e.user_account_id = tx_ref.user_account_id
			left join
				parser_error pe on
					pe.tx_id = tx_ref.tx_id and
					pe.user_account_id = tx_ref.user_account_id
		`
		const getEventsQuerySelectAfter = `
			group by
				tx.id
			order by
				-- global ordering
				tx.timestamp asc,
				-- network specific ordering in case of timestamps conflicts
				-- solana 
				(tx.data->>'slot')::bigint asc,
				(tx.data->>'blockIndex')::integer asc,
				-- evm
				(tx.data->>'block')::bigint asc,
				(tx.data->>'txIdx')::integer asc
		`

		var eventsRows pgx.Rows
		var err error

		switch {
		case txId != "":
			getEventByTxId := fmt.Sprintf(`
					%s
					where
						tx_ref.user_account_id = $1 and tx.id = $2
					%s
				`,
				getEventsQuerySelect,
				getEventsQuerySelectAfter,
			)
			eventsRows, err = pool.Query(
				ctx,
				getEventByTxId,
				userAccountId,
				txId,
			)
		default:
			getEventsQuery := fmt.Sprintf(`
					%s
					where
						tx_ref.user_account_id = $1 and
						(e.tx_id is not null or pe.id is not null) and

						-- pagination
						(
							(
								tx.network = 'solana' and (
									(tx.data->>'slot')::bigint > $3 or (
										(tx.data->>'slot')::bigint = $3 and 
										(tx.data->>'blockIndex')::integer > $4
									)
								)
							) or (
								tx.network = 'arbitrum' and (
									(tx.data->>'block')::bigint > $5 or (
										(tx.data->>'block')::bigint = $5 and
										(tx.data->>'txIdx')::integer > $6
									)
								)
							)
						)
					%s
					limit
						$2
				`,
				getEventsQuerySelect,
				getEventsQuerySelectAfter,
			)
			eventsRows, err = pool.Query(
				ctx,
				getEventsQuery,
				// args
				userAccountId,
				limit,
				eventsOffset.SolanaSlot, eventsOffset.SolanaBlockIndex,
				eventsOffset.ArbitrumBlock, eventsOffset.ArbitrumBlockIndex,
			)
		}

		if err != nil {
			renderError(w, 500, "unable to query txs: %s", err)
			return
		}

		eventsTableRows := make([]eventTableRowComponentData, 0)
		getTokensMetaBatch := pgx.Batch{}
		fetchTokensMetadataQueue := make([]*fetchTokenMetadataQueued, 0)
		var prevDateUnix int64

		for eventsRows.Next() {
			var (
				txId                                   string
				network                                db.Network
				timestamp                              time.Time
				solanaSlot, evmBlock                   pgtype.Int8
				solanaBlockIndex, evmBlockIndex        pgtype.Int4
				eventsMarshaled, parserErrorsMarshaled json.RawMessage
			)

			if err := eventsRows.Scan(
				&txId, &network, &timestamp,
				&solanaSlot, &solanaBlockIndex,
				&evmBlock, &evmBlockIndex,
				&eventsMarshaled, &parserErrorsMarshaled,
			); err != nil {
				renderError(w, 500, "unable to scan txs: %s", err)
				return
			}

			switch network {
			case db.NetworkSolana:
				eventsOffset.SolanaSlot = solanaSlot.Int64
				eventsOffset.SolanaBlockIndex = solanaBlockIndex.Int32
			case db.NetworkArbitrum:
				eventsOffset.ArbitrumBlock = evmBlock.Int64
				eventsOffset.ArbitrumBlockIndex = evmBlockIndex.Int32
			}

			const daySecs = 60 * 60 * 24
			if dateUnix := timestamp.Unix() / daySecs * daySecs; prevDateUnix != dateUnix {
				prevDateUnix = dateUnix
				eventsTableRows = append(
					eventsTableRows,
					eventTableRowComponentData{
						Type:      0,
						Timestamp: timestamp,
					},
				)
			}

			networkGlobals, ok := networksGlobals[network]
			assert.True(ok, "missing globals for network: %s", network.String())

			rowData := eventTableRowComponentData{
				Type:      1,
				Timestamp: timestamp,
				TxId: fmt.Sprintf(
					"%s...%s",
					txId[:5], txId[len(txId)-2:],
				),
				ExplorerUrl: fmt.Sprintf(
					"%s/tx/%s",
					networkGlobals.explorerUrl, txId,
				),
				Events: make([]*eventComponentData, 0),
				ProcessErrors: eventErrorGroupComponentData{
					Type: 1,
				},
			}

			// NOTE: '[]' if empty
			if len(eventsMarshaled) != 2 {
				var events []*struct {
					*db.Event
					SerializedData json.RawMessage `json:"Data"`
				}

				if err := json.Unmarshal(eventsMarshaled, &events); err != nil {
					renderError(w, 500, "unable to unmarshal events: %s", err)
					return
				}

				for _, e := range events {
					if err := e.UnmarshalData(e.SerializedData); err != nil {
						renderError(w, 500, "unmable to unmarshal event data: %s", err)
						return
					}

					eventComponentData := &eventComponentData{
						NetworkImgUrl: networkGlobals.imgUrl,
						EventType:     toTitle(string(e.Type)),
						Method:        toTitle(string(e.UiAppName)),
					}
					rowData.Events = append(rowData.Events, eventComponentData)

					///////////////
					// transfers

					switch data := e.Data.(type) {
					case *db.EventTransfer:
						tokenData, fiatData, profitData := eventsRenderTokenAmounts(
							data.Amount,
							data.Value,
							data.Profit,
							data.Price,
							data.Token,
							network,
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
							network,
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
				}
			}

			if len(parserErrorsMarshaled) != 2 {
				var errors []*struct {
					IxIdx   int32
					EventId pgtype.Int4
					Origin  db.ErrOrigin
					Type    db.ParserErrorType
					Data    json.RawMessage
				}
				if err := json.Unmarshal(parserErrorsMarshaled, &errors); err != nil {
					renderError(w, 500, "unable to unmarshal parser errors: %s", err)
					return
				}

				rowData.HasErrors = true

				for _, err := range errors {
					var errorComponentData eventErrorComponentData
					errorComponentData.Type = int(err.Type)

					switch err.Type {
					case db.ParserErrorTypeMissingAccount:
						var data db.ParserErrorMissingAccount
						if err := json.Unmarshal(err.Data, &data); err != nil {
							renderError(w, 500, "unable to unmarshal parser error: %s", err)
							return
						}
						errorComponentData.Address = data.AccountAddress
					case db.ParserErrorTypeAccountBalanceMismatch:
						var data db.ParserErrorAccountBalanceMismatch
						if err := json.Unmarshal(err.Data, &data); err != nil {
							renderError(w, 500, "unable to unmarshal parser error: %s", err)
							return
						}
						errorComponentData.Address = data.AccountAddress
						errorComponentData.Had = data.Real.StringFixed(2)
						errorComponentData.Expected = data.Expected.StringFixed(2)
					case db.ParserErrorTypeAccountDataMismatch:
						var data db.ParserErrorMissingAccount
						if err := json.Unmarshal(err.Data, &data); err != nil {
							renderError(w, 500, "unable to unmarshal parser error: %s", err)
							return
						}
						errorComponentData.Address = data.AccountAddress
					default:
						assert.True(false, "invalid parser error: %d", err.Type)
					}

					switch err.Origin {
					case db.ErrOriginPreprocess:
						rowData.PreprocessErrors.Errors = append(
							rowData.PreprocessErrors.Errors,
							errorComponentData,
						)
					case db.ErrOriginProcess:
						rowData.ProcessErrors.Errors = append(
							rowData.ProcessErrors.Errors,
							errorComponentData,
						)
					}
				}
			}

			eventsTableRows = append(eventsTableRows, rowData)
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

		eventsPageData := eventsPageData{
			NextUrl: fmt.Sprintf("/events?offset=%s", eventsOffset.serialize()),
			Rows:    eventsTableRows,
		}

		eventsPageContent := executeTemplateMust(
			templates,
			"events_page",
			eventsPageData,
		)

		page := executeTemplateMust(templates, "page_layout", pageLayoutComponentData{
			Content: template.HTML(eventsPageContent),
		})
		w.Write(page)
	}
}
