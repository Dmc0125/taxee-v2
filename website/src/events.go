package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"strconv"
	"strings"
	"taxee/pkg/assert"
	"taxee/pkg/db"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

type networkGlobals struct {
	explorerUrl        string
	explorerAccountUrl string
	imgUrl             string
}

var networksGlobals = map[db.Network]networkGlobals{
	db.NetworkSolana: {
		explorerUrl:        "https://solscan.io",
		explorerAccountUrl: "https://solscan.io/account",
		imgUrl:             "/static/logo_solana.svg",
	},
	db.NetworkArbitrum: {
		explorerUrl:        "https://arbiscan.io",
		explorerAccountUrl: "https://arbiscan.io/address",
		imgUrl:             "/static/logo_arbitrum.svg",
	},
}

var appImgUrls = map[string]string{
	"0-native":           "/static/logo_solana.svg",
	"0-system":           "/static/logo_solana.svg",
	"0-token":            "/static/logo_solana.svg",
	"0-associated_token": "/static/logo_solana.svg",
	"0-meteora_pools":    "/static/logo_meteora.svg",
	"0-meteora_farms":    "/static/logo_meteora.svg",
	"0-jupiter":          "/static/logo_jupiter.svg",
	"0-jupiter_dca":      "/static/logo_jupiter.svg",
	"0-drift":            "/static/logo_drift.svg",
	"3-native":           "/static/logo_arbitrum.svg",
}

type eventTokenImgComponentData struct {
	ImgUrl      string
	CoingeckoId string
}

type eventTokenAmountComponentData struct {
	Account    string
	ImgData    eventTokenImgComponentData
	Amount     string
	Symbol     string
	LongAmount string
}

type eventFiatAmountComponentData struct {
	Amount   string
	Currency string
	Price    string
	Zero     bool
	Missing  bool
	Sign     int
	IsProfit bool
}

type eventTransfersComponentData struct {
	Wallet string
	Tokens []*eventTokenAmountComponentData
	Fiats  []*eventFiatAmountComponentData
}

type eventTransferSourceComponentData struct {
	EventId   uuid.UUID
	Timestamp time.Time
	Method    string

	TokenImgData eventTokenImgComponentData
	Token        string

	TokenAmount     string
	UsedTokenAmount string
}

type eventComponentData struct {
	Id        uuid.UUID
	Timestamp time.Time
	Sources   []*db.EventTransferSource

	// UI
	Native        bool
	NetworkImgUrl string
	Method        string
	App           string
	AppImgUrl     string

	OutgoingTransfers *eventTransfersComponentData
	IncomingTransfers *eventTransfersComponentData
	Profits           []*eventFiatAmountComponentData

	SourcesUi []*eventTransferSourceComponentData

	MissingBalances []*eventTokenAmountComponentData
}

type eventErrorComponentData struct {
	Wallet  string
	Account string
	IxIdx   int

	ImgData            eventTokenImgComponentData
	Token              string
	LocalZero          bool
	LocalAmount        string
	LocalAmountLong    string
	ExternalZero       bool
	ExternalAmount     string
	ExternalAmountLong string

	Message string
}

type eventErrorGroupComponentData struct {
	Type                 int
	Empty                bool
	MissingAccounts      []*eventErrorComponentData
	BalanceMismatches    []*eventErrorComponentData
	DataMismatches       []*eventErrorComponentData
	MissingSwapTransfers []int
}

type eventsTableRowComponentData struct {
	Type uint8

	Timestamp     time.Time
	TxId          string
	ExplorerUrl   string
	NetworkImgUrl string

	Events []*eventComponentData

	HasErrors        bool
	PreprocessErrors eventErrorGroupComponentData
	ProcessErrors    eventErrorGroupComponentData
}

type eventsProgressIndicatorComponentData struct {
	Done      bool
	RequestId int32
	Status    string
}

type eventsPageData struct {
	ProgressIndicator eventsProgressIndicatorComponentData
	NextUrl           string
	Rows              []eventsTableRowComponentData
}

func getTokenSymbolAndImg(
	token string,
	tokenSource uint16,
	network db.Network,
	symbolPtr *string,
	imgDataPtr *eventTokenImgComponentData,
	getTokensMetaBatch *pgx.Batch,
) {
	switch tokenSource {
	case math.MaxUint16:
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
				symbolPtr,
				&imgUrl,
			)
			if err != nil {
				return err
			}
			if imgUrl.Valid {
				imgDataPtr.ImgUrl = imgUrl.String
			} else {
				imgDataPtr.CoingeckoId = token
			}
			return nil
		})
	default:
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
				symbolPtr,
				&imgUrl,
				&coingeckoId,
			)
			if errors.Is(err, pgx.ErrNoRows) {
				*symbolPtr = shorten(token, 3, 2)
				return nil
			}
			if imgUrl.Valid {
				imgDataPtr.ImgUrl = imgUrl.String
			} else {
				imgDataPtr.CoingeckoId = coingeckoId
			}
			return err
		})
	}
}

func eventsCreateTokenAmountData(
	amount decimal.Decimal,
	token, account string,
	network db.Network,
	tokenSource uint16,
	getTokensMetaBatch *pgx.Batch,
) *eventTokenAmountComponentData {
	tokenData := &eventTokenAmountComponentData{
		Account:    account,
		Amount:     amount.StringFixed(2),
		LongAmount: amount.String(),
	}
	getTokenSymbolAndImg(
		token, tokenSource, network,
		&tokenData.Symbol, &tokenData.ImgData,
		getTokensMetaBatch,
	)
	return tokenData
}

func eventsCreateFiatAmountsData(
	transfer *db.EventTransfer,
) (fiatData, profitData *eventFiatAmountComponentData) {
	fiatData = &eventFiatAmountComponentData{
		Amount:   transfer.Value.StringFixed(2),
		Currency: "eur",
		Price:    transfer.Price.StringFixed(2),
		Sign:     transfer.Value.Sign(),
		Missing:  transfer.Price.Equal(decimal.Zero),
		Zero:     transfer.Value.Equal(decimal.Zero),
	}
	profitData = &eventFiatAmountComponentData{
		Amount:   transfer.Profit.StringFixed(2),
		Currency: "eur",
		Sign:     transfer.Profit.Sign(),
		Zero:     transfer.Profit.Equal(decimal.Zero),
		IsProfit: true,
	}
	return
}

type urlParam[T any] struct {
	value T
	set   bool
}

func buildGetEventsQuery(
	userAccountId int32,
	eventId uuid.UUID,
	txId string,
	limit int32,
	fromItxPosition float32,
	devMode bool,
) (q string, params []any) {
	const getEventsQuerySelect = `
		select
			itx.tx_id, 
			itx.network, 
			itx.timestamp,
			itx.position,
			coalesce(events.events, '[]'::json) as events,
			coalesce(errors.errors,	'[]'::json) as errors
		from
			internal_tx itx
		left join lateral (
			select json_agg(
				json_build_object(
					'Id', e.id,
					'App', e.app,
					'Method', e.method,
					'Type', e.type,
					'Transfers', (
						select json_agg(
							json_build_object(
								'id', et.id,
								'direction', et.direction,
								'fromWallet', et.from_wallet,
								'fromAccount', et.from_account,
								'toWallet', et.to_wallet,
								'toAccount', et.to_account,
								'token', et.token,
								'amount', et.amount,
								'tokenSource', et.token_source,
								'price', et.price,
								'value', et.value,
								'profit', et.profit,
								'missingAmount', et.missing_amount,
								'sources', (
									select json_agg(
										json_build_object(
											'transferId', ts.source_transfer_id,
											'usedAmount', ts.used_amount
										)
									) 
									from
										event_transfer_source ts
									where
										ts.transfer_id = et.id
								)
							) order by et.position asc
						)
						from
							event_transfer et
						where
							et.event_id = e.id
					)
				) order by e.position asc
			) as events 
			from 
				event e
			where
				e.internal_tx_id = itx.id
		) events on true
		left join lateral (
			select json_agg(
		 		json_build_object(
		 			'IxIdx', pe.ix_idx,
		 			'Origin', pe.origin,
		 			'Type', pe.type,
		 			'Data', pe.data
		 		) order by pe.origin asc
		 	) as errors
		 	from 
		 		parser_error pe
		 	where 
		 		pe.internal_tx_id = itx.id
		 ) errors on true
	`
	const getEventsQuerySelectAfter = "order by itx.position asc"

	var filterEmptyTxs string
	if !devMode {
		filterEmptyTxs = "and (events.events is not null or errors.errors is not null)"
	}

	switch {
	case eventId != uuid.Nil:
		q = fmt.Sprintf(
			`
				%s
				where 
					exists (
						select 1 from event e where e.id = $2 and e.internal_tx_id = itx.id
					) and
					itx.user_account_id = $1
			`,
			getEventsQuerySelect,
		)
		params = append(params, userAccountId, eventId)
	case txId != "":
		q = fmt.Sprintf(
			"%s where itx.user_account_id = $1 and itx.tx_id = $2",
			getEventsQuerySelect,
		)
		params = append(params, userAccountId, txId)
	default:
		q = fmt.Sprintf(`
				%s
				where
					itx.user_account_id = $1 and
					itx.position > $2 
					%s
				%s
				limit
					$3
			`,
			getEventsQuerySelect,
			filterEmptyTxs,
			getEventsQuerySelectAfter,
		)
		params = append(params, userAccountId, fromItxPosition, limit)
	}

	return
}

func renderEvents(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
	eventId uuid.UUID,
	txId string,
	limit int32,
	offset float32,
	devMode bool,
) ([]eventsTableRowComponentData, float32, error) {
	getEventsQuery, getEventsParams := buildGetEventsQuery(
		userAccountId,
		eventId,
		txId,
		limit,
		offset,
		devMode,
	)
	transactionsRows, err := pool.Query(ctx, getEventsQuery, getEventsParams...)

	if err != nil {
		return nil, 0, fmt.Errorf("unable to query txs: %w", err)
	}

	eventsTableRows := make([]eventsTableRowComponentData, 0)

	type eventTransferWrapped struct {
		*db.EventTransfer
		EventId     uuid.UUID
		Timestamp   time.Time
		Network     db.Network
		EventMethod string
	}
	transfers := make(map[uuid.UUID]eventTransferWrapped)

	getTokensMetaBatch := pgx.Batch{}
	var prevDateUnix int64
	var lastItxPosition float32

	for transactionsRows.Next() {
		var (
			txId                                   string
			network                                db.Network
			timestamp                              time.Time
			eventsMarshaled, parserErrorsMarshaled json.RawMessage
		)

		if err := transactionsRows.Scan(
			&txId, &network, &timestamp, &lastItxPosition,
			&eventsMarshaled, &parserErrorsMarshaled,
		); err != nil {
			return nil, 0, fmt.Errorf("unable to scan txs: %w", err)
		}

		const daySecs = 60 * 60 * 24
		if dateUnix := timestamp.Unix() / daySecs * daySecs; prevDateUnix != dateUnix {
			prevDateUnix = dateUnix
			eventsTableRows = append(
				eventsTableRows,
				eventsTableRowComponentData{
					Type:      0,
					Timestamp: timestamp,
				},
			)
		}

		networkGlobals, ok := networksGlobals[network]
		assert.True(ok, "missing globals for network: %s", network.String())

		rowData := eventsTableRowComponentData{
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
			NetworkImgUrl: networkGlobals.imgUrl,
			Events:        make([]*eventComponentData, 0),
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
				return nil, 0, fmt.Errorf("unable to unmarshal events: %w", err)
			}

			for _, e := range events {
				appImgUrl := appImgUrls[fmt.Sprintf("%d-%s", network, e.App)]
				var native bool
				switch e.App {
				case "native", "system", "token", "associated_token":
					native = true
				}

				eventComponentData := eventComponentData{
					Id:        e.Id,
					Timestamp: timestamp,

					Native:          native,
					NetworkImgUrl:   networkGlobals.imgUrl,
					Method:          e.Method,
					App:             e.App,
					AppImgUrl:       appImgUrl,
					MissingBalances: make([]*eventTokenAmountComponentData, 0),
				}
				rowData.Events = append(rowData.Events, &eventComponentData)

				///////////////
				// transfers

				outgoingTransfers := new(eventTransfersComponentData)
				incomingTransfers := new(eventTransfersComponentData)
				eventComponentData.OutgoingTransfers = outgoingTransfers
				eventComponentData.IncomingTransfers = incomingTransfers

				for _, t := range e.Transfers {
					eventComponentData.Sources = append(
						eventComponentData.Sources,
						t.Sources...,
					)
					transfers[t.Id] = eventTransferWrapped{
						EventTransfer: t,
						EventId:       e.Id,
						Timestamp:     timestamp,
						Network:       network,
						EventMethod:   e.Method,
					}

					fiatData, profitData := eventsCreateFiatAmountsData(t)

					if t.MissingAmount.GreaterThan(decimal.Zero) {
						tokenData := &eventTokenAmountComponentData{
							Account:    t.FromAccount,
							Amount:     t.MissingAmount.String(),
							LongAmount: t.MissingAmount.String(),
						}
						getTokenSymbolAndImg(
							t.Token, t.TokenSource, network,
							&tokenData.Symbol, &tokenData.ImgData,
							&getTokensMetaBatch,
						)
						eventComponentData.MissingBalances = append(
							eventComponentData.MissingBalances,
							tokenData,
						)
					}

					if t.Direction == db.EventTransferInternal {
						eventComponentData.Profits = append(eventComponentData.Profits, profitData)

						outgoingTransfers.Wallet = shorten(t.FromWallet, 4, 4)
						incomingTransfers.Wallet = shorten(t.ToWallet, 4, 4)

						fromTokenData := eventsCreateTokenAmountData(
							t.Amount, t.Token, t.FromAccount,
							network, t.TokenSource,
							&getTokensMetaBatch,
						)
						outgoingTransfers.Tokens = append(outgoingTransfers.Tokens, fromTokenData)
						toTokenData := eventsCreateTokenAmountData(
							t.Amount, t.Token, t.ToAccount,
							network, t.TokenSource,
							&getTokensMetaBatch,
						)
						incomingTransfers.Tokens = append(incomingTransfers.Tokens, toTokenData)

						fiatData.Sign = 0
						fiatData.Missing = false
						outgoingTransfers.Fiats = append(outgoingTransfers.Fiats, fiatData)
						incomingTransfers.Fiats = append(incomingTransfers.Fiats, fiatData)
					} else {
						var account, wallet string
						var transfers *eventTransfersComponentData

						switch t.Direction {
						case db.EventTransferIncoming:
							account, wallet = t.ToAccount, t.ToWallet
							transfers = incomingTransfers

							if e.Type < db.EventTypeSwapBr {
								eventComponentData.Profits = append(
									eventComponentData.Profits,
									profitData,
								)
							}
						case db.EventTransferOutgoing:
							account, wallet = t.FromAccount, t.FromWallet
							transfers = outgoingTransfers

							if e.Type > db.EventTypeSwapBr {
								eventComponentData.Profits = append(
									eventComponentData.Profits,
									profitData,
								)
							}
						}

						tokenData := eventsCreateTokenAmountData(
							t.Amount,
							t.Token, account,
							network,
							t.TokenSource,
							&getTokensMetaBatch,
						)

						transfers.Wallet = shorten(wallet, 4, 4)
						transfers.Tokens = append(transfers.Tokens, tokenData)
						transfers.Fiats = append(transfers.Fiats, fiatData)
					}
				}
			}
		}

		if len(parserErrorsMarshaled) != 2 {
			var errors []*struct {
				IxIdx  int32
				Origin db.ErrOrigin
				Type   db.ParserErrorType
				Data   json.RawMessage
			}
			if err := json.Unmarshal(parserErrorsMarshaled, &errors); err != nil {
				return nil, 0, fmt.Errorf("unable to unmarshal parser errors: %w", err)
			}

			for _, err := range errors {
				rowData.HasErrors = true

				var errors *eventErrorGroupComponentData
				switch err.Origin {
				case db.ErrOriginPreprocess:
					errors = &rowData.PreprocessErrors
				case db.ErrOriginProcess:
					errors = &rowData.ProcessErrors
				default:
					assert.True(false, "invalid error origin: %d", err.Origin)
				}

				errors.Empty = false

				switch err.Type {
				case db.ParserErrorTypeOneSidedSwap:
					errors.MissingSwapTransfers = append(errors.MissingSwapTransfers, int(err.IxIdx))
				case db.ParserErrorTypeMissingAccount:
					var data db.ParserErrorMissingAccount
					if err := json.Unmarshal(err.Data, &data); err != nil {
						return nil, 0, fmt.Errorf("unable to unmarshal parser error: %w", err)
					}

					errors.MissingAccounts = append(
						errors.MissingAccounts,
						&eventErrorComponentData{
							Account: data.AccountAddress,
							IxIdx:   int(err.IxIdx),
						},
					)
				case db.ParserErrorTypeAccountBalanceMismatch:
					var data db.ParserErrorAccountBalanceMismatch
					if err := json.Unmarshal(err.Data, &data); err != nil {
						return nil, 0, fmt.Errorf("unable to unmarshal parser error: %w", err)
					}

					errComponentData := eventErrorComponentData{
						Wallet:             data.Wallet,
						Account:            data.AccountAddress,
						LocalZero:          data.Local.Equal(decimal.Zero),
						LocalAmount:        data.Local.StringFixed(2),
						LocalAmountLong:    data.Local.String(),
						ExternalZero:       data.External.Equal(decimal.Zero),
						ExternalAmount:     data.External.StringFixed(2),
						ExternalAmountLong: data.External.String(),
					}

					getTokenSymbolAndImg(
						data.Token, uint16(network), network,
						&errComponentData.Token, &errComponentData.ImgData,
						&getTokensMetaBatch,
					)

					errors.BalanceMismatches = append(
						errors.BalanceMismatches,
						&errComponentData,
					)
				case db.ParserErrorTypeAccountDataMismatch:
					var data db.ParserErrorAccountDataMismatch
					if err := json.Unmarshal(err.Data, &data); err != nil {
						return nil, 0, fmt.Errorf("unable to unmarshal parser error: %w", err)
					}

					errors.DataMismatches = append(
						errors.DataMismatches,
						&eventErrorComponentData{
							Account: data.AccountAddress,
							IxIdx:   int(err.IxIdx),
							Message: data.Message,
						},
					)
				default:
					assert.True(false, "invalid parser error: %d", err.Type)
				}
			}
		}

		eventsTableRows = append(eventsTableRows, rowData)
	}

	///////////////
	//preceding events
	batch2 := pgx.Batch{}

	for _, row := range eventsTableRows {
		if row.Type != 1 {
			continue
		}

		for _, event := range row.Events {
			for _, source := range event.Sources {
				t, ok := transfers[source.TransferId]
				if !ok {
					// TODO: Need to fetch
					const getTransferQuery = `
						select
							et.token,
							et.token_source,
							et.amount,
							e.method,
							e.id,
							itx.network,
							itx.timestamp
						from
							event_transfer et
						join
							event e on e.id = et.event_id
						join
							internal_tx itx on itx.id = e.internal_tx_id
						where
							et.id = $1
					`
					q := getTokensMetaBatch.Queue(getTransferQuery, source.TransferId)
					q.QueryRow(func(row pgx.Row) error {
						var (
							tokenSource   uint16
							amount        decimal.Decimal
							token, method string
							network       db.Network
							timestamp     time.Time
							eventId       uuid.UUID
						)
						err := row.Scan(
							&token, &tokenSource, &amount,
							&method, &eventId,
							&network, &timestamp,
						)
						assert.NoErr(err, "unable to query event transfer")

						cmp := eventTransferSourceComponentData{
							EventId:         eventId,
							Timestamp:       timestamp,
							Method:          method,
							TokenAmount:     amount.String(),
							UsedTokenAmount: source.UsedAmount.String(),
						}
						getTokenSymbolAndImg(
							token, tokenSource, network,
							&cmp.Token, &cmp.TokenImgData,
							&batch2,
						)
						event.SourcesUi = append(event.SourcesUi, &cmp)

						return nil
					})
					continue
				}

				cmp := eventTransferSourceComponentData{
					EventId:         t.EventId,
					Timestamp:       t.Timestamp,
					Method:          t.EventMethod,
					TokenAmount:     t.Amount.String(),
					UsedTokenAmount: source.UsedAmount.String(),
				}
				getTokenSymbolAndImg(
					t.Token, t.TokenSource, t.Network,
					&cmp.Token, &cmp.TokenImgData,
					&getTokensMetaBatch,
				)
				event.SourcesUi = append(event.SourcesUi, &cmp)
			}
		}
	}

	///////////////
	// execute network stuff

	br := pool.SendBatch(ctx, &getTokensMetaBatch)
	if err := br.Close(); err != nil {
		return nil, 0, fmt.Errorf("unable to query tokens meta: %w", err)
	}

	br = pool.SendBatch(ctx, &batch2)
	if err := br.Close(); err != nil {
		return nil, 0, fmt.Errorf("unable to query transfers: %w", err)
	}

	return eventsTableRows, lastItxPosition, nil
}

func eventsHandler(
	ctx context.Context,
	pool *pgxpool.Pool,
	templates *template.Template,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("cache-control", "no-cache")

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
			limit           = urlParam[int32]{value: 50, set: true}
			fromItxPosition = float32(-1)
			mode            = urlParam[string]{}
			eventId         uuid.UUID
		)

		var (
			idQueryVal      = query.Get("id")
			offsetQueryVal  = query.Get("offset")
			limitQueryVal   = query.Get("limit")
			txIdQueryVal    = query.Get("tx_id")
			modeQueryVal    = query.Get("mode")
			partialQueryVal = query.Get("partial")
		)

		if v, err := uuid.Parse(idQueryVal); err == nil {
			eventId = v
		}
		if v, err := strconv.Atoi(limitQueryVal); err == nil {
			limit.value = int32(v)
		}
		if v, err := strconv.ParseFloat(offsetQueryVal, 32); err == nil {
			fromItxPosition = float32(v)
		}
		if modeQueryVal == "dev" {
			mode.value = modeQueryVal
			mode.set = true
		}

		fullPageRender := partialQueryVal != "true"

		if fullPageRender {
			/////////////
			// get sync status
			const getSyncRequestStatus = `
				select status, id from sync_request where user_account_id = $1
				order by
					timestamp desc
				limit
					1
			`
			syncRequestRow := pool.QueryRow(ctx, getSyncRequestStatus, userAccountId)

			progressIndicator := eventsProgressIndicatorComponentData{
				Done: true,
			}
			var syncRequestStatus db.SyncRequestStatus
			if err := syncRequestRow.Scan(
				&syncRequestStatus,
				&progressIndicator.RequestId,
			); err != nil {
				if !errors.Is(err, pgx.ErrNoRows) {
					renderError(w, 500, "unable to query sync_request: %s", err)
					return
				}
			} else {
				progressIndicator.Done = false
				progressIndicator.Status = syncRequestStatus.String()
			}

			/////////////
			// get events
			eventsTableRows, lastItxPosition, err := renderEvents(
				ctx, pool,
				userAccountId,
				eventId,
				txIdQueryVal,
				limit.value,
				fromItxPosition,
				mode.value == "dev",
			)
			if err != nil {
				renderError(w, 500, err.Error())
				return
			}

			nextUrl := strings.Builder{}
			nextUrl.WriteString("/events?")

			if txIdQueryVal == "" {
				nextUrl.WriteString(fmt.Sprintf("offset=%f", lastItxPosition))

				if limit.set {
					nextUrl.WriteString("&limit=")
					nextUrl.WriteString(fmt.Sprintf("%d", limit.value))
				}

				if mode.set {
					nextUrl.WriteString("&mode=")
					nextUrl.WriteString(mode.value)
				}
			}

			eventsPageData := eventsPageData{
				ProgressIndicator: progressIndicator,
				NextUrl:           nextUrl.String(),
				Rows:              eventsTableRows,
			}

			eventsPageContent := executeTemplateMust(
				templates,
				"events_page",
				eventsPageData,
			)

			page := executeTemplateMust(templates, "dashboard_layout", cmpDashboard{
				Content: template.HTML(eventsPageContent),
			})
			w.WriteHeader(200)
			w.Write(page)
		} else {
			eventsTableRows, _, err := renderEvents(
				ctx, pool,
				userAccountId,
				eventId,
				txIdQueryVal,
				limit.value,
				fromItxPosition,
				mode.value == "dev",
			)
			if err != nil {
				renderError(w, 500, err.Error())
				return
			}

			table := executeTemplateMust(templates, "events_rows", eventsTableRows)
			w.WriteHeader(200)
			w.Write(table)
		}
	}
}
