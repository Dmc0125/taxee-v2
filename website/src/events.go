package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"math"
	"net/http"
	"slices"
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

var appImgUrls = map[string]string{
	"0-native":           "/static/logo_solana.svg",
	"0-system":           "/static/logo_solana.svg",
	"0-token":            "/static/logo_solana.svg",
	"0-associated_token": "/static/logo_solana.svg",
	"0-meteora_pools":    "/static/logo_sol_meteora.svg",
	"0-meteora_farms":    "/static/logo_sol_meteora.svg",
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

type eventPrecedingEventComponentData struct {
	Id          uuid.UUID
	Timestamp   time.Time
	Method      string
	TokenAmount *eventTokenAmountComponentData
	FiatAmount  eventFiatAmountComponentData
	Price       eventFiatAmountComponentData
}

type eventComponentData struct {
	Id                 uuid.UUID
	Timestamp          time.Time
	PrecedingEventsIds []uuid.UUID

	// UI
	Native        bool
	NetworkImgUrl string
	Method        string
	App           string
	AppImgUrl     string

	OutgoingTransfers *eventTransfersComponentData
	IncomingTransfers *eventTransfersComponentData
	Profits           []*eventFiatAmountComponentData

	PrecedingEvents []eventPrecedingEventComponentData

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
	value, price, profit decimal.Decimal,
) (fiatData, profitData *eventFiatAmountComponentData) {
	fiatData = &eventFiatAmountComponentData{
		Amount:   value.StringFixed(2),
		Currency: "eur",
		Price:    price.StringFixed(2),
		Sign:     value.Sign(),
		Missing:  price.Equal(decimal.Zero),
		Zero:     value.Equal(decimal.Zero),
	}
	profitData = &eventFiatAmountComponentData{
		Amount:   profit.StringFixed(2),
		Currency: "eur",
		Sign:     profit.Sign(),
		Zero:     profit.Equal(decimal.Zero),
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
			coalesce(events.events, '[]'::jsonb) as events,
			coalesce(errors.errors,	'[]'::jsonb) as errors
		from
			internal_tx itx
		left join lateral (
			select jsonb_agg(
				jsonb_build_object(
					'Id', e.id,
					'PrecedingEvents', e.preceding_events_ids,
					'UiAppName', e.ui_app_name,
					'UiMethodName', e.ui_method_name,
					'Type', e.type,
					'Data', e.data
				) order by e.position asc
			) as events 
			from 
				event e
			where
				e.internal_tx_id = itx.id
		) events on true
		left join lateral (
			select jsonb_agg(
		 		jsonb_build_object(
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

func renderTransferEvent(
	eventComponentData *eventComponentData,
	data *db.EventTransfer,
	network db.Network,
	getTokensMetaBatch *pgx.Batch,
) {
	fiatData, profitData := eventsCreateFiatAmountsData(
		data.Value, data.Price, data.Profit,
	)
	eventComponentData.Profits = append(
		eventComponentData.Profits,
		profitData,
	)

	if data.MissingAmount.GreaterThan(decimal.Zero) {
		tokenData := &eventTokenAmountComponentData{
			Account:    data.FromAccount,
			Amount:     data.Amount.String(),
			LongAmount: data.Amount.String(),
		}
		getTokenSymbolAndImg(
			data.Token, data.TokenSource, network,
			&tokenData.Symbol, &tokenData.ImgData,
			getTokensMetaBatch,
		)
		eventComponentData.MissingBalances = append(
			eventComponentData.MissingBalances,
			tokenData,
		)
	}

	if data.Direction == db.EventTransferInternal {
		fromTokenData := eventsCreateTokenAmountData(
			data.Amount, data.Token, data.FromAccount,
			network, data.TokenSource,
			getTokensMetaBatch,
		)
		toTokenData := eventsCreateTokenAmountData(
			data.Amount, data.Token, data.ToAccount,
			network, data.TokenSource,
			getTokensMetaBatch,
		)

		fiatData.Sign = 0
		fiatData.Missing = false

		eventComponentData.OutgoingTransfers = &eventTransfersComponentData{
			Wallet: shorten(data.FromWallet, 4, 4),
			Tokens: []*eventTokenAmountComponentData{fromTokenData},
			Fiats:  []*eventFiatAmountComponentData{fiatData},
		}
		eventComponentData.IncomingTransfers = &eventTransfersComponentData{
			Wallet: shorten(data.ToWallet, 4, 4),
			Tokens: []*eventTokenAmountComponentData{toTokenData},
			Fiats:  []*eventFiatAmountComponentData{fiatData},
		}
	} else {
		var account, wallet string
		var transfers *eventTransfersComponentData

		switch data.Direction {
		case db.EventTransferIncoming:
			account, wallet = data.ToAccount, data.ToWallet
			eventComponentData.IncomingTransfers = &eventTransfersComponentData{}
			transfers = eventComponentData.IncomingTransfers
		case db.EventTransferOutgoing:
			account, wallet = data.FromAccount, data.FromWallet
			eventComponentData.OutgoingTransfers = &eventTransfersComponentData{}
			transfers = eventComponentData.OutgoingTransfers
		}

		tokenData := eventsCreateTokenAmountData(
			data.Amount,
			data.Token, account,
			network,
			data.TokenSource,
			getTokensMetaBatch,
		)

		*transfers = eventTransfersComponentData{
			Wallet: shorten(wallet, 4, 4),
			Tokens: []*eventTokenAmountComponentData{
				tokenData,
			},
			Fiats: []*eventFiatAmountComponentData{
				fiatData,
			},
		}
	}
}

func renderSwapEvent(
	eventComponentData *eventComponentData,
	data *db.EventSwap,
	network db.Network,
	getTokensMetaBatch *pgx.Batch,
) {
	walletShort := shorten(data.Wallet, 4, 4)
	outgoing := eventTransfersComponentData{
		Wallet: walletShort,
	}
	eventComponentData.OutgoingTransfers = &outgoing
	incoming := eventTransfersComponentData{
		Wallet: walletShort,
	}
	eventComponentData.IncomingTransfers = &incoming

	for _, swap := range data.Outgoing {
		tokenData := eventsCreateTokenAmountData(
			swap.Amount, swap.Token, swap.Account,
			network,
			swap.TokenSource,
			getTokensMetaBatch,
		)
		fiatData, profitData := eventsCreateFiatAmountsData(
			swap.Value, swap.Price, swap.Profit,
		)

		eventComponentData.Profits = append(
			eventComponentData.Profits,
			profitData,
		)

		outgoing.Tokens = append(outgoing.Tokens, tokenData)
		outgoing.Fiats = append(outgoing.Fiats, fiatData)

		if swap.MissingAmount.GreaterThan(decimal.Zero) {
			tokenData := &eventTokenAmountComponentData{
				Account:    swap.Account,
				Amount:     swap.Amount.String(),
				LongAmount: swap.Amount.String(),
			}
			getTokenSymbolAndImg(
				swap.Token, swap.TokenSource, network,
				&tokenData.Symbol, &tokenData.ImgData,
				getTokensMetaBatch,
			)
			eventComponentData.MissingBalances = append(
				eventComponentData.MissingBalances,
				tokenData,
			)
		}
	}

	for _, swap := range data.Incoming {
		tokenData := eventsCreateTokenAmountData(
			swap.Amount, swap.Token, swap.Account,
			network, swap.TokenSource,
			getTokensMetaBatch,
		)
		fiatData, _ := eventsCreateFiatAmountsData(
			swap.Value, swap.Price, swap.Profit,
		)

		incoming.Tokens = append(incoming.Tokens, tokenData)
		incoming.Fiats = append(incoming.Fiats, fiatData)
	}
}

func renderEvents(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
	txId string,
	limit int32,
	offset float32,
	devMode bool,
) ([]eventsTableRowComponentData, float32, error) {
	getEventsQuery, getEventsParams := buildGetEventsQuery(
		userAccountId,
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
	allEvents := make([]*eventComponentData, 0)
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
				if err := e.UnmarshalData(e.SerializedData); err != nil {
					return nil, 0, fmt.Errorf("unable to unmarshal event data: %w", err)
				}

				appImgUrl := appImgUrls[fmt.Sprintf("%d-%s", network, e.UiAppName)]
				var native bool
				switch e.UiAppName {
				case "native", "system", "token", "associated_token":
					native = true
				}

				eventComponentData := eventComponentData{
					Id:                 e.Id,
					Timestamp:          timestamp,
					PrecedingEventsIds: e.PrecedingEvents,

					Native:          native,
					NetworkImgUrl:   networkGlobals.imgUrl,
					Method:          e.UiMethodName,
					App:             e.UiAppName,
					AppImgUrl:       appImgUrl,
					MissingBalances: make([]*eventTokenAmountComponentData, 0),
				}
				rowData.Events = append(rowData.Events, &eventComponentData)
				allEvents = append(allEvents, &eventComponentData)

				///////////////
				// transfers

				switch data := e.Data.(type) {
				case *db.EventTransfer:
					renderTransferEvent(
						&eventComponentData,
						data,
						network,
						&getTokensMetaBatch,
					)
				case *db.EventSwap:
					renderSwapEvent(
						&eventComponentData,
						data,
						network,
						&getTokensMetaBatch,
					)
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
						Token:              "USDC",
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

	// TODO: will need to fetch events that are no fetched currently
	// TODO: also show the actual used amount from the transfer
	for _, e := range allEvents {
		for _, precedingEventId := range e.PrecedingEventsIds {
			idx := slices.IndexFunc(allEvents, func(e2 *eventComponentData) bool {
				return e2.Id == precedingEventId
			})
			if idx == -1 {
				// not found
				continue
			}

			// create preceding event
			precedingEvent := allEvents[idx]

			var tokenAmount *eventTokenAmountComponentData
			var transferIdx int

			for i, incomingTransfer := range precedingEvent.IncomingTransfers.Tokens {
				for _, outgoingTransfer := range e.OutgoingTransfers.Tokens {
					if incomingTransfer.Symbol == outgoingTransfer.Symbol {
						tokenAmount = incomingTransfer
						transferIdx = i
						break
					}
				}
			}

			fiatAmount := *precedingEvent.IncomingTransfers.Fiats[transferIdx]
			price := eventFiatAmountComponentData{
				Amount:   fiatAmount.Price,
				Currency: fiatAmount.Currency,
			}

			precedingEventComponent := eventPrecedingEventComponentData{
				Id:          precedingEvent.Id,
				Timestamp:   precedingEvent.Timestamp,
				Method:      precedingEvent.Method,
				TokenAmount: tokenAmount,
				FiatAmount:  fiatAmount,
				Price:       price,
			}
			e.PrecedingEvents = append(e.PrecedingEvents, precedingEventComponent)
		}
	}

	///////////////
	// execute network stuff

	br := pool.SendBatch(ctx, &getTokensMetaBatch)
	if err := br.Close(); err != nil {
		return nil, 0, fmt.Errorf("unable to query tokens meta: %w", err)
	}

	// TODO: this should not be done like this but after the html was sent
	// via server sent events or something
	// insertTokenImgUrlBatch := pgx.Batch{}
	// logger.Info("Fetching tokens: %d", len(fetchTokensMetadataQueue))
	// for _, queued := range fetchTokensMetadataQueue {
	// 	meta, err := coingecko.GetCoinMetadata(queued.coingeckoId)
	// 	if err == nil {
	// 		for _, imgUrlPtr := range queued.imgUrls {
	// 			*imgUrlPtr = meta.Image.Small
	// 		}
	//
	// 		const insertTokenImgUrl = `
	// 			update coingecko_token_data set
	// 				image_url = $1
	// 			where
	// 				coingecko_id = $2
	// 		`
	// 		insertTokenImgUrlBatch.Queue(
	// 			insertTokenImgUrl,
	// 			meta.Image.Small,
	// 			queued.coingeckoId,
	// 		)
	// 	}
	// }
	//
	// br = pool.SendBatch(ctx, &insertTokenImgUrlBatch)
	// if err := br.Close(); err != nil {
	// 	return nil, 0, fmt.Errorf("unable to insert token img urls: %w", err)
	// }

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
		)

		var (
			offsetQueryVal  = query.Get("offset")
			limitQueryVal   = query.Get("limit")
			txIdQueryVal    = query.Get("tx_id")
			modeQueryVal    = query.Get("mode")
			partialQueryVal = query.Get("partial")
		)

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

			page := executeTemplateMust(templates, "dashboard_layout", pageLayoutComponentData{
				Content: template.HTML(eventsPageContent),
			})
			w.WriteHeader(200)
			w.Write(page)
		} else {
			eventsTableRows, _, err := renderEvents(
				ctx, pool,
				userAccountId,
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
