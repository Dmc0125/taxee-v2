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
	Rows              []eventTableRowComponentData
}

type eventComponentData struct {
	Idx int

	NetworkImgUrl string
	EventType     string
	Method        string

	OutgoingTransfers *eventTransfersComponentData
	IncomingTransfers *eventTransfersComponentData
	Profits           []*eventFiatAmountComponentData

	MissingBalances []*eventTokenAmountComponentData
}

type eventErrorComponentData struct {
	Wallet  string
	Account string
	IxIdx   int

	ImgUrl             string
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
	Type              int
	MissingAccounts   []*eventErrorComponentData
	BalanceMismatches []*eventErrorComponentData
	DataMismatches    []*eventErrorComponentData
}

type eventTransfersComponentData struct {
	Wallet string
	Tokens []*eventTokenAmountComponentData
	Fiats  []*eventFiatAmountComponentData
}

type eventTokenAmountComponentData struct {
	Account    string
	ImgUrl     string
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

type fetchTokenMetadataQueued struct {
	coingeckoId string
	imgUrls     []*string
}

func getTokenSymbolAndImg(
	token string,
	tokenSource uint16,
	network db.Network,
	symbolPtr, imgUrlPtr *string,
	tokensQueue *[]*fetchTokenMetadataQueued,
	getTokensMetaBatch *pgx.Batch,
) {
	appendTokenToQueue := func(coingeckoId string) {
		contains := false
		for _, q := range *tokensQueue {
			if q.coingeckoId == coingeckoId {
				contains = true
				q.imgUrls = append(q.imgUrls, imgUrlPtr)
				break
			}
		}

		if !contains {
			*tokensQueue = append(*tokensQueue, &fetchTokenMetadataQueued{
				coingeckoId: coingeckoId,
				imgUrls:     []*string{imgUrlPtr},
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
				symbolPtr,
				&imgUrl,
			)
			if err != nil {
				return err
			}
			if imgUrl.Valid {
				*imgUrlPtr = imgUrl.String
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
				symbolPtr,
				&imgUrl,
				&coingeckoId,
			)
			if errors.Is(err, pgx.ErrNoRows) {
				*symbolPtr = shorten(token, 3, 2)
				return nil
			}
			if imgUrl.Valid {
				*imgUrlPtr = imgUrl.String
			} else {
				appendTokenToQueue(coingeckoId)
			}
			return err
		})
	}
}

func eventsRenderTokenAmounts(
	amount,
	value,
	profit,
	price decimal.Decimal,
	token, account string,
	network db.Network,
	tokenSource uint16,
	getTokensMetaBatch *pgx.Batch,
	tokensQueue *[]*fetchTokenMetadataQueued,
) (tokenData *eventTokenAmountComponentData, fiatData, profitData *eventFiatAmountComponentData) {
	tokenData = &eventTokenAmountComponentData{
		Account:    account,
		Amount:     amount.StringFixed(2),
		LongAmount: amount.String(),
	}

	getTokenSymbolAndImg(
		token, tokenSource, network,
		&tokenData.Symbol, &tokenData.ImgUrl,
		tokensQueue,
		getTokensMetaBatch,
	)

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

type eventsPagination struct {
	SolanaSlot         int64
	SolanaBlockIndex   int32
	ArbitrumBlock      int64
	ArbitrumBlockIndex int32
}

func (p *eventsPagination) serialize() string {
	q := strings.Builder{}

	q.WriteString(fmt.Sprintf("%d_", p.SolanaSlot))
	q.WriteString(fmt.Sprintf("%d_", p.SolanaBlockIndex))
	q.WriteString(fmt.Sprintf("%d_", p.ArbitrumBlock))
	q.WriteString(fmt.Sprintf("%d", p.ArbitrumBlockIndex))

	return q.String()
}

func (p *eventsPagination) deserialize(src []byte) error {
	setOffsets := func(block *int64, blockIndex *int32, serializedBlock, serializedIndex string) error {
		if serializedBlock != "" {
			b, err := strconv.ParseInt(serializedBlock, 10, 64)
			if err != nil {
				return fmt.Errorf("invalid network block: %s", serializedBlock)
			}
			*block = b
		}

		if serializedIndex != "" {
			i, err := strconv.ParseInt(serializedIndex, 10, 32)
			if err != nil {
				return fmt.Errorf("invalid network block index: %s", serializedIndex)
			}
			*blockIndex = int32(i)
		}

		return nil
	}

	offsets := strings.Split(string(src), "_")
	if len(offsets) != 4 {
		return fmt.Errorf("invalid networks offsets: %s", string(src))
	}

	var err error
	if err = setOffsets(&p.SolanaSlot, &p.SolanaBlockIndex, offsets[0], offsets[1]); err != nil {
		return err
	}
	if err = setOffsets(&p.ArbitrumBlock, &p.ArbitrumBlockIndex, offsets[2], offsets[2]); err != nil {
		return err
	}

	return nil
}

func buildGetEventsQuery(
	userAccountId int32,
	txId string,
	limit int32,
	pagination *eventsPagination,
	devMode bool,
) (q string, params []any) {
	const getEventsQuerySelect = `
			select
				tx.id,
				tx.network,
				tx.timestamp,
				(tx.data->>'slot')::bigint as solana_slot,
				(tx.data->>'blockIndex')::integer as solana_block_index,
				(tx.data->>'block')::bigint as evm_block,
				(tx.data->>'txIdx')::integer as evm_block_index,
				coalesce(events.events, '[]'::jsonb) as events,
				coalesce(errors.errors,	'[]'::jsonb) as errors
			from
				tx_ref
			inner join
				tx on tx.id = tx_ref.tx_id
			left join lateral (
				select jsonb_agg(
					jsonb_build_object(
						'Idx', e.idx,
						'IxIdx', e.ix_idx,
						'UiAppName', e.ui_app_name,
						'UiMethodName', e.ui_method_name,
						'Type', e.type,
						'Data', e.data
					) order by e.idx asc
				) as events 
				from 
					event e
				where
					e.tx_id = tx_ref.tx_id and e.user_account_id = tx_ref.user_account_id
			) events on true
			left join lateral (
				select jsonb_agg(
					jsonb_build_object(
						'IxIdx', pe.ix_idx,
						'EventIdx', pe.event_idx,
						'Origin', pe.origin,
						'Type', pe.type,
						'Data', pe.data
					) order by pe.origin asc
				) as errors
				from 
					parser_error pe
				where 
					pe.tx_id = tx_ref.tx_id and pe.user_account_id = tx_ref.user_account_id
			) errors on true
		`
	const getEventsQuerySelectAfter = `
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

	var filterEmptyTxs string
	if !devMode {
		filterEmptyTxs = "(events.events is not null or errors.errors is not null) and"
	}

	switch {
	case txId != "":
		q = fmt.Sprintf(`
					%s
					where
						tx_ref.user_account_id = $1 and tx.id = $2
					%s
				`,
			getEventsQuerySelect,
			getEventsQuerySelectAfter,
		)
		params = append(params, userAccountId, txId)
	default:
		q = fmt.Sprintf(`
					%s
					where
						tx_ref.user_account_id = $1 and
						%s

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
			filterEmptyTxs,
			getEventsQuerySelectAfter,
		)
		params = append(
			params,
			userAccountId,
			limit,
			pagination.SolanaSlot, pagination.SolanaBlockIndex,
			pagination.ArbitrumBlock, pagination.ArbitrumBlockIndex,
		)
	}

	return
}

func renderEvents(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
	txId string,
	limit int32,
	pagination *eventsPagination,
	devMode bool,
) ([]eventTableRowComponentData, error) {
	getEventsQuery, getEventsParams := buildGetEventsQuery(
		userAccountId,
		txId,
		limit,
		pagination,
		devMode,
	)
	transactionsRows, err := pool.Query(ctx, getEventsQuery, getEventsParams...)

	if err != nil {
		return nil, fmt.Errorf("unable to query txs: %w", err)
	}

	eventsTableRows := make([]eventTableRowComponentData, 0)
	getTokensMetaBatch := pgx.Batch{}
	fetchTokensMetadataQueue := make([]*fetchTokenMetadataQueued, 0)
	var prevDateUnix int64

	for transactionsRows.Next() {
		var (
			txId                                   string
			network                                db.Network
			timestamp                              time.Time
			solanaSlot, evmBlock                   pgtype.Int8
			solanaBlockIndex, evmBlockIndex        pgtype.Int4
			eventsMarshaled, parserErrorsMarshaled json.RawMessage
		)

		if err := transactionsRows.Scan(
			&txId, &network, &timestamp,
			&solanaSlot, &solanaBlockIndex,
			&evmBlock, &evmBlockIndex,
			&eventsMarshaled, &parserErrorsMarshaled,
		); err != nil {
			return nil, fmt.Errorf("unable to scan txs: %w", err)
		}

		switch network {
		case db.NetworkSolana:
			pagination.SolanaSlot = solanaSlot.Int64
			pagination.SolanaBlockIndex = solanaBlockIndex.Int32
		case db.NetworkArbitrum:
			pagination.ArbitrumBlock = evmBlock.Int64
			pagination.ArbitrumBlockIndex = evmBlockIndex.Int32
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
				return nil, fmt.Errorf("unable to unmarshal events: %w", err)
			}

			for _, e := range events {
				if err := e.UnmarshalData(e.SerializedData); err != nil {
					return nil, fmt.Errorf("unmable to unmarshal event data: %w", err)
				}

				eventComponentData := &eventComponentData{
					Idx:             e.Idx,
					NetworkImgUrl:   networkGlobals.imgUrl,
					EventType:       toTitle(string(e.UiMethodName)),
					Method:          toTitle(string(e.UiAppName)),
					MissingBalances: make([]*eventTokenAmountComponentData, 0),
				}
				rowData.Events = append(rowData.Events, eventComponentData)

				///////////////
				// transfers

				switch data := e.Data.(type) {
				case *db.EventTransfer:
					tokenData, fiatData, profitData := eventsRenderTokenAmounts(
						data.Amount, data.Value, data.Profit, data.Price,
						data.Token, data.Account,
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
					fromTokenData, fiatData, profitData := eventsRenderTokenAmounts(
						data.Amount, data.Value, data.Profit, data.Price,
						data.Token, data.FromAccount,
						network,
						data.TokenSource,
						&getTokensMetaBatch,
						&fetchTokensMetadataQueue,
					)
					fiatData.Sign = 0
					fiatData.Missing = false

					eventComponentData.Profits = append(
						eventComponentData.Profits,
						profitData,
					)

					eventComponentData.OutgoingTransfers = &eventTransfersComponentData{
						Wallet: shorten(data.FromWallet, 4, 4),
						Tokens: []*eventTokenAmountComponentData{fromTokenData},
						Fiats:  []*eventFiatAmountComponentData{fiatData},
					}

					toTokenData := *fromTokenData
					toTokenData.Account = data.ToAccount
					eventComponentData.IncomingTransfers = &eventTransfersComponentData{
						Wallet: shorten(data.ToWallet, 4, 4),
						Tokens: []*eventTokenAmountComponentData{&toTokenData},
						Fiats:  []*eventFiatAmountComponentData{fiatData},
					}
				case *db.EventSwap:
					outgoing := eventTransfersComponentData{
						Wallet: shorten(data.Wallet, 4, 4),
					}
					eventComponentData.OutgoingTransfers = &outgoing
					incoming := eventTransfersComponentData{
						Wallet: shorten(data.Wallet, 4, 4),
					}
					eventComponentData.IncomingTransfers = &incoming

					for _, swap := range data.Outgoing {
						tokenData, fiatData, profitData := eventsRenderTokenAmounts(
							swap.Amount, swap.Value, swap.Profit, swap.Price,
							swap.Token, swap.Account,
							network,
							swap.TokenSource,
							&getTokensMetaBatch,
							&fetchTokensMetadataQueue,
						)

						eventComponentData.Profits = append(
							eventComponentData.Profits,
							profitData,
						)

						outgoing.Tokens = append(outgoing.Tokens, tokenData)
						outgoing.Fiats = append(outgoing.Fiats, fiatData)
					}

					for _, swap := range data.Incoming {
						tokenData, fiatData, _ := eventsRenderTokenAmounts(
							swap.Amount, swap.Value, swap.Profit, swap.Price,
							swap.Token, swap.Account,
							network,
							swap.TokenSource,
							&getTokensMetaBatch,
							&fetchTokensMetadataQueue,
						)
						incoming.Tokens = append(incoming.Tokens, tokenData)
						incoming.Fiats = append(incoming.Fiats, fiatData)
					}
				}
			}
		}

		if len(parserErrorsMarshaled) != 2 {
			var errors []*struct {
				IxIdx    int32
				EventIdx pgtype.Int4
				Origin   db.ErrOrigin
				Type     db.ParserErrorType
				Data     json.RawMessage
			}
			if err := json.Unmarshal(parserErrorsMarshaled, &errors); err != nil {
				return nil, fmt.Errorf("unable to unmarshal parser errors: %w", err)
			}

			for _, err := range errors {
				if err.Type == db.ParserErrorTypeMissingBalance {
					var data db.ParserErrorMissingBalance
					if err := json.Unmarshal(err.Data, &data); err != nil {
						return nil, fmt.Errorf("unable to unmarshal parser error: %w", err)
					}

					for _, event := range rowData.Events {
						if event.Idx == int(err.EventIdx.Int32) {
							tokenData := &eventTokenAmountComponentData{
								Account:    data.AccountAddress,
								Amount:     data.Amount.String(),
								LongAmount: data.Amount.String(),
							}

							getTokenSymbolAndImg(
								data.Token, data.TokenSource, network,
								&tokenData.Symbol, &tokenData.ImgUrl,
								&fetchTokensMetadataQueue,
								&getTokensMetaBatch,
							)

							event.MissingBalances = append(
								event.MissingBalances,
								tokenData,
							)
							break
						}
					}
					continue
				}

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

				switch err.Type {
				case db.ParserErrorTypeMissingAccount:
					var data db.ParserErrorMissingAccount
					if err := json.Unmarshal(err.Data, &data); err != nil {
						return nil, fmt.Errorf("unable to unmarshal parser error: %w", err)
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
						return nil, fmt.Errorf("unable to unmarshal parser error: %w", err)
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
						&errComponentData.Token, &errComponentData.ImgUrl,
						&fetchTokensMetadataQueue,
						&getTokensMetaBatch,
					)

					errors.BalanceMismatches = append(
						errors.BalanceMismatches,
						&errComponentData,
					)
				case db.ParserErrorTypeAccountDataMismatch:
					var data db.ParserErrorAccountDataMismatch
					if err := json.Unmarshal(err.Data, &data); err != nil {
						return nil, fmt.Errorf("unable to unmarshal parser error: %w", err)
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
	// execute network stuff

	br := pool.SendBatch(ctx, &getTokensMetaBatch)
	if err := br.Close(); err != nil {
		return nil, fmt.Errorf("unable to query tokens meta: %w", err)
	}

	// TODO: this should not be done like this but after the html was sent
	// via server sent events or something
	insertTokenImgUrlBatch := pgx.Batch{}
	logger.Info("Fetching tokens: %d", len(fetchTokensMetadataQueue))
	for _, queued := range fetchTokensMetadataQueue {
		meta, err := coingecko.GetCoinMetadata(queued.coingeckoId)
		if err == nil {
			for _, imgUrlPtr := range queued.imgUrls {
				*imgUrlPtr = meta.Image.Small
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
		return nil, fmt.Errorf("unable to insert token img urls: %w", err)
	}

	return eventsTableRows, nil
}

func eventsHandler(
	ctx context.Context,
	pool *pgxpool.Pool,
	templates *template.Template,
) http.HandlerFunc {
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
			limit  = urlParam[int32]{value: 50, set: true}
			offset eventsPagination
			mode   = urlParam[string]{}
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
		if offsetQueryVal != "" {
			if err := offset.deserialize([]byte(offsetQueryVal)); err != nil {
				renderError(w, 400, "invalid pagination: %s", err)
				return
			}
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
			eventsTableRows, err := renderEvents(
				ctx, pool,
				userAccountId,
				txIdQueryVal,
				limit.value,
				&offset,
				mode.value == "dev",
			)
			if err != nil {
				renderError(w, 500, err.Error())
				return
			}

			nextUrl := strings.Builder{}
			nextUrl.WriteString("/events?")

			if txIdQueryVal == "" {
				nextUrl.WriteString("offset=")
				nextUrl.WriteString(offset.serialize())

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

			page := executeTemplateMust(templates, "page_layout", pageLayoutComponentData{
				Content: template.HTML(eventsPageContent),
			})
			w.WriteHeader(200)
			w.Write(page)
		} else {
			eventsTableRows, err := renderEvents(
				ctx, pool,
				userAccountId,
				txIdQueryVal,
				limit.value,
				&offset,
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
