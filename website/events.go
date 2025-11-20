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

type eventsPageData struct {
	NextUrl string
	Rows    []eventTableRowComponentData
}

type eventComponentData struct {
	NetworkImgUrl string
	EventType     string
	Method        string

	OutgoingTransfers *eventTransfersComponentData
	IncomingTransfers *eventTransfersComponentData
	Profits           []*eventFiatAmountComponentData
}

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

type eventTransfersComponentData struct {
	Wallet string
	Tokens []*eventTokenAmountComponentData
	Fiats  []*eventFiatAmountComponentData
}

type eventTokenAmountComponentData struct {
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
		Amount:     amount.StringFixed(2),
		LongAmount: amount.String(),
	}
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

		fmt.Println(r.URL.RawQuery)

		var (
			limit  = urlParam[int32]{value: 50, set: true}
			offset eventsPagination
			mode   = urlParam[string]{}
		)

		var (
			offsetQueryVal = query.Get("offset")
			limitQueryVal  = query.Get("limit")
			txIdQueryVal   = query.Get("tx_id")
			modeQueryVal   = query.Get("mode")
		)

		fmt.Println(offsetQueryVal)

		if v, err := strconv.Atoi(limitQueryVal); err == nil {
			limit.value = int32(v)
		}
		if offsetQueryVal != "" {
			fmt.Println("Hey")
			if err := offset.deserialize([]byte(offsetQueryVal)); err != nil {
				renderError(w, 400, "invalid pagination: %s", err)
				return
			}
		}
		if modeQueryVal == "dev" {
			mode.value = modeQueryVal
			mode.set = true
		}

		fmt.Println(offset)

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

		var filterEmptyTxs string
		if mode.value != "dev" {
			filterEmptyTxs = "(e.tx_id is not null or pe.id is not null) and"
		}

		switch {
		case txIdQueryVal != "":
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
				txIdQueryVal,
			)
		default:
			getEventsQuery := fmt.Sprintf(`
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
			eventsRows, err = pool.Query(
				ctx,
				getEventsQuery,
				// args
				userAccountId,
				limit.value,
				offset.SolanaSlot, offset.SolanaBlockIndex,
				offset.ArbitrumBlock, offset.ArbitrumBlockIndex,
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
				offset.SolanaSlot = solanaSlot.Int64
				offset.SolanaBlockIndex = solanaBlockIndex.Int32
			case db.NetworkArbitrum:
				offset.ArbitrumBlock = evmBlock.Int64
				offset.ArbitrumBlockIndex = evmBlockIndex.Int32
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
						EventType:     toTitle(string(e.UiMethodName)),
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
								swap.Amount,
								swap.Value,
								swap.Profit,
								swap.Price,
								swap.Token,
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
								swap.Amount,
								swap.Value,
								swap.Profit,
								swap.Price,
								swap.Token,
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
			NextUrl: nextUrl.String(),
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
