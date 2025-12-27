package parser

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	_ "net/http/pprof"
	"slices"
	"strconv"
	"taxee/cmd/fetcher/evm"
	"taxee/pkg/assert"
	"taxee/pkg/coingecko"
	"taxee/pkg/db"
	"taxee/pkg/jsonrpc"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

// TODOs:
//
// 1. the parsers must stay alive in production, there should be no asserts
// 	  if the instruction is not what we expect
//    	- missing inner ixs
//		- missing data
//	  just skip parsing
// 2. not sure how I feel about the errors
//    - missing amount / missing price - good
//	  - balance mismatch - good
//
//    - preprocess errors are kinda useless for the user, they can not change
// 		them so those probably don't need to exist
//	  - same for data mismatch / missing account
//
// 	  the alternative to these errors is just showing the balance mismatches,
//	  missing amount and missing prices and having a good and easy way to fix
//	  them with custom events
//
//    there could also be hints, like, some ix and therefore an event was
// 	  expected, but something was missing (account, invalid data, ...) so it
//    could not be parsed but the user would have an idea about whats going on
//

const tokenSourceCoingecko uint16 = math.MaxUint16

func newDecimalFromRawAmount(amount uint64, decimals uint8) decimal.Decimal {
	d := decimal.NewFromBigInt(
		new(big.Int).SetUint64(amount),
		-int32(decimals),
	)
	return d
}

func solNewEvent(ctx *solContext, app, method string, t db.EventType) *db.Event {
	assert.True(ctx.events != nil, "events are nil")

	id, err := uuid.NewRandom()
	assert.NoErr(err, "unable to generate event id")

	event := &db.Event{
		Id:        id,
		Timestamp: ctx.timestamp,
		Network:   db.NetworkSolana,
		App:       app,
		Method:    method,
		Type:      t,
	}
	*ctx.events = append(*ctx.events, event)
	return event
}

func evmNewEvent(ctx *evmContext) *db.Event {
	id, err := uuid.NewRandom()
	assert.NoErr(err, "unable to generate event id")

	return &db.Event{
		Id:        id,
		Timestamp: ctx.timestamp,
		Network:   ctx.network,
	}
}

func solNewError(ctx *solContext) *db.ParserError {
	err := db.ParserError{
		TxId:   ctx.txId,
		IxIdx:  int32(ctx.ixIdx),
		Origin: ctx.errOrigin,
	}
	*ctx.errors = append(*ctx.errors, &err)
	return &err
}

func getTransferEventDirection(fromInternal, toInternal bool) db.EventTransferDirection {
	assert.True(fromInternal || toInternal, "not a valid transfer direction")
	var direction db.EventTransferDirection
	switch {
	case fromInternal && toInternal:
		direction = db.EventTransferInternal
	case fromInternal:
		direction = db.EventTransferOutgoing
	case toInternal:
		direction = db.EventTransferIncoming
	}
	return direction
}

func FetchCoingeckoTokens(
	ctx context.Context,
	pool *pgxpool.Pool,
) error {
	coins, err := coingecko.GetCoins(ctx)
	if err != nil {
		return fmt.Errorf("unable to fetch coingecko tokens: %w", err)
	}

	batch := pgx.Batch{}

	for _, coin := range coins {
		const insertCoingeckoTokenData = `
			insert into coingecko_token_data (
				coingecko_id, symbol, name
			) values (
				$1, $2, $3
			) on conflict (coingecko_id) do nothing
		`
		batch.Queue(
			insertCoingeckoTokenData,
			coin.Id,
			coin.Symbol,
			coin.Name,
		)

		for platform, mint := range coin.Platforms {
			var network db.Network
			switch platform {
			case "solana":
				network = db.NetworkSolana
			case "arbitrum-one":
				network = db.NetworkArbitrum
			case "avalanche":
				network = db.NetworkAvaxC
			case "ethereum":
				network = db.NetworkEthereum
			case "binance-smart-chain":
				network = db.NetworkBsc
			case "cosmos", "osmosis":
				continue
			default:
				continue
			}

			const insertCoingeckoToken = `
				insert into coingecko_token (
					coingecko_id, network, address
				) values (
					$1, $2, $3
				) on conflict (network, address) do nothing
			`
			batch.Queue(
				insertCoingeckoToken,
				coin.Id,
				network,
				mint,
			)
		}
	}

	if err := pool.SendBatch(ctx, &batch).Close(); err != nil {
		return fmt.Errorf("unable to insert coingecko tokens: %w", err)
	}

	return nil
}

func ParseTransactions(
	ctx context.Context,
	pool *pgxpool.Pool,
	alchemyApiKey string,
	userAccountId int32,
) error {
	slog.Info("fetch coingecko tokens")
	if err := FetchCoingeckoTokens(ctx, pool); err != nil {
		return err
	}

	slog.Info("query transactions")
	var solanaContext solContext
	evmContexts := make(map[db.Network]*evmContext)
	for i := db.NetworkEvmStart + 1; i < db.NetworksCount; i += 1 {
		evmContexts[db.Network(i)] = &evmContext{
			contracts: make(map[string][]evmContractImplementation),
			network:   db.Network(i),
		}
	}

	type transaction struct {
		id        string
		err       bool
		data      any
		network   db.Network
		timestamp time.Time
	}
	transactions := make([]*transaction, 0)

	err := db.ExecuteTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
		const selectWalletsQuery = `
			select address, network from wallet where
				user_account_id = $1 and
				status = 'success'
		`
		rows, err := tx.Query(ctx, selectWalletsQuery, userAccountId)
		if err != nil {
			return fmt.Errorf("unable to select wallets: %w", err)
		}
		walletsCount := 0
		for rows.Next() {
			walletsCount += 1
			var address string
			var network db.Network
			if err := rows.Scan(&address, &network); err != nil {
				return fmt.Errorf("unable to scan wallets: %w", err)
			}

			switch {
			case network == db.NetworkSolana:
				solanaContext.wallets = append(solanaContext.wallets, address)
			case db.NetworkEvmStart < network && network < db.NetworksCount:
				evmContexts[network].wallets = append(evmContexts[network].wallets, address)
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("unable to read wallets: %w", err)
		}
		slog.Info("fetched wallets", "count", walletsCount)

		// select all user transactions that have at least one existing and
		// fetch wallet related to them
		const selectTransactionsQuery = `
			select
				tx.id, tx.err, tx.data, tx.network, tx.timestamp
			from
				tx
			where
				tx.id in (
					select distinct tr.tx_id from tx_ref tr where
						tr.user_account_id = $1 and ((
							tr.wallet_id is not null and exists (
								select 1 from wallet w where
									w.id = tr.wallet_id and
									w.status = 'success'
						)) or (
							tr.related_account_id is not null and exists (
								select 1 from solana_related_account ra 
								join
									wallet w on
										w.id = ra.wallet_id and
										w.status = 'success'
								where
									ra.id = tr.related_account_id
							)
						))
				)
			order by 
				-- global ordering
				tx.timestamp asc,
				-- network specific ordering in case of timestamps conflicts
				-- solana 
				(tx.data->>'slot')::bigint asc,
				(tx.data->>'blockIndex')::integer asc,
				-- evm
				(tx.data->>'block')::bigint asc,
				(tx.data->>'txIdx')::integer asc,
				tx.id
		`
		rows, err = tx.Query(ctx, selectTransactionsQuery, userAccountId)
		if err != nil {
			return fmt.Errorf("unabel to select transactions: %w", err)
		}
		txCount := 0
		for rows.Next() {
			txCount += 1

			var transaction transaction
			var dataSerialized json.RawMessage
			if err := rows.Scan(
				&transaction.id,
				&transaction.err,
				&dataSerialized,
				&transaction.network,
				&transaction.timestamp,
			); err != nil {
				return fmt.Errorf("unable to scan transactions: %w", err)
			}

			switch {
			case transaction.network == db.NetworkSolana:
				var data db.SolanaTransactionData
				if err := json.Unmarshal(dataSerialized, &data); err != nil {
					return fmt.Errorf("unable to unmarshal solana tx data: %w", err)
				}
				transaction.data = &data
			case db.NetworkEvmStart < transaction.network && transaction.network < db.NetworksCount:
				var data db.EvmTransactionData
				if err := json.Unmarshal(dataSerialized, &data); err != nil {
					return fmt.Errorf("unable to unmarshal evm tx data: %w", err)
				}
				transaction.data = &data
			default:
				return fmt.Errorf("invalid network: %d", transaction.network)
			}

			transactions = append(transactions, &transaction)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("unable to read transactions: %w", err)
		}

		slog.Info("fetched transactions", "count", txCount)

		return nil
	})
	if err != nil {
		return err
	}

	slog.Info("preprocess transactions")

	errors := make([]*db.ParserError, 0)

	solanaContext.accounts = make(map[string][]accountLifetime)
	solanaContext.errors = &errors
	solanaContext.errOrigin = db.ErrOriginPreprocess

	evmAddresses := make(map[db.Network]map[string][]uint64)
	appendEvmAddress := func(network db.Network, address string, block uint64, internalTxs []*db.EvmInternalTx) {
		addresses, ok := evmAddresses[network]
		if !ok {
			addresses = make(map[string][]uint64)
		}

		if !slices.Contains(addresses[address], block) {
			addresses[address] = append(addresses[address], block)
		}

		for _, itx := range internalTxs {
			if !slices.Contains(addresses[itx.To], block) {
				addresses[itx.To] = append(addresses[itx.To], block)
			}
		}

		evmAddresses[network] = addresses
	}

	// preprocess transactions
	for _, tx := range transactions {
		if tx.err {
			continue
		}

		switch data := tx.data.(type) {
		case *db.SolanaTransactionData:
			solanaContext.txId = tx.id
			solanaContext.timestamp = tx.timestamp
			solanaContext.slot = data.Slot
			solanaContext.nativeBalances = data.NativeBalances
			solanaContext.tokenBalances = data.TokenBalances

			solPreprocessTx(&solanaContext, data.Instructions)
		case *db.EvmTransactionData:
			appendEvmAddress(
				tx.network,
				data.To,
				data.Block,
				data.InternalTxs,
			)
		default:
			return fmt.Errorf("invalid tx data: %T", tx.data)
		}
	}

	for network, addresses := range evmAddresses {
		err := evmIdentifyContracts(
			ctx, network, alchemyApiKey,
			addresses, evmContexts[network],
		)
		if err != nil {
			return err
		}
	}

	slog.Info("process transactions")

	// process transactions
	newFeeEvent := func(
		events *[]*db.Event,
		timestamp time.Time,
		network db.Network,
		fee decimal.Decimal,
		from, token string,
		tokenSource uint16,
	) error {
		id, err := uuid.NewRandom()
		if err != nil {
			return fmt.Errorf("unable to generate event id: %w", err)
		}
		*events = append(*events, &db.Event{
			Id:        id,
			Timestamp: timestamp,
			Network:   network,
			App:       "native",
			Method:    "fee",
			Type:      db.EventTypeTransfer,
			Transfers: []*db.EventTransfer{{
				Direction:   db.EventTransferOutgoing,
				FromWallet:  from,
				FromAccount: from,
				Token:       token,
				Amount:      fee,
				TokenSource: tokenSource,
			}},
		})
		return nil
	}

	solanaContext.errOrigin = db.ErrOriginProcess
	eventsByTx := make(map[string][]*db.Event)

	for _, tx := range transactions {
		transactionEvents := make([]*db.Event, 0)

		switch data := tx.data.(type) {
		case *db.SolanaTransactionData:
			if slices.Contains(solanaContext.wallets, data.Signer) {
				err := newFeeEvent(
					&transactionEvents,
					tx.timestamp, tx.network,
					data.Fee, data.Signer,
					SOL_MINT_ADDRESS, uint16(db.NetworkSolana),
				)
				if err != nil {
					return err
				}
			}

			if tx.err {
				continue
			}

			solanaContext.events = &transactionEvents
			solanaContext.txId = tx.id
			solanaContext.timestamp = tx.timestamp
			solanaContext.slot = data.Slot
			solanaContext.tokensDecimals = data.TokenDecimals

			for ixIdx, ix := range data.Instructions {
				solanaContext.ixIdx = uint32(ixIdx)
				solProcessIx(&solanaContext, ix)
			}
		case *db.EvmTransactionData:
			evmContext := evmContexts[tx.network]

			if slices.Contains(evmContext.wallets, data.From) {
				err := newFeeEvent(
					&transactionEvents,
					tx.timestamp, tx.network,
					data.Fee, data.From,
					"ethereum", tokenSourceCoingecko,
				)
				if err != nil {
					return err
				}
			}

			if tx.err {
				continue
			}

			evmContext.timestamp = tx.timestamp
			evmContext.txId = tx.id
			evmProcessTx(evmContext, &transactionEvents, data)
		default:
			return fmt.Errorf("invalid tx data: %T", tx.data)
		}

		if len(transactionEvents) != 0 {
			eventsByTx[tx.id] = transactionEvents
		}
	}

	slog.Info("fetch evm decimals")

	// fetch evm tokens decimals
	slog.Info("fetching evm tokens decimals")
	evmTokens := make(map[db.Network]map[string][]*decimal.Decimal)

	for _, events := range eventsByTx {
		for _, event := range events {
			if db.NetworkEvmStart < event.Network && event.Network < db.NetworksCount {
				tokens, ok := evmTokens[event.Network]
				if !ok {
					tokens = make(map[string][]*decimal.Decimal)
				}

				for _, t := range event.Transfers {
					if t.TokenSource != tokenSourceCoingecko {
						tokens[t.Token] = append(tokens[t.Token], &t.Amount)
					}
				}

				evmTokens[event.Network] = tokens
			}
		}
	}

	for network, tokens := range evmTokens {
		url, err := evm.AlchemyApiUrl(network, alchemyApiKey)
		if err != nil {
			return err
		}
		batch := jsonrpc.NewBatch(url)

		for token, amounts := range tokens {
			params := evm.CallParams{
				To: token,
				// NOTE: ERC20 decimals() selector - 0x313ce567
				Input:    evm.DataBytes{49, 60, 229, 103},
				BlockTag: "latest",
			}
			batch.Queue(
				evm.Call, &params,
				func(rm json.RawMessage) (bool, error) {
					srm := string(rm)
					if srm == "null" {
						return true, nil
					}
					srm = srm[1 : len(srm)-1] // remove quotes

					decimals := int32(0)

					// 32 bytes * 2 + 0x = 66
					if len(srm) == 66 {
						// use last byte only
						d, err := strconv.ParseUint(srm[len(srm)-2:], 16, 8)
						if err != nil {
							return false, fmt.Errorf("unable to parse decimals: %w", err)
						}
						decimals = int32(d)
					} else if srm != "0x" {
						return false, fmt.Errorf("invalid decimals response: %s", string(srm))
					}

					for _, a := range amounts {
						*a = decimal.NewFromBigInt(a.BigInt(), -decimals)
					}

					return false, nil
				},
			)
		}

		if err := jsonrpc.CallBatch(ctx, batch); err != nil {
			return fmt.Errorf("unable to get decimals for %s: %w", network.String(), err)
		}
	}

	slog.Info("insert results")
	// insert events

	err = db.ExecuteTx(ctx, pool, func(ctx context.Context, tx pgx.Tx) error {
		batch := pgx.Batch{}

		const deleteInternalTxsQuery = `
			delete from internal_tx where user_account_id = $1
		`
		batch.Queue(deleteInternalTxsQuery, userAccountId)

		const insertInternalTxsQuery = `
			insert into internal_tx (
				position, user_account_id, tx_id, network, timestamp
			) values (
				$1, $2, $3, $4, $5
			) returning
				id
		`
		internalTxsByHash := make(map[string]int32)
		for i, tx := range transactions {
			batch.Queue(
				insertInternalTxsQuery,
				float32(i), userAccountId, tx.id, tx.network, tx.timestamp,
			).QueryRow(func(row pgx.Row) error {
				var id int32
				err := row.Scan(&id)
				if err != nil {
					return fmt.Errorf("unable to scan internal tx: %w", err)
				}
				internalTxsByHash[tx.id] = id
				return nil
			})
		}

		if err := tx.SendBatch(ctx, &batch).Close(); err != nil {
			return fmt.Errorf("unable to insert internal txs: %w", err)
		}

		batch = pgx.Batch{}

		const insertParserErrorQuery = `
			insert into parser_error (
				internal_tx_id, ix_idx, origin, type, data
			) values (
				$1, $2, $3, $4, $5
			)
		`
		for _, err := range errors {
			internalTxId, ok := internalTxsByHash[err.TxId]
			if !ok {
				return fmt.Errorf("missing internal tx id for %s", err.TxId)
			}

			batch.Queue(
				insertParserErrorQuery,
				internalTxId, err.IxIdx, err.Origin, err.Type, err.Data,
			)
		}

		const insertEventQuery = `
			insert into event (
				id, position, internal_tx_id, app, method, type
			) values (
				$1, $2, $3, $4, $5, $6
			)
		`
		const insertTransferQuery = `
			insert into event_transfer (
				id, event_id, position,
				direction, from_wallet, from_account, to_wallet, to_account,
				token, amount, token_source,
				price, value, profit, missing_amount
			) values (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
			)
		`

		for txId, transactionEvents := range eventsByTx {
			internalTxId, ok := internalTxsByHash[txId]
			if !ok {
				return fmt.Errorf("missing internal tx id for %s", txId)
			}

			for eventPosition, event := range transactionEvents {
				eventId, err := uuid.NewRandom()
				if err != nil {
					return fmt.Errorf("unable to generate event id: %w", err)
				}
				batch.Queue(
					insertEventQuery,
					eventId, eventPosition, internalTxId,
					event.App, event.Method, event.Type,
				)

				for transferPosition, transfer := range event.Transfers {
					transferId, err := uuid.NewRandom()
					if err != nil {
						return fmt.Errorf("unable to generate transfer id: %w", err)
					}
					batch.Queue(
						insertTransferQuery,
						// args
						transferId, eventId, transferPosition,
						transfer.Direction,
						transfer.FromWallet, transfer.FromAccount,
						transfer.ToWallet, transfer.ToAccount,
						transfer.Token, transfer.Amount, transfer.TokenSource,
						transfer.Price, transfer.Value,
						transfer.Profit, transfer.MissingAmount,
					)
				}
			}
		}

		if err := tx.SendBatch(ctx, &batch).Close(); err != nil {
			return fmt.Errorf("unable to insert events: %w", err)
		}
		return nil
	})

	slog.Info("parse transactions done")
	return err
}

const selectEventsQuery = `
	select 
		itx.id,
		itx.tx_id,
		itx.network,
		itx.timestamp,
		coalesce(
			(
				select
					json_agg(json_build_object(
						'type', e.type,
						'transfers', (
							select
								json_agg(json_build_object(
									'id', et.id,
									'direction', et.direction,
									'fromWallet', et.from_wallet,
									'fromAccount', et.from_account,
									'toWallet', et.to_wallet,
									'toAccount', et.to_account,
									'token', et.token,
									'amount', et.amount,
									'tokenSource', et.token_source
								) order by et.position asc)
							from
								event_transfer et
							where
								et.event_id = e.id
						)
					) order by e.position asc)
				from
					event e
				where
					e.internal_tx_id = itx.id
			),
			'[]'::json
		) as events,
		(
			case
				when itx.network = 'solana' and tx.id is not null then (tx.data->>'nativeBalances')::json
				else '{}'::json
			end
		),
		(
			case
				when itx.network = 'solana' and tx.id is not null then (tx.data->>'tokenBalances')::json
				else '{}'::json
			end
		),
		(
			case
				when itx.network = 'solana' and tx.id is not null then (tx.data->>'tokenDecimals')::json
				else '{}'::json
			end
		)
	from
		internal_tx itx
	left join
		tx on tx.id = itx.tx_id
	where
		itx.user_account_id = $1
	order by
		itx.position asc
`

type coingeckoPricesCache struct {
	found   map[string][]*coingecko.OhlcCoinData
	missing map[string][]*coingecko.MissingPricepointsRange
}

func (cache *coingeckoPricesCache) get(
	coingeckoId string,
	ts time.Time,
) (price decimal.Decimal, found, ok bool) {
	prices, okPrices := cache.found[coingeckoId]
	if okPrices {
		for _, p := range prices {
			if p.Timestamp.Equal(ts) {
				price, found, ok = p.Close, true, true
				return
			}
		}
	}

	tsUnix := ts.Unix()
	ranges, okRanges := cache.missing[coingeckoId]
	if okRanges {
		for _, r := range ranges {
			if r.TimestampFrom.Unix() <= tsUnix && tsUnix <= r.TimestampTo.Unix() {
				found, ok = false, true
			}
		}
	}

	return
}

func ParseEvents(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
) error {
	slog.Info("query events")
	rows, err := pool.Query(ctx, selectEventsQuery, userAccountId)
	if err != nil {
		return fmt.Errorf("unable to query internal txs: %w", err)
	}

	type event struct {
		Type      db.EventType        `json:"type"`
		Transfers []*db.EventTransfer `json:"transfers"`
	}

	type internalTx struct {
		id                   int32
		txId                 string
		network              db.Network
		timestamp            time.Time
		events               []*event
		solanaNativeBalances map[string]*db.SolanaNativeBalance
		solanaTokenBalances  map[string]*db.SolanaTokenBalances
		solanaTokenDecimals  map[string]uint8
	}

	var internalTxs []*internalTx

	type queuedCoingeckoToken struct {
		coingeckoId string
		timestamp   time.Time
		transfer    *db.EventTransfer
	}

	var batch pgx.Batch
	var coingeckoTokensQueue []*queuedCoingeckoToken

	handleQueriedToken := func(
		row pgx.Row,
		roundedTimestamp time.Time,
		transfer *db.EventTransfer,
	) error {
		var coingeckoId string
		var price pgtype.Text
		var missing bool
		err := row.Scan(&coingeckoId, &price, &missing)
		if errors.Is(err, pgx.ErrNoRows) || missing {
			return nil
		}
		if err != nil {
			return fmt.Errorf("unable to scan pricepoint: %w", err)
		}
		if price.Valid {
			if transfer.Price, err = decimal.NewFromString(price.String); err != nil {
				return fmt.Errorf("can not convert price to decimal for %s %d: %w", coingeckoId, len(price.String), err)
			}
			transfer.Value = transfer.Price.Mul(transfer.Amount)
			return nil
		}
		coingeckoTokensQueue = append(
			coingeckoTokensQueue,
			&queuedCoingeckoToken{
				coingeckoId,
				roundedTimestamp,
				transfer,
			},
		)
		return nil
	}

	for rows.Next() {
		itx := internalTx{
			solanaNativeBalances: make(map[string]*db.SolanaNativeBalance),
			solanaTokenBalances:  make(map[string]*db.SolanaTokenBalances),
			solanaTokenDecimals:  make(map[string]uint8),
		}
		var eventsSer, solanaNativeBalancesSer, solanaTokenBalancesSer, solanaTokenDecimalsSer json.RawMessage
		if err := rows.Scan(
			&itx.id, &itx.txId, &itx.network, &itx.timestamp,
			&eventsSer, &solanaNativeBalancesSer,
			&solanaTokenBalancesSer, &solanaTokenDecimalsSer,
		); err != nil {
			return fmt.Errorf("unable to scan internal tx: %w", err)
		}

		if err := json.Unmarshal(eventsSer, &itx.events); err != nil {
			return fmt.Errorf("unable to unmarshal events: %w", err)
		}
		if err := json.Unmarshal(solanaNativeBalancesSer, &itx.solanaNativeBalances); err != nil {
			return fmt.Errorf(
				"unable to unmarshal solana native balances: %s: %w",
				string(solanaNativeBalancesSer), err,
			)
		}
		if err := json.Unmarshal(solanaTokenBalancesSer, &itx.solanaTokenBalances); err != nil {
			return fmt.Errorf(
				"unable to unmarshal solana token balances: %s: %w",
				string(solanaTokenBalancesSer), err,
			)
		}
		if err := json.Unmarshal(solanaTokenDecimalsSer, &itx.solanaTokenDecimals); err != nil {
			return fmt.Errorf(
				"unable to unmarshal solana token decimals: %s: %w",
				string(solanaTokenDecimalsSer), err,
			)
		}

		internalTxs = append(internalTxs, &itx)

		// query prices from db or append to coingecko queue
		const hour, halfHour int64 = 60 * 60, 60 * 30
		roundedTimestamp := time.Unix(itx.timestamp.Unix()+halfHour/hour*hour, 0)

		for _, event := range itx.events {
			for _, transfer := range event.Transfers {
				if transfer.TokenSource == tokenSourceCoingecko {
					batch.Queue(
						db.GetPricepointByCoingeckoId,
						transfer.Token, roundedTimestamp,
					).QueryRow(func(row pgx.Row) error {
						return handleQueriedToken(row, roundedTimestamp, transfer)
					})
				} else {
					batch.Queue(
						db.GetPricepointByNetworkAndTokenAddress,
						roundedTimestamp, itx.network, transfer.Token,
					).QueryRow(func(row pgx.Row) error {
						return handleQueriedToken(row, roundedTimestamp, transfer)
					})
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("unable to read internal txs: %w", err)
	}

	slog.Info("query prices")
	if err := pool.SendBatch(ctx, &batch).Close(); err != nil {
		return err
	}

	slog.Info("fetch prices")
	batch = pgx.Batch{}
	// TODO: this probably should be global
	coingeckoCache := coingeckoPricesCache{
		found:   make(map[string][]*coingecko.OhlcCoinData),
		missing: make(map[string][]*coingecko.MissingPricepointsRange),
	}

	for _, token := range coingeckoTokensQueue {
		if price, found, ok := coingeckoCache.get(token.coingeckoId, token.timestamp); ok {
			if found {
				token.transfer.Price = price
				token.transfer.Value = price.Mul(token.transfer.Amount)
			}
			continue
		}

		prices, missingPrices, err := coingecko.GetCoinOhlc(
			ctx,
			token.coingeckoId, "eur", token.timestamp,
		)
		if err != nil {
			return fmt.Errorf("unable to get prices from coingecko: %w", err)
		}

		for _, price := range prices {
			if price.Timestamp.Equal(token.timestamp) {
				token.transfer.Price = price.Close
				token.transfer.Value = price.Close.Mul(token.transfer.Amount)
			}

			batch.Queue(
				db.InsertPricepoint,
				price, price.Timestamp, token.coingeckoId,
			)

			coingeckoCache.found[token.coingeckoId] = append(
				coingeckoCache.found[token.coingeckoId],
				price,
			)
		}

		for _, mp := range missingPrices {
			const setMissingPricepointQuery = "call set_missing_pricepoint($1, $2, $3)"
			batch.Queue(
				setMissingPricepointQuery,
				token.coingeckoId, mp.TimestampFrom, mp.TimestampTo,
			)

			coingeckoCache.missing[token.coingeckoId] = append(
				coingeckoCache.missing[token.coingeckoId],
				mp,
			)
		}
	}

	if err := pool.SendBatch(ctx, &batch).Close(); err != nil {
		return fmt.Errorf("unable to insert prices: %w", err)
	}

	slog.Info("process events")
	// process events
	solanaWallets := make([]string, 0)
	// NOTE: because of this query, wallet must NOT change state when parsing
	// events is in progress
	//
	// if a wallet changes state in between parse_transactions and parse_events
	// parse_transactions needs to be done again
	const selectSolanaWalletsQuery = `
		select address from wallet where	
			user_account_id = $1 and
			network = 'solana' and
			status = 'success'
	`
	rows, err = pool.Query(ctx, selectSolanaWalletsQuery, userAccountId)
	if err != nil {
		return fmt.Errorf("unable to query wallets: %w", err)
	}
	for rows.Next() {
		var address string
		if err := rows.Scan(&address); err != nil {
			return fmt.Errorf("unable to scan wallets: %w", err)
		}
		solanaWallets = append(solanaWallets, address)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("unable to read wallets: %w", err)
	}

	batch = pgx.Batch{}
	batch.Queue(
		`
			delete from event_transfer_source s where
				s.transfer_id in (
					select et.id from event_transfer et
					join
						event e on e.id = et.event_id
					join
						internal_tx itx on itx.id = e.internal_tx_id
					where
						itx.user_account_id = $1
				)
		`,
		userAccountId,
	)

	inv := inventory{
		accounts: make(map[inventoryAccountId][]*inventoryRecord),
	}

	for _, itx := range internalTxs {
		for _, event := range itx.events {
			inv.processEvent(event.Type, itx.network, event.Transfers)

			const updateTransferQuery = `
				update event_transfer set
					price = $2,
					value = $3,
					profit = $4,
					missing_amount = $5
				where
					id = $1
			`

			const insertTransferSourceQuery = `
				insert into event_transfer_source (
					transfer_id, source_transfer_id, used_amount
				) values (
					$1, $2, $3
				)
			`

			for _, t := range event.Transfers {
				batch.Queue(
					updateTransferQuery,
					t.Id, t.Price, t.Value, t.Profit, t.MissingAmount,
				)

				for _, s := range t.Sources {
					batch.Queue(
						insertTransferSourceQuery,
						t.Id, s.TransferId, s.UsedAmount,
					)
				}
			}
		}

		const insertErrorQuery = `
			insert into parser_error (
				internal_tx_id, ix_idx, origin, type, data
			) values (
				$1, $2, $3, $4, $5
			)
		`

		// validate balances
		switch itx.network {
		case db.NetworkSolana:
			for account, balance := range itx.solanaNativeBalances {
				invAccountId := inventoryAccountId{db.NetworkSolana, account, SOL_MINT_ADDRESS}
				accountBalances, ok := inv.accounts[invAccountId]

				local, expected := decimal.Zero, decimal.Zero

				// validate the balance if the account exists in the inventory
				// and if the wallet is owned even if the account does not exist
				// this allows validation if program accounts too
				if ok {
					for _, b := range accountBalances {
						local = local.Add(b.amount)
					}
					expected = newDecimalFromRawAmount(balance.Post, 9)
				} else if !ok && slices.Contains(solanaWallets, account) {
					expected = newDecimalFromRawAmount(balance.Post, 9)
				}

				if !expected.Equal(local) {
					errData, err := json.Marshal(db.ParserErrorAccountBalanceMismatch{
						Wallet:         account,
						AccountAddress: account,
						Token:          SOL_MINT_ADDRESS,
						External:       expected,
						Local:          local,
					})
					if err != nil {
						return fmt.Errorf("unable to marshal native balance error data: %w", err)
					}
					batch.Queue(
						insertErrorQuery,
						itx.id, -1, db.ErrOriginProcess, db.ParserErrorTypeAccountBalanceMismatch,
						errData,
					)
				}
			}

			for account, balance := range itx.solanaTokenBalances {
				for _, token := range balance.Tokens {
					if token.Token == SOL_MINT_ADDRESS {
						continue
					}

					invAccountId := inventoryAccountId{db.NetworkSolana, account, token.Token}
					accountBalances, ok := inv.accounts[invAccountId]

					local, expected := decimal.Zero, decimal.Zero
					decimals := itx.solanaTokenDecimals[token.Token]

					// TODO: validatin this way means that staked, lending
					// accounts need to have a prefix so the accounts do not match
					if ok {
						for _, b := range accountBalances {
							local = local.Add(b.amount)
						}
						expected = newDecimalFromRawAmount(token.Post, decimals)
					} else if !ok && slices.Contains(solanaWallets, balance.Owner) {
						expected = newDecimalFromRawAmount(token.Post, decimals)
					}

					if !expected.Equal(local) {
						errData, err := json.Marshal(db.ParserErrorAccountBalanceMismatch{
							Wallet:         balance.Owner,
							AccountAddress: account,
							Token:          token.Token,
							External:       expected,
							Local:          local,
						})
						if err != nil {
							return fmt.Errorf("unable to marshal token balance error data: %w", err)
						}
						batch.Queue(
							insertErrorQuery,
							itx.id, -1, db.ErrOriginProcess, db.ParserErrorTypeAccountBalanceMismatch,
							errData,
						)
					}
				}
			}
		}
	}

	slog.Info("insert results")
	if err := pool.SendBatch(ctx, &batch).Close(); err != nil {
		return fmt.Errorf("unable to update events: %w", err)
	}

	return nil
}
