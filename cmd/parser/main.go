package parser

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	_ "net/http/pprof"
	"slices"
	"strconv"
	"taxee/cmd/fetcher/evm"
	"taxee/pkg/assert"
	"taxee/pkg/coingecko"
	"taxee/pkg/db"
	"taxee/pkg/logger"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
)

const tokenSourceCoingecko uint16 = math.MaxUint16

func newDecimalFromRawAmount(amount uint64, decimals uint8) decimal.Decimal {
	d := decimal.NewFromBigInt(
		new(big.Int).SetUint64(amount),
		-int32(decimals),
	)
	return d
}

func solNewEvent(ctx *solContext) *db.Event {
	id, err := uuid.NewRandom()
	assert.NoErr(err, "unable to generate event id")

	return &db.Event{
		Id:        id,
		Timestamp: ctx.timestamp,
		Network:   db.NetworkSolana,
	}
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

func setEventTransfer(
	event *db.Event,
	fromWallet, toWallet,
	from, to string,
	fromInternal, toInternal bool,
	amount decimal.Decimal,
	token string,
	tokenSource uint16,
) {
	event.Type = db.EventTypeTransfer
	event.Transfers = append(event.Transfers, &db.EventTransfer{
		Direction:   getTransferEventDirection(fromInternal, toInternal),
		FromWallet:  fromWallet,
		ToWallet:    toWallet,
		FromAccount: from,
		ToAccount:   to,
		Token:       token,
		Amount:      amount,
		TokenSource: tokenSource,
	})
}

func devDeleteEvents(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
) error {
	_, err := pool.Exec(ctx, "delete from internal_tx where user_account_id = $1", userAccountId)
	if err != nil {
		return fmt.Errorf("unable to delete events")
	}

	return nil
}

func fetchCoingeckoTokens(
	ctx context.Context,
	pool *pgxpool.Pool,
) {
	coins, err := coingecko.GetCoins()
	if err != nil {
		logger.Warn("unable to update coingecko tokens: %s", err)
		return
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

	qr := pool.SendBatch(ctx, &batch)
	err = qr.Close()
	assert.NoErr(err, "unable to insert coingecko tokens")
}

func fetchDecimalsAndPrices(
	ctx context.Context,
	pool *pgxpool.Pool,
	events []*db.Event,
	evmClient *evm.Client,
	evmContexts map[db.Network]*evmContext,
) {
	///////////////
	// fetch evm decimals
	logger.Info("Fetch EVM tokens decimals")
	tokensByNetwork := make(map[db.Network][]string)

	for _, event := range events {
		for _, t := range event.Transfers {
			isEvm := t.TokenSource != tokenSourceCoingecko &&
				t.TokenSource > uint16(db.NetworkEvmStart)
			if isEvm {
				tokens := tokensByNetwork[db.Network(t.TokenSource)]
				if len(tokens) == 0 || !slices.Contains(tokens, t.Token) {
					tokens = append(tokens, t.Token)
					tokensByNetwork[db.Network(t.TokenSource)] = tokens
				}
			}
		}
	}

	evmQueriesCount := 0
	evmQueriesStart := time.Now()

	// TODO: save and use from db
	for network, tokens := range tokensByNetwork {
		requests := make([]*evm.RpcRequest, len(tokens))
		for i, token := range tokens {
			params := struct {
				To    string `json:"to"`
				Input string `json:"input"`
			}{
				To: token,
				// decimals() selector
				Input: "0x313ce567",
			}
			requests[i] = evmClient.NewRpcRequest(
				"eth_call",
				[]any{params, "latest"},
				i,
			)
		}

		evmQueriesCount += 1
		results := queryBatchUntilAllSuccessful(evmClient, network, requests)

		ctx, ok := evmContexts[network]
		assert.True(ok, "missing evm ctx %s", network.String())

		for i, result := range results {
			valueEncoded, ok := result.Data.(string)
			assert.True(ok, "invalid value type: %T", result.Data)

			token := tokens[i]

			if valueEncoded == "0x" {
				ctx.decimals[token] = 0
			} else {
				decimals, err := strconv.ParseInt(valueEncoded, 0, 8)
				assert.NoErr(err, "unable to parse decimals")
				ctx.decimals[token] = uint8(decimals)
			}
		}
	}

	logger.Info(
		"Executed %d evm queries in %d ms",
		evmQueriesCount,
		time.Since(evmQueriesStart).Milliseconds(),
	)

	///////////////
	// set evm decimals and fetch prices
	logger.Info("Set EVM tokens decimals and fetch tokens prices")

	queryPricesBatch := pgx.Batch{}
	type tokenPriceToFetch struct {
		coingeckoId string
		timestamp   int64
		price       *decimal.Decimal
		value       *decimal.Decimal
		amount      *decimal.Decimal
	}
	tokenPricesToFetch := make([]tokenPriceToFetch, 0)

	dbPriceQueriesCount := 0
	dbPriceQueriesStart := time.Now()

	handleQueriedPricepoint := func(
		row pgx.Row,
		roundedTimestamp time.Time,
		transfer *db.EventTransfer,
	) error {
		var cid, priceStr string
		var missing bool
		err := row.Scan(&cid, &priceStr, &missing)
		if errors.Is(err, pgx.ErrNoRows) {
			// NOTE: coingecko does not have this token, need to use backup
			// like birdeye or something
			return nil
		} else {
			assert.NoErr(
				err,
				fmt.Sprintf("unable to scan pricepoint: %s", transfer.Token),
			)
		}
		if missing {
			return nil
		}

		if priceStr == "" {
			tokenPricesToFetch = append(
				tokenPricesToFetch,
				tokenPriceToFetch{
					coingeckoId: cid,
					timestamp:   roundedTimestamp.Unix(),
					price:       &transfer.Price,
					value:       &transfer.Value,
					amount:      &transfer.Amount,
				},
			)
		} else {
			pricepoint, err := decimal.NewFromString(priceStr)
			assert.NoErr(err, fmt.Sprintf("invalid price: %s", priceStr))

			transfer.Price = pricepoint
			transfer.Value = pricepoint.Mul(transfer.Amount)
		}

		return nil
	}

	queryEventDataPrice := func(
		roundedTimestamp time.Time,
		network db.Network,
		transfer *db.EventTransfer,
	) {
		dbPriceQueriesCount += 1

		if transfer.TokenSource == math.MaxUint16 {
			q := queryPricesBatch.Queue(
				db.GetPricepointByCoingeckoId,
				transfer.Token,
				roundedTimestamp,
			)
			q.QueryRow(func(row pgx.Row) error {
				return handleQueriedPricepoint(
					row,
					roundedTimestamp,
					transfer,
				)
			})
		} else {
			if transfer.TokenSource > uint16(db.NetworkEvmStart) {
				// TODO: cant just use network????
				n := db.Network(transfer.TokenSource)
				evmCtx, ok := evmContexts[n]
				assert.True(ok, "missing evm context %s", n.String())

				decimals, ok := evmCtx.decimals[transfer.Token]
				assert.True(
					ok,
					"missing decimals for evm %d token: %s",
					transfer.TokenSource, transfer.Token,
				)

				transfer.Amount = decimal.NewFromBigInt(
					transfer.Amount.BigInt(),
					-int32(decimals),
				)
			}

			q := queryPricesBatch.Queue(
				db.GetPricepointByNetworkAndTokenAddress,
				roundedTimestamp,
				network,
				transfer.Token,
			)
			q.QueryRow(func(row pgx.Row) error {
				return handleQueriedPricepoint(
					row,
					roundedTimestamp,
					transfer,
				)
			})
		}
	}

	for _, event := range events {
		const hour, halfHour = 60 * 60, 60 * 30
		roundedTimestamp := time.Unix(
			(event.Timestamp.Unix()+halfHour)/int64(hour)*int64(hour),
			0,
		)

		for _, t := range event.Transfers {
			queryEventDataPrice(roundedTimestamp, event.Network, t)
		}
	}

	br := pool.SendBatch(ctx, &queryPricesBatch)
	err := br.Close()
	assert.NoErr(err, "unable to query prices")

	logger.Info(
		"Executed %d db price queries in %d ms",
		dbPriceQueriesCount,
		time.Since(dbPriceQueriesStart).Milliseconds(),
	)

	type coingeckoTokenPriceId struct {
		coingeckoId string
		timestamp   int64
	}
	coingeckoTokensPrices := make(map[coingeckoTokenPriceId]decimal.Decimal)

	coingeckoQueriesCount := 0
	coingeckoQueriesStart := time.Now()

	for _, token := range tokenPricesToFetch {
		timestampFrom := time.Unix(token.timestamp, 0)
		price, ok := coingeckoTokensPrices[coingeckoTokenPriceId{
			token.coingeckoId, token.timestamp,
		}]

		if !ok {
			coingeckoQueriesCount += 1
			coingeckoPrices, timestampTo, err := coingecko.GetCoinOhlc(
				token.coingeckoId,
				coingecko.FiatCurrencyEur,
				timestampFrom,
			)
			assert.NoErr(err, "unable to get coingecko prices")

			if len(coingeckoPrices) > 0 {
				insertPricesBatch := pgx.Batch{}
				for _, p := range coingeckoPrices {
					insertPricesBatch.Queue(
						db.InsertPricepoint,
						p.Close,
						p.Timestamp,
						token.coingeckoId,
					)

					id := coingeckoTokenPriceId{token.coingeckoId, p.Timestamp.Unix()}
					coingeckoTokensPrices[id] = p.Close
				}
				br := pool.SendBatch(ctx, &insertPricesBatch)
				err := br.Close()
				assert.NoErr(err, "unable to insert coingecko prices")

				if p := coingeckoPrices[0]; p.Timestamp.Equal(timestampFrom) {
					price, ok = p.Close, true
				}
			} else {
				_, err := pool.Exec(
					ctx,
					db.SetMissingPricepoint,
					token.coingeckoId,
					timestampFrom,
					timestampTo,
				)
				assert.NoErr(err, "unable to set missing pricepoint")
				continue
			}
		}

		*token.price = price
		*token.value = price.Mul(*token.amount)
	}

	logger.Info(
		"Executed %d coingecko queries in %d ms",
		coingeckoQueriesCount,
		time.Since(coingeckoQueriesStart).Milliseconds(),
	)
}

func Parse(
	ctx context.Context,
	pool *pgxpool.Pool,
	evmClient *evm.Client,
	userAccountId int32,
) {
	logger.Info("Update coingecko tokens")
	fetchCoingeckoTokens(ctx, pool)

	err := devDeleteEvents(ctx, pool, userAccountId)
	assert.NoErr(err, "")

	/////////////////
	// Init
	const createInternalTxsQuery = `
		insert into internal_tx (
			position, user_account_id, network, tx_id, timestamp
		) select	
			row_number() over (
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
			) as position,
			$1,
			tx.network,
			tx_ref.tx_id,
			tx.timestamp
		from
			tx_ref
		join
			tx on tx.id = tx_ref.tx_id
		where
			tx_ref.user_account_id = $1
		returning
			id, tx_id
	`
	internalTxsByTxId := make(map[string]int32)
	if rows, err := pool.Query(ctx, createInternalTxsQuery, userAccountId); err == nil {
		for rows.Next() {
			var id int32
			var txId string
			err := rows.Scan(&id, &txId)
			assert.NoErr(err, "unable to scan internal_tx id")
			internalTxsByTxId[txId] = id
		}
	} else {
		assert.True(false, "unable to create internal txs: %s", err)
	}

	parserErrors := make([]*db.ParserError, 0)

	logger.Info("Init parser state")
	solCtx := solContext{
		accounts:  make(map[string][]accountLifetime),
		errors:    &parserErrors,
		errOrigin: db.ErrOriginPreprocess,
	}
	evmContexts := make(map[db.Network]*evmContext)

	for i := db.NetworkEvmStart + 1; i < db.NetworksCount; i += 1 {
		evmContexts[i] = &evmContext{
			contracts: make(map[string][]evmContractImplementation),
			network:   i,
			decimals:  make(map[string]uint8),
		}
	}

	wallets, err := db.GetWallets(ctx, pool, userAccountId)
	assert.NoErr(err, "")

	for _, w := range wallets {
		switch {
		case w.Network == db.NetworkSolana:
			solCtx.wallets = append(solCtx.wallets, w.Address)
		case w.Network > db.NetworkEvmStart:
			ctx, ok := evmContexts[w.Network]
			assert.True(ok, "missing evm context %s", w.Network.String())
			ctx.wallets = append(ctx.wallets, w.Address)
		}
	}

	// TODO:
	// pagination ??
	// 700 txs ~= 6.4mb of allocations

	txs, err := db.GetParsableTransactions(ctx, pool, userAccountId)
	assert.NoErr(err, "")

	///////////////////
	// preprocess txs
	logger.Info("Preprocess txs")

	evmAddresses := make(map[db.Network]map[string][]uint64)

	for _, tx := range txs {
		if tx.Err {
			continue
		}

		switch txData := tx.Data.(type) {
		case *db.SolanaTransactionData:
			solCtx.txId = tx.Id
			solCtx.timestamp = tx.Timestamp
			solCtx.slot = txData.Slot
			solCtx.nativeBalances = txData.NativeBalances
			solCtx.tokenBalances = txData.TokenBalances

			solPreprocessTx(&solCtx, txData.Instructions)
		case *db.EvmTransactionData:
			addresses, ok := evmAddresses[tx.Network]
			if !ok {
				addresses = make(map[string][]uint64)
			}

			addresses[txData.To] = append(addresses[txData.To], txData.Block)
			for _, itx := range txData.InternalTxs {
				addresses[itx.To] = append(addresses[itx.To], txData.Block)
			}

			evmAddresses[tx.Network] = addresses
		}
	}

	logger.Info("Identify EVM contracts")
	for network, addresses := range evmAddresses {
		ctx, ok := evmContexts[network]
		assert.True(ok, "missing evm context %s", network.String())
		evmIdentifyContracts(
			evmClient,
			network,
			addresses,
			ctx,
		)
	}

	///////////////////
	// process txs
	logger.Info("Process txs")

	newFeeEvent := func(
		tx *db.TransactionRow,
		from, token string,
		tokenSource uint16,
		amount decimal.Decimal,
	) *db.Event {
		id, err := uuid.NewRandom()
		assert.NoErr(err, "unable to generate event id")
		return &db.Event{
			Id:        id,
			Timestamp: tx.Timestamp,
			Network:   tx.Network,
			App:       "native",
			Method:    "fee",
			Type:      db.EventTypeTransfer,
			Transfers: []*db.EventTransfer{{
				Direction:   db.EventTransferOutgoing,
				FromWallet:  from,
				FromAccount: from,
				Token:       token,
				Amount:      amount,
				TokenSource: tokenSource,
			}},
		}
	}

	events := make([]*db.Event, 0)
	groupedEvents := make(map[string][]*db.Event)
	solCtx.errOrigin = db.ErrOriginProcess

	for _, tx := range txs {
		eventsGroup := make([]*db.Event, 0)

		switch txData := tx.Data.(type) {
		case *db.SolanaTransactionData:
			if txData.Fee.GreaterThan(decimal.Zero) && solCtx.walletOwned(txData.Signer) {
				eventsGroup = append(eventsGroup, newFeeEvent(
					tx, txData.Signer, SOL_MINT_ADDRESS,
					uint16(db.NetworkSolana), txData.Fee,
				))
			}

			if !tx.Err {
				solCtx.txId = tx.Id
				solCtx.timestamp = tx.Timestamp
				solCtx.slot = txData.Slot
				solCtx.tokensDecimals = txData.TokenDecimals

				for ixIdx, ix := range txData.Instructions {
					solCtx.ixIdx = uint32(ixIdx)
					solProcessIx(&eventsGroup, &solCtx, ix)
				}
			}
		case *db.EvmTransactionData:
			ctx, ok := evmContexts[tx.Network]
			assert.True(ok, "missing evm context %s", tx.Network.String())

			if txData.Fee.GreaterThan(decimal.Zero) && evmWalletOwned(ctx, txData.From) {
				event := newFeeEvent(
					tx, txData.From, "ethereum",
					tokenSourceCoingecko, txData.Fee,
				)
				// event.IxIdx = 0
				eventsGroup = append(eventsGroup, event)
			}

			if !tx.Err {
				ctx.timestamp = tx.Timestamp
				ctx.txId = tx.Id

				evmProcessTx(ctx, &eventsGroup, txData)
			}
		}

		if len(eventsGroup) > 0 {
			groupedEvents[tx.Id] = eventsGroup
			for _, event := range eventsGroup {
				events = append(events, event)
			}
		}
	}

	fetchDecimalsAndPrices(
		ctx,
		pool,
		events,
		evmClient,
		evmContexts,
	)

	///////////////
	// process events
	logger.Info("Process events")

	inv := inventory{
		accounts: make(map[inventoryAccountId][]*inventoryRecord),
	}
	batch := pgx.Batch{}
	eventPosition := 0

	for _, tx := range txs {
		txEvents, ok := groupedEvents[tx.Id]
		if !ok {
			continue
		}

		internalTxId, ok := internalTxsByTxId[tx.Id]
		assert.True(ok, "missing internal_tx id for: %s", tx.Id)

		for _, event := range txEvents {
			for _, t := range event.Transfers {
				if t.Id == uuid.Nil {
					var err error
					t.Id, err = uuid.NewRandom()
					assert.NoErr(err, "unable to generate transfer uuid")
				}
			}

			inv.processEvent(event)

			const insertEventQuery = `
				insert into event (
					id, position, internal_tx_id,
					app, method, type
				) values (
					$1, $2, $3, $4, $5, $6
				)
			`
			// if event.PrecedingEvents == nil {
			// 	event.PrecedingEvents = make([]uuid.UUID, 0)
			// }

			batch.Queue(
				insertEventQuery,
				// args
				event.Id,
				eventPosition,
				internalTxId,
				event.App,
				event.Method,
				event.Type,
			)
			eventPosition += 1

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
			for i, t := range event.Transfers {
				batch.Queue(
					insertTransferQuery,
					// args
					t.Id, event.Id, i,
					t.Direction, t.FromWallet, t.FromAccount, t.ToWallet, t.ToAccount,
					t.Token, t.Amount, t.TokenSource,
					t.Price, t.Value, t.Profit, t.MissingAmount,
				)
			}
		}

		/////////////////
		// validate sol balances

		switch tx.Data.(type) {
		case *db.SolanaTransactionData:
			txData, ok := tx.Data.(*db.SolanaTransactionData)
			assert.True(ok, "invalid transaction data: %T", tx.Data)

			for account, balance := range txData.NativeBalances {
				if !solCtx.walletOwned(account) {
					continue
				}

				invAccountId := inventoryAccountId{db.NetworkSolana, account, SOL_MINT_ADDRESS}
				accountBalances := inv.accounts[invAccountId]

				sum := decimal.Zero
				for _, b := range accountBalances {
					sum = sum.Add(b.amount)
				}

				expectedBalance := newDecimalFromRawAmount(balance.Post, 9)

				if !expectedBalance.Equal(sum) {
					err := db.ParserError{
						TxId:   tx.Id,
						Origin: db.ErrOriginProcess,
						Type:   db.ParserErrorTypeAccountBalanceMismatch,
						Data: &db.ParserErrorAccountBalanceMismatch{
							Wallet:         account,
							AccountAddress: account,
							Token:          SOL_MINT_ADDRESS,
							External:       expectedBalance,
							Local:          sum,
						},
					}
					parserErrors = append(parserErrors, &err)
				}
			}

			for account, balance := range txData.TokenBalances {
				for _, token := range balance.Tokens {
					if token.Token == SOL_MINT_ADDRESS {
						continue
					}
					if !solCtx.walletOwned(balance.Owner) {
						continue
					}

					invAccountId := inventoryAccountId{db.NetworkSolana, account, token.Token}
					accountBalances := inv.accounts[invAccountId]

					sum := decimal.Zero
					for _, b := range accountBalances {
						sum = sum.Add(b.amount)
					}

					decimals := txData.TokenDecimals[token.Token]
					expectedAmount := newDecimalFromRawAmount(token.Post, decimals)

					if !expectedAmount.Equal(sum) {
						err := db.ParserError{
							TxId:   tx.Id,
							Origin: db.ErrOriginProcess,
							Type:   db.ParserErrorTypeAccountBalanceMismatch,
							Data: &db.ParserErrorAccountBalanceMismatch{
								Wallet:         balance.Owner,
								AccountAddress: account,
								Token:          token.Token,
								External:       expectedAmount,
								Local:          sum,
							},
						}
						parserErrors = append(parserErrors, &err)
					}
				}
			}
		}
	}

	for _, error := range parserErrors {
		internalTxId, ok := internalTxsByTxId[error.TxId]
		assert.True(ok, "missing internal_tx for %s", error.TxId)

		data, err := json.Marshal(error.Data)
		assert.NoErr(err, "unable to marshal error data")

		const insertErrorQuery = `
			insert into parser_error (
				internal_tx_id, ix_idx, origin, type, data
			) values (
				$1, $2, $3, $4, $5
			)
		`
		batch.Queue(
			insertErrorQuery,
			internalTxId,
			error.IxIdx,
			error.Origin,
			error.Type,
			data,
		)
	}

	br := pool.SendBatch(ctx, &batch)
	err = br.Close()
	assert.NoErr(err, "unable to insert events")
}
