package parser

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/http"
	_ "net/http/pprof"
	"os"
	"slices"
	"strconv"
	"sync"
	"taxee/cmd/fetcher/evm"
	"taxee/pkg/assert"
	"taxee/pkg/coingecko"
	"taxee/pkg/db"
	"taxee/pkg/logger"
	"time"

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

func setEventTransfer(
	event *db.Event,
	fromWallet, toWallet,
	from, to string,
	fromInternal, toInternal bool,
	amount decimal.Decimal,
	token string,
	tokenSource uint16,
) {
	switch {
	case fromInternal && toInternal:
		event.Type = db.EventTypeTransferInternal
		event.Data = &db.EventTransferInternal{
			FromWallet:  fromWallet,
			ToWallet:    toWallet,
			FromAccount: from,
			ToAccount:   to,
			Token:       token,
			Amount:      amount,
			TokenSource: tokenSource,
		}
	case fromInternal:
		event.Type = db.EventTypeTransfer
		event.Data = &db.EventTransfer{
			Direction:   db.EventTransferOutgoing,
			Wallet:      fromWallet,
			Account:     from,
			Token:       token,
			Amount:      amount,
			TokenSource: tokenSource,
		}
	case toInternal:
		event.Type = db.EventTypeTransfer
		event.Data = &db.EventTransfer{
			Direction:   db.EventTransferIncoming,
			Wallet:      toWallet,
			Account:     to,
			Token:       token,
			Amount:      amount,
			TokenSource: tokenSource,
		}
	}
}

func appendErrUnique(
	errors map[string][]*db.ParserError,
	n *db.ParserError,
	address string,
) {
	accountErrors, ok := errors[address]

	isEq := func(e1, e2 any) bool {
		if d1, ok := e1.(*db.ParserErrorMissingAccount); ok {
			if d2, ok := e2.(*db.ParserErrorMissingAccount); ok {
				return d1.AccountAddress == d2.AccountAddress
			}
			return false
		}

		if d1, ok := e1.(*db.ParserErrorAccountBalanceMismatch); ok {
			if d2, ok := e2.(*db.ParserErrorAccountBalanceMismatch); ok {
				return d1.AccountAddress == d2.AccountAddress &&
					d1.Expected.Equal(d2.Expected) &&
					d1.Real.Equal(d2.Real)
			}
			return false
		}

		if d1, ok := e1.(*db.ParserErrorAccountDataMismatch); ok {
			if d2, ok := e2.(*db.ParserErrorAccountDataMismatch); ok {
				return d1.AccountAddress == d2.AccountAddress &&
					d1.Message == d2.Message
			}
			return false
		}

		return false
	}

	if !ok && len(accountErrors) == 0 {
		errors[address] = append(errors[address], n)
	} else {
		l := accountErrors[len(accountErrors)-1]
		if !isEq(n.Data, l.Data) {
			errors[address] = append(errors[address], n)
		}
	}
}

func devDeleteEvents(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
) error {
	batch := pgx.Batch{}
	batch.Queue("delete from event where user_account_id = $1", userAccountId)
	batch.Queue("delete from parser_error where user_account_id = $1", userAccountId)

	br := pool.SendBatch(ctx, &batch)
	err := br.Close()
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
	assert.NoErr(err, "unable to get coingecko coins")

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

func Parse(
	ctx context.Context,
	pool *pgxpool.Pool,
	evmClient *evm.Client,
	userAccountId int32,
	fresh bool,
) {
	if os.Getenv("MEM_PROF") == "1" {
		var wg sync.WaitGroup
		wg.Add(1)
		defer wg.Wait()

		go func() {
			defer wg.Done()
			fmt.Println("Pprof available at: http://localhost:6060/debug/pprof")
			err := http.ListenAndServe("localhost:6060", nil)
			assert.NoErr(err, "")
		}()
	}

	logger.Info("Update congecko tokens")
	fetchCoingeckoTokens(ctx, pool)

	if fresh {
		err := devDeleteEvents(ctx, pool, userAccountId)
		assert.NoErr(err, "")
	}

	/////////////////
	// Init

	logger.Info("Init parser state")
	solCtx := solanaContext{
		accounts:         make(map[string][]accountLifetime),
		preprocessErrors: make(map[string][]*db.ParserError),
		processErrors:    make(map[string][]*db.ParserError),
	}
	evmContexts := make(map[db.Network]*evmContext)

	for i := db.NetworkEvmStart + 1; i < db.NetworksCount; i += 1 {
		evmContexts[i] = &evmContext{
			contracts:     make(map[string][]evmContractImplementation),
			network:       i,
			decimals:      make(map[string]uint8),
			processErrors: make(map[string][]*db.ParserError),
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

	txs, err := db.GetTransactions(ctx, pool, userAccountId)
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
	// insert preprocess errors
	insertErrorsBatch := pgx.Batch{}
	for _, preprocessErrs := range solCtx.preprocessErrors {
		for _, preprocessErr := range preprocessErrs {
			dataSerialized, err := json.Marshal(preprocessErr.Data)
			assert.NoErr(err, "unable to marshal preprocess error data")

			const insertErrorQuery = `
				insert into parser_error (
					user_account_id, tx_id, ix_idx, origin, type, data
				) values (
					$1, $2, $3, $4, $5, $6
				)
			`
			insertErrorsBatch.Queue(
				insertErrorQuery,
				userAccountId,
				preprocessErr.TxId,
				preprocessErr.IxIdx,
				db.ErrOriginPreprocess,
				preprocessErr.Type,
				dataSerialized,
			)
		}
	}
	br := pool.SendBatch(ctx, &insertErrorsBatch)
	err = br.Close()
	assert.NoErr(err, "unable to insert preprocess errors")

	///////////////////
	// process txs
	logger.Info("Process txs")

	events := make([]*db.Event, 0)

	for _, tx := range txs {
		if tx.Err {
			continue
		}

		switch txData := tx.Data.(type) {
		case *db.SolanaTransactionData:
			solCtx.txId = tx.Id
			solCtx.timestamp = tx.Timestamp
			solCtx.slot = txData.Slot
			solCtx.tokensDecimals = txData.TokenDecimals

			for ixIdx, ix := range txData.Instructions {
				solCtx.ixIdx = uint32(ixIdx)
				solProcessIx(&events, &solCtx, ix)
			}
		case *db.EvmTransactionData:
			ctx, ok := evmContexts[tx.Network]
			assert.True(ok, "missing evm context %s", tx.Network.String())
			ctx.timestamp = tx.Timestamp
			ctx.txId = tx.Id

			evmProcessTx(ctx, &events, txData)
		}
	}

	///////////////
	// fetch evm decimals
	logger.Info("Fetch EVM tokens decimals")
	tokensByNetwork := make(map[db.Network][]string)

	appendEvmTokensByNetwork := func(token string, tokenSource uint16) {
		isEvm := tokenSource != tokenSourceCoingecko &&
			tokenSource > uint16(db.NetworkEvmStart)
		if isEvm {
			tokens := tokensByNetwork[db.Network(tokenSource)]
			if len(tokens) == 0 || !slices.Contains(tokens, token) {
				tokens = append(tokens, token)
				tokensByNetwork[db.Network(tokenSource)] = tokens
			}
		}
	}

	for _, event := range events {
		switch data := event.Data.(type) {
		case *db.EventTransferInternal:
			appendEvmTokensByNetwork(data.Token, data.TokenSource)
		case *db.EventTransfer:
			appendEvmTokensByNetwork(data.Token, data.TokenSource)
		case *db.EventSwap:
			for _, t := range data.Outgoing {
				appendEvmTokensByNetwork(t.Token, t.TokenSource)
			}
			for _, t := range data.Incoming {
				appendEvmTokensByNetwork(t.Token, t.TokenSource)
			}
		default:
			assert.True(false, "unknown event data: %T %#v", data, *event)
			continue
		}
	}

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

	queryEventDataPrice := func(
		roundedTimestamp time.Time,
		network db.Network,
		token string,
		tokenSource uint16,
		price, value, amount *decimal.Decimal,
	) {
		if tokenSource == math.MaxUint16 {
			q := queryPricesBatch.Queue(
				db.GetPricepointByCoingeckoId,
				token,
				roundedTimestamp,
			)
			q.QueryRow(func(row pgx.Row) error {
				var cid, priceStr string
				err := row.Scan(&cid, &priceStr)
				if errors.Is(err, pgx.ErrNoRows) {
					// ERROR
					return nil
				} else {
					assert.NoErr(
						err,
						fmt.Sprintf("unable to scan pricepoint: %s", token),
					)
				}

				if priceStr == "" {
					tokenPricesToFetch = append(
						tokenPricesToFetch,
						tokenPriceToFetch{
							coingeckoId: token,
							timestamp:   roundedTimestamp.Unix(),
							price:       price,
							value:       value,
							amount:      amount,
						},
					)
				} else {
					// logger.Info("Found coingecko pricepoint in db: %s %s", token, roundedTimestamp)
					pricepoint, err := decimal.NewFromString(priceStr)
					assert.NoErr(err, fmt.Sprintf("invalid price: %s", priceStr))

					*price = pricepoint
					*value = pricepoint.Mul(*amount)
				}

				return nil
			})
		} else {
			if tokenSource > uint16(db.NetworkEvmStart) {
				n := db.Network(tokenSource)
				evmCtx, ok := evmContexts[n]
				assert.True(ok, "missing evm context %s", n.String())

				decimals, ok := evmCtx.decimals[token]
				assert.True(
					ok,
					"missing decimals for evm %d token: %s",
					tokenSource, token,
				)

				*amount = decimal.NewFromBigInt(
					amount.BigInt(),
					-int32(decimals),
				)
			}

			q := queryPricesBatch.Queue(
				db.GetPricepointByNetworkAndTokenAddress,
				roundedTimestamp,
				network,
				token,
			)
			q.QueryRow(func(row pgx.Row) error {
				var coingeckoId, priceStr string
				err := row.Scan(&coingeckoId, &priceStr)
				if errors.Is(err, pgx.ErrNoRows) {
					// ERROR
					return nil
				} else {
					assert.NoErr(err, fmt.Sprintf("unable to scan pricepoint: %s", token))
				}

				if priceStr == "" {
					tokenPricesToFetch = append(
						tokenPricesToFetch,
						tokenPriceToFetch{
							coingeckoId: coingeckoId,
							timestamp:   roundedTimestamp.Unix(),
							price:       price,
							value:       value,
							amount:      amount,
						},
					)
				} else {
					pricepoint, err := decimal.NewFromString(priceStr)
					assert.NoErr(err, fmt.Sprintf("invalid price: %s", priceStr))
					// logger.Info("Found pricepoint in db: %s %s", coingeckoId, roundedTimestamp)

					*price = pricepoint
					*value = pricepoint.Mul(*amount)
				}
				return nil
			})
		}
	}

	for _, event := range events {
		const hour, halfHour = 60 * 60, 60 * 30
		roundedTimestamp := time.Unix(
			(event.Timestamp.Unix()+halfHour)/int64(hour)*int64(hour),
			0,
		)

		switch data := event.Data.(type) {
		case *db.EventTransferInternal:
			queryEventDataPrice(
				roundedTimestamp,
				event.Network,
				data.Token,
				data.TokenSource,
				&data.Price, &data.Value, &data.Amount,
			)
		case *db.EventTransfer:
			queryEventDataPrice(
				roundedTimestamp,
				event.Network,
				data.Token,
				data.TokenSource,
				&data.Price, &data.Value, &data.Amount,
			)
		case *db.EventSwap:
			for _, transfer := range data.Outgoing {
				queryEventDataPrice(
					roundedTimestamp,
					event.Network,
					transfer.Token,
					transfer.TokenSource,
					&transfer.Price, &transfer.Value, &transfer.Amount,
				)
			}
		default:
			assert.True(false, "unknown event data: %T %#v", data, *event)
			continue
		}
	}

	br = pool.SendBatch(ctx, &queryPricesBatch)
	err = br.Close()
	assert.NoErr(err, "unable to query prices")

	type coingeckoTokenPriceId struct {
		coingeckoId string
		timestamp   int64
	}
	coingeckoTokensPrices := make(map[coingeckoTokenPriceId]decimal.Decimal)

	for _, token := range tokenPricesToFetch {
		ts := time.Unix(token.timestamp, 0)
		price, ok := coingeckoTokensPrices[coingeckoTokenPriceId{
			token.coingeckoId, token.timestamp,
		}]

		if !ok {
			coingeckoPrices, err := coingecko.GetCoinOhlc(
				token.coingeckoId,
				coingecko.FiatCurrencyEur,
				ts,
			)
			assert.NoErr(err, "unable to get coingecko prices")

			if len(coingeckoPrices) > 0 {
				insertPricesBatch := pgx.Batch{}
				for _, p := range coingeckoPrices {
					const insertPricepoint = `
						insert into pricepoint (
							price, timestamp, coingecko_id
						) values (
							$1, $2, $3
						) on conflict (timestamp, coingecko_id) do nothing
					`
					insertPricesBatch.Queue(
						insertPricepoint,
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

				if p := coingeckoPrices[0]; p.Timestamp.Equal(ts) {
					// logger.Info("Fetched pricepoint from coingecko: %s %s", token.coingeckoId, ts)
					price, ok = p.Close, true
				}
			} else {
				// ERROR
				continue
			}
		} else {
			// logger.Info("Found pricepoint in coingecko prices: %s %s", token.coingeckoId, ts)
		}

		*token.price = price
		*token.value = price.Mul(*token.amount)
	}

	///////////////
	// process events
	logger.Info("Process events")

	inv := inventory{
		accounts: make(map[inventoryAccountId][]*inventoryAccount),
	}
	eventsBatch := pgx.Batch{}
	for _, event := range events {
		switch {
		case event.Network == db.NetworkSolana:
			inv.processEvent(event, solCtx.processErrors)
		case event.Network > db.NetworkEvmStart:
			ctx := evmContexts[event.Network]
			inv.processEvent(event, ctx.processErrors)
		}

		eventData, err := json.Marshal(event.Data)
		assert.NoErr(err, "unable to marshal event data")
		const insertEventQuery = `
			insert into event (
				user_account_id, tx_id, ix_idx, idx,
				ui_app_name, ui_method_name, type, data
			) values (
				$1, $2, $3, $4, $5, $6, $7, $8
			)
		`
		eventsBatch.Queue(
			insertEventQuery,
			userAccountId,
			event.TxId,
			event.IxIdx,
			event.Idx,
			event.UiAppName,
			event.UiMethodName,
			event.Type,
			eventData,
		)
	}

	batchAppendErrors := func(errors []*db.ParserError) {
		for _, error := range errors {
			data, err := json.Marshal(error.Data)
			assert.NoErr(err, "unable to marshal error data")

			const insertErrorQuery = `
				insert into parser_error (
					user_account_id, tx_id, ix_idx, event_idx, 
					origin, type, data
				) values (
					$1, $2, $3, $4, $5, $6, $7
				)
			`
			eventsBatch.Queue(
				insertErrorQuery,
				// args
				userAccountId,
				error.TxId, error.IxIdx, error.EventIdx,
				db.ErrOriginProcess, error.Type, data,
			)
		}
	}

	for _, errors := range solCtx.processErrors {
		batchAppendErrors(errors)
	}
	for _, ctx := range evmContexts {
		for _, errors := range ctx.processErrors {
			batchAppendErrors(errors)
		}
	}

	br = pool.SendBatch(ctx, &eventsBatch)
	err = br.Close()
	assert.NoErr(err, "unable to insert events")
}
