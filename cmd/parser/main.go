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
	_, isPrice := n.Data.(*db.ParserErrorMissingPrice)
	assert.True(!isPrice, "missing price error should not be unique")
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

func appendMissingPriceError(
	errors *[]*db.ParserError,
	txId string,
	ixIdx uint32,
	token string,
	timestamp time.Time,
) {
	e2Data := db.ParserErrorMissingPrice{
		Token:     token,
		Timestamp: timestamp,
	}
	e2 := db.ParserError{
		TxId:  txId,
		IxIdx: ixIdx,
		Type:  db.ParserErrorTypeMissingPrice,
		Data:  &e2Data,
	}

	if len(*errors) == 0 {
		*errors = append(*errors, &e2)
		return
	}

	// NOTE: errors are appended chronologically by time, so check from last
	for i := len(*errors) - 1; i >= 0; i -= 1 {
		e1Data := (*errors)[i].Data.(*db.ParserErrorMissingPrice)

		if e2Data.Timestamp != e1Data.Timestamp {
			*errors = append(*errors, &e2)
			return
		}
		if e2Data.Token == e1Data.Token {
			return
		}
	}

}

func devDeleteParsed(
	ctx context.Context,
	pool *pgxpool.Pool,
	userAccountId int32,
) error {
	const q = "call dev_delete_parsed($1)"
	_, err := pool.Exec(ctx, q, userAccountId)
	if err != nil {
		return fmt.Errorf("unable to call dev_delete_parsed: %w", err)
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

	fetchCoingeckoTokens(ctx, pool)

	if fresh {
		err := devDeleteParsed(ctx, pool, userAccountId)
		assert.NoErr(err, "")
	}

	/////////////////
	// Init

	solCtx := solanaContext{
		accounts:         make(map[string][]accountLifetime),
		preprocessErrors: make(map[string][]*db.ParserError),
		processErrors:    make(map[string][]*db.ParserError),
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

	txs, err := db.GetTransactions(ctx, pool, userAccountId)
	assert.NoErr(err, "")

	///////////////////
	// preprocess txs

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

			insertErrorsBatch.Queue(
				db.InsertParserError,
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
	tokensByNetwork := make(map[db.Network][]string)

	for _, event := range events {
		var token string
		var tokenSource uint16

		switch data := event.Data.(type) {
		case *db.EventTransferInternal:
			token = data.Token
			tokenSource = data.TokenSource
		case *db.EventTransfer:
			token = data.Token
			tokenSource = data.TokenSource
		default:
			assert.True(false, "unknown event data: %T %#v", data, *event)
			continue
		}

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

	queryPricesBatch := pgx.Batch{}
	type tokenPriceToFetch struct {
		coingeckoId string
		timestamp   int64
		eventIdx    int
	}
	tokenPricesToFetch := make([]tokenPriceToFetch, 0)
	priceErrors := make([]*db.ParserError, 0)

	for eventIdx, event := range events {
		var token string
		var tokenSource uint16

		switch data := event.Data.(type) {
		case *db.EventTransferInternal:
			token = data.Token
			tokenSource = data.TokenSource
		case *db.EventTransfer:
			token = data.Token
			tokenSource = data.TokenSource
		default:
			assert.True(false, "unknown event data: %T %#v", data, *event)
			continue
		}

		const hour, halfHour = 60 * 60, 60 * 30
		roundedTimestamp := time.Unix(
			(event.Timestamp.Unix()+halfHour)/int64(hour)*int64(hour),
			0,
		)

		if tokenSource == tokenSourceCoingecko {
			q := queryPricesBatch.Queue(
				db.GetPricepointByCoingeckoId,
				token,
				roundedTimestamp,
			)
			q.QueryRow(func(row pgx.Row) error {
				var cid, priceStr string
				err := row.Scan(&cid, &priceStr)
				if errors.Is(err, pgx.ErrNoRows) {
					appendMissingPriceError(
						&priceErrors,
						event.TxId,
						uint32(event.IxIdx),
						token,
						roundedTimestamp,
					)
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
							eventIdx:    eventIdx,
						},
					)
				} else {
					logger.Info("Found coingecko pricepoint in db: %s %s", token, roundedTimestamp)
					pricepoint, err := decimal.NewFromString(priceStr)
					assert.NoErr(err, fmt.Sprintf("invalid price: %s", priceStr))

					event.Data.SetPrice(pricepoint)
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

				event.Data.SetAmountDecimals(decimals)
			}

			q := queryPricesBatch.Queue(
				db.GetPricepointByNetworkAndTokenAddress,
				roundedTimestamp,
				event.Network,
				token,
			)
			q.QueryRow(func(row pgx.Row) error {
				var coingeckoId, priceStr string
				err := row.Scan(&coingeckoId, &priceStr)
				if errors.Is(err, pgx.ErrNoRows) {
					appendMissingPriceError(
						&priceErrors,
						event.TxId,
						uint32(event.IxIdx),
						token,
						roundedTimestamp,
					)
					return nil
				} else {
					assert.NoErr(err, fmt.Sprintf("unable to scan pricepoint: %s", token))
				}

				if priceStr != "" {
					price, err := decimal.NewFromString(priceStr)
					assert.NoErr(err, fmt.Sprintf("invalid price: %s", priceStr))
					logger.Info("Found pricepoint in db: %s %s", coingeckoId, roundedTimestamp)
					event.Data.SetPrice(price)
				} else {
					tokenPricesToFetch = append(
						tokenPricesToFetch,
						tokenPriceToFetch{
							coingeckoId,
							roundedTimestamp.Unix(),
							eventIdx,
						},
					)
				}
				return nil
			})
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
					logger.Info("Fetched pricepoint from coingecko: %s %s", token.coingeckoId, ts)
					price, ok = p.Close, true
				}
			} else {
				event := events[token.eventIdx]
				appendMissingPriceError(
					&priceErrors,
					event.TxId,
					uint32(event.IxIdx),
					token.coingeckoId,
					ts,
				)
			}
		} else {
			logger.Info("Found pricepoint in coingecko prices: %s %s", token.coingeckoId, ts)
		}

		event := events[token.eventIdx]
		event.Data.SetPrice(price)
	}

	insertErrorsBatch = pgx.Batch{}
	for _, priceError := range priceErrors {
		dataSerialized, err := json.Marshal(priceError.Data)
		assert.NoErr(err, "unable to marshal data")
		insertErrorsBatch.Queue(
			db.InsertParserError,
			userAccountId,
			priceError.TxId,
			priceError.IxIdx,
			db.ErrOriginProcess,
			priceError.Type,
			dataSerialized,
		)
	}

	br = pool.SendBatch(ctx, &insertErrorsBatch)
	err = br.Close()
	assert.NoErr(err, "unable to insert price errors")

	///////////////
	// process events

	inv := inventory{
		accounts: make(map[inventoryAccountId][]*inventoryAccount),
	}
	eventsBatch := pgx.Batch{}
	for eventIdx, event := range events {
		inv.processEvent(event)

		_, err := db.EnqueueInsertEvent(
			&eventsBatch,
			userAccountId,
			int32(eventIdx),
			event,
		)
		assert.NoErr(err, "")
	}

	br = pool.SendBatch(ctx, &eventsBatch)
	err = br.Close()
	assert.NoErr(err, "unable to insert events")
}
