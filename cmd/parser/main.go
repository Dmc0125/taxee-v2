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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
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
			Account:     from,
			Token:       token,
			Amount:      amount,
			TokenSource: tokenSource,
		}
	case toInternal:
		event.Type = db.EventTypeTransfer
		event.Data = &db.EventTransfer{
			Direction:   db.EventTransferIncoming,
			Account:     to,
			Token:       token,
			Amount:      amount,
			TokenSource: tokenSource,
		}
	}
}

type parserError interface {
	IsEq(parserError) bool
	Address() string
}

type parserErrors map[string][]parserError

func (errors parserErrors) appendErr(
	err parserError,
) {
	addr := err.Address()
	accountErrors := errors[addr]

	switch {
	case len(accountErrors) == 0:
		accountErrors = append(accountErrors, err)
	default:
		last := accountErrors[len(accountErrors)-1]
		if !last.IsEq(err) {
			accountErrors = append(accountErrors, err)
		}
	}

	errors[addr] = accountErrors
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

	if fresh {
		err := devDeleteParsed(ctx, pool, userAccountId)
		assert.NoErr(err, "")
	}

	wallets, err := db.GetWallets(ctx, pool, userAccountId)
	assert.NoErr(err, "")

	solanaWallets := make([]string, 0)
	evmContexts := make(map[db.Network]*evmContext)

	for _, w := range wallets {
		switch w.Network {
		case db.NetworkSolana:
			solanaWallets = append(solanaWallets, w.Address)
		case db.NetworkArbitrum, db.NetworkAvaxC, db.NetworkBsc, db.NetworkEthereum:
			// TODO: maybe initialize separately
			ctx, ok := evmContexts[w.Network]
			if !ok {
				ctx = &evmContext{
					contracts: make(map[string][]evmContractImplementation),
					network:   w.Network,
					decimals:  make(map[string]uint8),
				}
			}

			ctx.wallets = append(ctx.wallets, w.Address)
			evmContexts[w.Network] = ctx
		}
	}

	// TODO:
	// pagination ??
	// 700 txs ~= 6.4mb of allocations

	txs, err := db.GetTransactions(ctx, pool, userAccountId)
	assert.NoErr(err, "")

	///////////////////
	// preprocess txs
	errs := make(parserErrors)
	solanaAccounts := make(map[string][]accountLifetime)
	evmAddresses := make(map[db.Network]map[string][]uint64)

	for _, tx := range txs {
		if tx.Err {
			continue
		}

		switch txData := tx.Data.(type) {
		case *db.SolanaTransactionData:
			solPreprocessTx(
				errs,
				solanaAccounts,
				solanaWallets,
				tx.Id,
				txData,
			)
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
		// TODO: just create the context ?
		ctx, ok := evmContexts[network]
		assert.True(ok, "missing evm context for: %s", network.String())

		evmIdentifyContracts(
			evmClient,
			network,
			addresses,
			ctx,
		)
	}

	var batch pgx.Batch
	// NOTE: Errors are created sequentially based on txs, so they are
	// always created in the correct order, so just keeping global
	// position is fine
	errPos := int32(0)
	for _, errs := range errs {
		for _, err := range errs {
			txId, address, ixIdx := "", "", int32(0)
			var (
				kind db.ErrType
				data []byte
			)

			switch e := err.(type) {
			case *errAccountMissing:
				txId, address = e.txId, e.address
				ixIdx = int32(e.ixIdx)
				kind = db.ErrTypeAccountMissing
			case *errAccountBalanceMismatch:
				txId, address = e.txId, e.address
				ixIdx = int32(e.ixIdx)
				kind = db.ErrTypeAccountBalanceMismatch

				var mErr error
				data, mErr = json.Marshal(db.ErrAccountBalanceMismatch{
					Expected: e.expected,
					Had:      e.had,
				})
				assert.NoErr(mErr, "unable to serialize ErrAccountBalanceMismatch")
			}

			q := db.EnqueueInsertErr(
				&batch,
				userAccountId,
				txId,
				pgtype.Int4{Int32: ixIdx, Valid: true},
				errPos,
				db.ErrOriginPreprocess,
				kind,
				address,
				data,
			)
			q.Exec(func(_ pgconn.CommandTag) error { return nil })
			errPos += 1
		}
	}

	br := pool.SendBatch(ctx, &batch)
	err = br.Close()
	assert.NoErr(err, "unable to insert errors")

	///////////////////
	// process txs

	events := make([]*db.Event, 0)

	for _, tx := range txs {
		if tx.Err {
			continue
		}

		switch txData := tx.Data.(type) {
		case *db.SolanaTransactionData:
			solanaCtx := solanaContext{
				wallets:        solanaWallets,
				accounts:       solanaAccounts,
				parserErrors:   errs,
				txId:           tx.Id,
				slot:           txData.Slot,
				timestamp:      tx.Timestamp,
				tokensDecimals: txData.TokenDecimals,
			}

			for ixIdx, ix := range txData.Instructions {
				solanaCtx.ixIdx = uint32(ixIdx)
				solProcessIx(&events, &solanaCtx, ix)
			}
		case *db.EvmTransactionData:
			ctx, ok := evmContexts[tx.Network]
			assert.True(ok, "missing evm context for: %s", tx.Network.String())

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

		if tokenSource != tokenSourceCoingecko &&
			tokenSource > uint16(db.NetworkEvmStart) {

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
		ctx := evmContexts[network]

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
					// NOTE: Coingecko does not have the token data
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
				tokenNetwork := db.Network(tokenSource)
				evmCtx, ok := evmContexts[tokenNetwork]
				assert.True(ok, "missing evm context for %s", tokenNetwork.String())

				decimals, ok := evmCtx.decimals[token]
				assert.True(
					ok,
					"missing decimals for evm %s token: %s",
					tokenNetwork.String(), token,
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
					// TODO: Error
					// NOTE: coingecko does not have the token data
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
					q := db.EnqueueInsertPricepoint(
						&insertPricesBatch,
						p.Close,
						p.Timestamp,
						token.coingeckoId,
					)
					q.Exec(func(ct pgconn.CommandTag) error { return nil })

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
			}
		} else {
			logger.Info("Found pricepoint in coingecko prices: %s %s", token.coingeckoId, ts)
		}

		event := events[token.eventIdx]
		event.Data.SetPrice(price)
	}

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
